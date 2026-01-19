// The gtakeaway command does incremental sync of Google Takeout zip files
// stored on Google Drive to the local filesystem, only downloading parts of the
// zip files you haven't downloaded from a previous sync.
//
// Once it's done, it can delete the zip files from Google Drive.
package main

import (
	"cmp"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"iter"
	"log"
	"maps"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"go4.org/ziputil"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/option"
)

var scopes = []string{drive.DriveReadonlyScope}

var (
	driveFolder = flag.String("drive-folder", "", "If non-empty, only consider Takeouts in this exact Drive folder ID or URL. Default: all 'Takeout' folders in root.")
	verbose     = flag.Bool("verbose", false, "verbose logging")
)

func usage() {
	fmt.Fprintf(os.Stderr, `Usage: gtakeaway [flags] <mode>

gtakeaway list
gtakeaway stats <takeout-prefix>
gtakeaway download <takeout-prefix> <local-dir>

For example:

  $ gtakeaway download takeout-20240101T120000Z $HOME/Takeout

Flags:
`)
	flag.PrintDefaults()
	os.Exit(1)
}

func main() {
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		usage()
	}
	mode := args[0]
	switch mode {
	case "list":
		if len(args) != 1 {
			usage()
		}
	case "stats":
		if len(args) != 2 {
			usage()
		}
	case "download":
		if len(args) != 3 {
			usage()
		}
	default:
		usage()
	}

	ctx := context.Background()
	confDir, err := os.UserConfigDir()
	if err != nil {
		log.Fatalf("getting home directory: %v", err)
	}
	confDir = filepath.Join(confDir, "gtakeaway")
	if err := os.MkdirAll(confDir, 0700); err != nil {
		log.Fatal(err)
	}

	confJSON, err := os.ReadFile(filepath.Join(confDir, "gtakeaway-client-secret.json"))
	if err != nil {
		log.Fatalf("read credentials.json: %v", err)
	}
	config, err := google.ConfigFromJSON(confJSON, scopes...)
	if err != nil {
		log.Fatalf("parse credentials.json: %v", err)
	}

	tokenFile := filepath.Join(confDir, "token.json")
	tok, err := tokenFromFile(tokenFile)
	if err != nil {
		tok = getTokenFromWeb(config)
		if err := saveToken(tokenFile, tok); err != nil {
			log.Fatalf("save token.json: %v", err)
		}
	}

	c, err := NewClient(ctx, config, tok)
	if err != nil {
		log.Fatalf("NewClient: %v", err)
	}
	c.verbose = *verbose

	if slash := strings.LastIndexByte(*driveFolder, '/'); slash != -1 {
		*driveFolder = (*driveFolder)[slash+1:]
	}

	takeoutFolders, err := c.TakeoutFolders(ctx, *driveFolder)
	if err != nil {
		log.Fatal(err)
	}

	exports, err := c.ExportsFromFolders(ctx, takeoutFolders)
	if err != nil {
		log.Fatalf("ExportsFromFolders: %v", err)
	}

	if len(exports) == 0 {
		var ids []string
		for _, df := range takeoutFolders {
			ids = append(ids, df.Id)
		}
		log.Fatalf("no Takeout exports found in folder(s) %v", ids)
	}

	if mode == "list" {
		fmt.Printf("Exports:\n")
		for _, exp := range exports {
			fmt.Printf(" * %s (folder %s): %d zip files\n", exp.Prefix, exp.FolderID, len(exp.Files))
		}
		return
	}

	if mode == "stats" {
		exportName := args[1]
		exp, ok := exportWithName(exports, exportName)
		if !ok {
			log.Fatalf("no export found with name %q; run 'gtakeaway list' to see available exports", exportName)
		}
		if err := c.PrintExportStats(ctx, exp); err != nil {
			log.Fatalf("PrintExportStats: %v", err)
		}
		return
	}

	if mode == "download" {
		takeoutName, localDir := args[1], args[2]
		exp, ok := exportWithName(exports, takeoutName)
		if !ok {
			log.Fatalf("no export found with name %q; run 'gtakeaway list' to see available exports", takeoutName)
		}
		if err := c.DownloadTakeout(ctx, exp, localDir); err != nil {
			log.Fatalf("DownloadTakeout: %v", err)
		}
		return
	}

	// if *listZips {
	// 	zips, err := c.ListZipFiles(ctx, *driveFolder)
	// 	if err != nil {
	// 		log.Fatalf("listing zip files: %v", err)
	// 	}
	// 	var sum int64
	// 	for _, z := range zips {
	// 		fmt.Printf("%11d %s\n", z.Size, z.Name)
	// 		sum += z.Size
	// 	}
	// 	fmt.Printf("%d zip files; total size %d bytes (%.2f GB)\n", len(zips), sum, float64(sum)/(1<<30))
	// 	return
	// }

	// if err := c.List(ctx, *driveFolder); err != nil {
	// 	log.Fatalf("listing %v: %v", *driveFolder, err)
	// }
	log.Fatal("TODO")
}

func (c *Client) ListZipFiles(ctx context.Context, folderID string) ([]*drive.File, error) {
	var files []*drive.File
	for df, err := range c.iterFolder(ctx, folderID) {
		if err != nil {
			return nil, err
		}
		if strings.HasPrefix(df.Name, "takeout-") && strings.HasSuffix(df.Name, ".zip") {
			files = append(files, df)
		}
	}
	slices.SortFunc(files, func(a, b *drive.File) int {
		return strings.Compare(a.Name, b.Name)
	})
	return files, nil
}

func (c *Client) List(ctx context.Context, folderID string) error {
	zips, err := c.ListZipFiles(ctx, folderID)
	if err != nil {
		return err
	}

	var sum int64
	for i, fi := range zips {
		log.Printf("scanning meta of zip %d/%d ...", i+1, len(zips))
		toc, err := c.readZipTOC(ctx, fi)
		if err != nil {
			log.Fatalf("readZipTOC(%q): %v", fi.Name, err)
		}

		zr, err := ziputil.ParseTOC(fi.Size, toc)
		if err != nil {
			log.Fatalf("ParseTOC: %v", err)
		}
		for _, zf := range zr.File {
			fmt.Printf("%s\t%11d %d %10d %s\n", fi.Name, zf.CRC32, zf.UncompressedSize64, zf.Method, zf.Name)

			if zf.Name == "Takeout/Google+ Stream/Photos/Photos from posts/2011-11-05/1738fec901q3m.jpg" {
				zfd, err := ziputil.OpenWithReader(zf, func(offsize, size int64) (rawReader io.ReadCloser, err error) {
					return c.streamRange(ctx, fi, offsize, offsize+size-1)
				})
				if err != nil {
					log.Fatalf("OpenWithReader: %v", err)
				}

				data, err := io.ReadAll(zfd)
				zfd.Close()
				if err != nil {
					log.Fatalf("io.ReadAll: %v", err)
				}
				if err := os.WriteFile("data/"+path.Base(zf.Name), data, 0644); err != nil {
					log.Fatalf("os.WriteFile: %v", err)
				}
			}
		}
	}
	log.Printf("Total size of files in Takeout: %d bytes (%.2f GB)\n", sum, float64(sum)/(1<<30))
	return nil
}

type Client struct {
	svc     *drive.Service
	verbose bool
}

func NewClient(ctx context.Context, config *oauth2.Config, tok *oauth2.Token) (*Client, error) {
	c := &Client{}
	client := config.Client(ctx, tok)

	svc, err := drive.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		return nil, fmt.Errorf("drive.NewService: %w", err)
	}

	c.svc = svc
	return c, nil
}

func (c *Client) readZipTOC(ctx context.Context, f *drive.File) (toc []byte, err error) {
	if f.HeadRevisionId == "" {
		return nil, fmt.Errorf("file %q has no HeadRevisionId", f.Name)
	}
	if f.Size <= 0 {
		return nil, fmt.Errorf("file %q has no size", f.Name)
	}
	if f.Id == "" {
		return nil, fmt.Errorf("file %q has no Id", f.Name)
	}

	tocFile := "toc-cache/" + url.QueryEscape(f.HeadRevisionId) + ".zip.toc"
	tailFile := "toc-cache/" + url.QueryEscape(f.HeadRevisionId) + ".zip.tail"
	if data, err := os.ReadFile(tocFile); err == nil {
		os.Remove(tailFile) // no longer useful, if present
		return data, nil
	}

	tail, err := os.ReadFile(tailFile)
	if err != nil {
		readLen := min(65<<10, f.Size)
		tail, err = c.readRange(ctx, f.Id, f.Size-readLen, f.Size)
		if err != nil {
			return nil, fmt.Errorf("readRange for TOC: %w", err)
		}
		if err := writeAtomic(tailFile, tail, 0400); err != nil {
			log.Printf("warning: writeAtomic(%q): %v", tailFile, err)
		}
	}
	defer func() {
		if err == nil {
			if err := writeAtomic(tocFile, toc, 0400); err != nil {
				log.Printf("warning: writeAtomic(%q): %v", tocFile, err)
			}
			os.Remove(tailFile) // no longer useful
		}
	}()

	tocSize, ok := ziputil.ZipTOCSize(f.Size, tail)
	if !ok {
		return nil, fmt.Errorf("could not find TOC size in last %d bytes of zip file %q", len(tail), f.Name)
	}
	if tocSize <= int64(len(tail)) {
		return tail[len(tail)-int(tocSize):], nil
	}

	return c.readRange(ctx, f.Id, f.Size-tocSize, f.Size)
}

func writeAtomic(filename string, data []byte, perm os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(filename), 0755); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(filename), filepath.Base(filename)+".***.tmp")
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(f.Name())
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(f.Name())
		return err
	}
	if err := os.Chmod(f.Name(), perm); err != nil {
		os.Remove(f.Name())
		return err
	}
	if err := os.Rename(f.Name(), filename); err != nil {
		os.Remove(f.Name())
		return err
	}
	return nil
}

func (c *Client) readRange(ctx context.Context, fileID string, start, endInclusive int64) ([]byte, error) {
	log.Printf("readRange: %s %d-%d (%d bytes)", fileID, start, endInclusive, endInclusive-start+1)
	if start < 0 || endInclusive < start {
		return nil, fmt.Errorf("invalid range %d-%d", start, endInclusive)
	}
	call := c.svc.Files.Get(fileID).Context(ctx)
	call.Header().Set("Range", fmt.Sprintf("bytes=%d-%d", start, endInclusive))

	resp, err := call.Download()
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Drive should respond 206 for Range requests.
	if resp.StatusCode != 206 && resp.StatusCode != 200 {
		// 200 can happen if Range is ignored; treat as error for your ReaderAt use case.
		return nil, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// If we got 200, b may be the whole file—dangerous for multi-GB.
	// You can hard-fail if 200 and (end-start+1) is small:
	if resp.StatusCode == 200 && int64(len(b)) > (endInclusive-start+1)+1024 {
		return nil, fmt.Errorf("server ignored Range; returned %d bytes", len(b))
	}
	return b, nil
}

// streamRange streams the given byte range [start, endInclusive] of the given
// Drive file's head revision. The caller is responsible for closing the
// returned ReadCloser when the error is nil.
func (c *Client) streamRange(ctx context.Context, df *drive.File, start, endInclusive int64) (io.ReadCloser, error) {
	log.Printf("streamRange: %s %d-%d (%d bytes)", df.Id, start, endInclusive, endInclusive-start+1)
	if start < 0 || endInclusive < start {
		return nil, fmt.Errorf("invalid range %d-%d", start, endInclusive)
	}
	call := c.svc.Revisions.Get(df.Id, df.HeadRevisionId).Context(ctx)
	call.Header().Set("Range", fmt.Sprintf("bytes=%d-%d", start, endInclusive))

	resp, err := call.Download()
	if err != nil {
		return nil, err
	}

	switch resp.StatusCode {
	case http.StatusPartialContent:
		// Happy case.
		return resp.Body, nil
	case http.StatusOK:
		// Also acceptable, but only if we asked for the whole
		// range.
		if start == 0 && endInclusive == df.Size-1 {
			return resp.Body, nil
		}
		// TODO(bradfitz): if we see 200s for small files under some threshold,
		// maybe just download it all and return the range from memory?
		// But only if we see such a 200 later.
	}
	resp.Body.Close()
	return nil, fmt.Errorf("unexpected HTTP status %d downloading (%v,%v) range [%d,%d]", resp.StatusCode, df.Id, df.HeadRevisionId, start, endInclusive)
}

func (c *Client) iterFolder(ctx context.Context, folderID string) iter.Seq2[*drive.File, error] {
	nFiles := 0
	return func(yield func(*drive.File, error) bool) {
		pageToken := ""
		for {
			r, err := c.svc.Files.List().
				Context(ctx).
				Q(fmt.Sprintf("'%s' in parents and trashed = false and (mimeType = 'application/zip' or name contains '.zip')", folderID)).
				Fields("nextPageToken, files(*,id,name,mimeType,createdTime,modifiedTime,size,headRevisionId,version)").
				PageSize(1000). // but it seems to cap out at 100 in any case
				PageToken(pageToken).
				Do()
			if err != nil {
				yield(nil, err)
				return
			}
			if c.verbose {
				nFiles += len(r.Files)
				log.Printf("# iterFolder(%s): got page of %d files; up to %d total\n", folderID, len(r.Files), nFiles)
			}
			for _, f := range r.Files {
				if !yield(f, nil) {
					return
				}
			}
			if r.NextPageToken == "" {
				return
			}
			pageToken = r.NextPageToken
		}
	}
}

// Installed-app flow: prints a URL; you open it, authorize, paste code.
// This is robust for CLI tools. Alternatives include a local redirect server.
func getTokenFromWeb(config *oauth2.Config) *oauth2.Token {
	authURL := config.AuthCodeURL("state-token", oauth2.AccessTypeOffline, oauth2.ApprovalForce)
	fmt.Printf("Open this link in your browser:\n%v\n\n", authURL)

	fmt.Print("Paste the authorization code: ")
	var code string
	if _, err := fmt.Scan(&code); err != nil {
		log.Fatalf("read code: %v", err)
	}

	tok, err := config.Exchange(context.Background(), code)
	if err != nil {
		log.Fatalf("exchange code for token: %v", err)
	}
	return tok
}

func tokenFromFile(file string) (*oauth2.Token, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	tok := &oauth2.Token{}
	return tok, json.NewDecoder(f).Decode(tok)
}

func saveToken(path string, token *oauth2.Token) error {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	return json.NewEncoder(f).Encode(token)
}

// TakeoutFolders returns either the provider folder (if optFolderID is
// non-empty), or all folders named "Takeout" in the root.
//
// It returns an error if none is found.
func (c *Client) TakeoutFolders(ctx context.Context, optFolderID string) ([]*drive.File, error) {
	r, err := c.svc.Files.List().
		Q("name = 'Takeout' and mimeType = 'application/vnd.google-apps.folder' and 'root' in parents and trashed = false").
		Fields("files(id,name,createdTime,mimeType)").
		PageSize(10).
		Do()
	if err != nil {
		return nil, err
	}
	// Sort by CreatedTime descending (newest first)
	slices.SortFunc(r.Files, func(a, b *drive.File) int {
		return cmp.Compare(b.CreatedTime, a.CreatedTime)
	})

	if optFolderID == "" {
		if len(r.Files) == 0 {
			return nil, fmt.Errorf("no Takeout folder(s) found under usual name & location; specify --drive-folder=<id> to pick alternate folder")
		}
		// By default, return all 1+ Takeout folders found.
		return r.Files, nil
	}

	// If they specified an explicit folder ID, find and return just that one.
	for _, f := range r.Files {
		if f.Id == optFolderID {
			return []*drive.File{f}, nil
		}
	}

	// If they specified an explicit folder ID that we didn't find,
	// assume they renamed it to something that wasn't "Takeout", and see
	// if exists on Google Drive and is a folder.
	f, err := c.svc.Files.Get(optFolderID).Fields("id,name,createdTime,mimeType").Do()
	if err != nil {
		switch len(r.Files) {
		case 0:
			return nil, fmt.Errorf("no Takeout folder found with ID %q: %v", optFolderID, err)
		case 1:
			return nil, fmt.Errorf("error reading Drive folder found with ID %q: %v\ndid you mean your Takeout folder %q ?", optFolderID, err, r.Files[0].Id)
		default:
			var opts []string
			for _, f := range r.Files {
				opts = append(opts, f.Id)
			}
			return nil, fmt.Errorf("error reading Drive folder found with ID %q: %v\ndid you mean one of your Takeout folders: %q ?", optFolderID, err, strings.Join(opts, ", "))
		}
	}
	if f.MimeType != "application/vnd.google-apps.folder" {
		return nil, fmt.Errorf("file with ID %q is not a folder (mimeType=%q)", optFolderID, f.MimeType)
	}
	return []*drive.File{f}, nil
}

type Export struct {
	FolderID string // drive folder ID
	Prefix   string // e.g. "takeout-20240101T120000Z"
	Time     time.Time
	Files    []*drive.File
}

const timeLayout = "20060102T150405Z"

func (c *Client) ExportsFromFolders(ctx context.Context, folders []*drive.File) ([]*Export, error) {
	var exports []*Export
	for _, folder := range folders {
		fexports, err := c.ExportsFromFolder(ctx, folder)
		if err != nil {
			return nil, fmt.Errorf("ExportsFromFolder(%q): %w", folder.Id, err)
		}
		exports = append(exports, fexports...)
	}
	slices.SortFunc(exports, func(a, b *Export) int {
		if v := cmp.Compare(a.Prefix, b.Prefix); v != 0 {
			return v
		}
		return cmp.Compare(a.FolderID, b.FolderID)
	})
	return exports, nil
}

func (c *Client) ExportsFromFolder(ctx context.Context, folder *drive.File) ([]*Export, error) {
	zips, err := c.ListZipFiles(ctx, folder.Id)
	if err != nil {
		return nil, fmt.Errorf("listing zip files in folder %q: %w", folder.Id, err)
	}
	exportsMap := map[string]*Export{}
	for _, df := range zips {
		suf, ok := strings.CutPrefix(df.Name, "takeout-")
		if !ok {
			continue
		}
		dateStr, _, ok := strings.Cut(suf, "-")
		if !ok {
			continue
		}
		prefix := "takeout-" + dateStr
		dt, err := time.Parse(timeLayout, dateStr)
		if err != nil {
			log.Printf("warning: ignoring unexpected filename %q: %v", df.Name, err)
			continue
		}
		exp, ok := exportsMap[prefix]
		if !ok {
			exp = &Export{
				FolderID: folder.Id,
				Prefix:   prefix,
				Time:     dt,
			}
			exportsMap[prefix] = exp
		}
		exp.Files = append(exp.Files, df)
	}
	exports := slices.Collect(maps.Values(exportsMap))
	slices.SortFunc(exports, func(a, b *Export) int {
		return cmp.Compare(a.Prefix, b.Prefix)
	})
	return exports, nil
}

func exportWithName(exports []*Export, name string) (*Export, bool) {
	if i := slices.IndexFunc(exports, func(exp *Export) bool {
		return exp.Prefix == name
	}); i >= 0 {
		return exports[i], true
	}
	return nil, false
}

func (c *Client) PrintExportStats(ctx context.Context, exp *Export) error {
	var sumZips int64
	for _, f := range exp.Files {
		sumZips += f.Size
	}
	fmt.Printf("Export %q (folder %s): %d zip files (%v total compressed)\n", exp.Prefix, exp.FolderID, len(exp.Files), niceBytes(sumZips))

	if *verbose {
		log.Printf("# reading TOCs of %d zip files ...", len(exp.Files))
	}
	zips, err := c.readExportZipTOCs(ctx, exp)
	if err != nil {
		return fmt.Errorf("readExportZipTOCs: %w", err)
	}
	var numFiles int64
	var sumUncompressed int64
	var byType = make(map[string]int)
	var folders = make(map[string]bool)
	for _, zar := range zips {
		for _, f := range zar.Reader.File {
			numFiles++
			sumUncompressed += int64(f.UncompressedSize64)
			if ext := strings.ToLower(path.Ext(f.Name)); ext != "" {
				byType[ext]++
			}
			dir := path.Dir(f.Name)
			folders[dir] = true
		}
	}
	fmt.Printf("  Contains %d dirs, %d files; total uncompressed size %v\n", len(folders), numFiles, niceBytes(sumUncompressed))
	fmt.Printf("  File types:\n")
	keys := slices.Collect(maps.Keys(byType))
	slices.SortFunc(keys, func(a, b string) int {
		return cmp.Compare(byType[b], byType[a]) // descending by count
	})
	for _, ext := range keys {
		fmt.Printf("    %8d %s\n", byType[ext], ext)
	}
	return nil
}

type Stats struct {
	NumFiles          int64
	BytesCompressed   int64
	BytesUncompressed int64
}

func (c *Client) DownloadTakeout(ctx context.Context, exp *Export, localDir string) error {
	if fi, err := os.Stat(localDir); err != nil {
		return err
	} else if !fi.IsDir() {
		return fmt.Errorf("%q is not a directory", localDir)
	}
	zips, err := c.readExportZipTOCs(ctx, exp)
	if err != nil {
		return fmt.Errorf("readExportZipTOCs: %w", err)
	}
	var total Stats
	var done Stats
	var todo []DownloadableItem
	for _, far := range zips {
		for _, fh := range far.Reader.File {
			total.NumFiles++
			total.BytesCompressed += int64(fh.CompressedSize64)
			total.BytesUncompressed += int64(fh.UncompressedSize64)

			dst := filepath.Join(localDir, fh.Name)
			if fi, err := os.Stat(dst); err == nil {
				if fi.Size() == int64(fh.UncompressedSize64) {
					done.NumFiles++
					done.BytesUncompressed += int64(fh.UncompressedSize64)
					done.BytesCompressed += int64(fh.CompressedSize64)
					continue
				}
			}
			todo = append(todo, DownloadableItem{
				DriveFile: far.File,
				Zip:       far.Reader,
				Header:    fh,
			})
		}
	}

	for _, item := range todo {
		log.Printf("Downloading from zip %q ...", item.DriveFile.Name)
		if path.Clean(item.Header.Name) != item.Header.Name {
			return fmt.Errorf("invalid file name %q", item.Header.Name)
		}
		dst := filepath.Join(localDir, item.Header.Name)
		if err := c.downloadItem(ctx, item, dst); err != nil {
			return fmt.Errorf("downloadItem(%q): %w", item.Header.Name, err)
		}
	}
	return nil
}

func (c *Client) downloadItem(ctx context.Context, item DownloadableItem, dst string) error {
	zfd, err := ziputil.OpenWithReader(item.Header, func(offsize, size int64) (rawReader io.ReadCloser, err error) {
		return c.streamRange(ctx, item.DriveFile, offsize, offsize+size-1)
	})
	if err != nil {
		return fmt.Errorf("OpenWithReader(%q in %q): %w", item.Header.Name, item.DriveFile.Name, err)
	}
	defer zfd.Close()
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return fmt.Errorf("MkdirAll(%q): %w", filepath.Dir(dst), err)
	}
	f, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("OpenFile(%q): %w", dst, err)
	}
	if _, err := io.Copy(f, zfd); err != nil {
		f.Close()
		return fmt.Errorf("Copy(%q): %w", dst, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("Close(%q): %w", dst, err)
	}
	zfd.Close()
	return nil
}

type FileAndReader struct {
	File   *drive.File
	Reader *ziputil.Reader
}

type DownloadableItem struct {
	DriveFile *drive.File
	Zip       *ziputil.Reader
	Header    *ziputil.FileHeader
}

func (c *Client) readExportZipTOCs(ctx context.Context, exp *Export) (map[string]FileAndReader, error) {
	zips := make(map[string]FileAndReader)
	for i, f := range exp.Files {
		if *verbose {
			log.Printf("# reading TOC of zip %d/%d: %s ...", i+1, len(exp.Files), f.Name)
		}
		toc, err := c.readZipTOC(ctx, f)
		if err != nil {
			return nil, fmt.Errorf("readZipTOC(%q): %w", f.Name, err)
		}
		zr, err := ziputil.ParseTOC(f.Size, toc)
		if err != nil {
			return nil, fmt.Errorf("ParseTOC(%q): %w", f.Name, err)
		}
		zips[f.Name] = FileAndReader{File: f, Reader: zr}
	}
	return zips, nil
}

func niceBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for n/div >= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(n)/float64(div), "KMGTPE"[exp])
}
