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
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"go4.org/ziputil"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/option"
)

var scopes = []string{drive.DriveReadonlyScope}

var (
	driveFolder = flag.String("drive-folder", "", "Drive folder ID (or drive URL with ID) to read Takeout zips from. If empty, it uses the root 'Takeout' folder.")
	verbose     = flag.Bool("verbose", false, "verbose logging")
	listZips    = flag.Bool("list-zips", false, "list zip files in the folder and exit")
)

func main() {
	flag.Parse()
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

	if *driveFolder == "" {
		takeoutFolders, err := c.TakeoutFolderIDs(ctx)
		if err != nil {
			log.Fatal(err)
		}
		switch len(takeoutFolders) {
		case 0:
			log.Fatalf("no Takeout folder(s) found under usual name & location; specify --drive-folder=<id> to pick one")
		case 1:
			*driveFolder = takeoutFolders[0].Id
		default:
			log.SetFlags(0)
			log.Printf("Warning: multiple Takeout folders found; specify --drive-folder=<id> to pick one. Options:")
			for _, f := range takeoutFolders {
				date, _, _ := strings.Cut(f.CreatedTime, "T") // chop off time in "2023-06-13T11:33:56.701Z"
				log.Printf("  %s https://drive.google.com/drive/u/0/folders/%s", date, f.Id)
			}
			os.Exit(1)
		}
	}

	if *listZips {
		zips, err := c.ListZipFiles(ctx, *driveFolder)
		if err != nil {
			log.Fatalf("listing zip files: %v", err)
		}
		var sum int64
		for _, z := range zips {
			fmt.Printf("%11d %s\n", z.Size, z.Name)
			sum += z.Size
		}
		fmt.Printf("%d zip files; total size %d bytes (%.2f GB)\n", len(zips), sum, float64(sum)/(1<<30))
		return
	}

	if err := c.List(ctx, *driveFolder); err != nil {
		log.Fatalf("listing %v: %v", *driveFolder, err)
	}
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

func listFolder(ctx context.Context, svc *drive.Service, folderID string) error {
	pageToken := ""
	for {
		r, err := svc.Files.List().
			Context(ctx).
			Q(fmt.Sprintf("'%s' in parents and trashed = false", folderID)).
			Fields("nextPageToken, files(id,name,mimeType,modifiedTime,size)").
			PageSize(1000).
			PageToken(pageToken).
			Do()
		if err != nil {
			return err
		}

		for _, f := range r.Files {
			fmt.Printf("%-40s %-60s %s\n", f.Id, f.Name, f.MimeType)
		}

		if r.NextPageToken == "" {
			break
		}
		pageToken = r.NextPageToken
	}
	return nil
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

func (c *Client) TakeoutFolderIDs(ctx context.Context) ([]*drive.File, error) {
	r, err := c.svc.Files.List().
		Q("name = 'Takeout' and mimeType = 'application/vnd.google-apps.folder' and 'root' in parents and trashed = false").
		Fields("files(id,name,createdTime)").
		PageSize(10).
		Do()
	if err != nil {
		return nil, err
	}
	// Sort by CreatedTime descending (newest first)
	slices.SortFunc(r.Files, func(a, b *drive.File) int {
		return cmp.Compare(b.CreatedTime, a.CreatedTime)
	})
	return r.Files, nil
}
