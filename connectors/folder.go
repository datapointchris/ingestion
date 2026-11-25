package connectors

import (
	"context"
	"errors"
	"fmt"
	"mime"
	"path"
	"strings"
	"time"

	"github.com/datapointchris/ingestion/landing"
)

// bookMediaTypes names the media type of each book format a folder lands, so
// a folder's media types do not hang on the host's /etc/mime.types.
var bookMediaTypes = map[string]string{
	".epub": "application/epub+zip",
	".mobi": "application/x-mobipocket-ebook",
	".azw3": "application/vnd.amazon.ebook",
	".djvu": "image/vnd.djvu",
	".md":   "text/markdown",
	".txt":  "text/plain",
}

// Scanned is what one folder scan found and landed.
type Scanned struct {
	Files   int
	Landed  int
	Skipped int
	// Failed counts the files that did not land, each named in the scan's
	// error.
	Failed int
	// Settling counts the files written too recently to land, which a later
	// scan lands once they settle.
	Settling int
	// Unfinished says the scan stopped at its deadline with files left, which
	// the next scan picks up.
	Unfinished bool
}

// ScanFolder lands by reference each file in c's folder that c has not
// already landed as it is now. A file whose recorded size and modification
// time still match is skipped unread, so a rerun over an unchanged folder
// lands nothing and hashes nothing. A file modified after settledBefore may
// still be being written, so it waits for a later scan. The scan stops landing
// once until passes. A file that fails to land is named in the error, and the
// scan goes on to the rest.
func ScanFolder(ctx context.Context, l *landing.Lander, c Connector, until, settledBefore time.Time) (Scanned, error) {
	files, err := l.Folder(c.Root, c.Path)
	if err != nil {
		return Scanned{}, err
	}
	out := Scanned{Files: len(files)}
	var failed []error
	for _, f := range files {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		seen, err := l.Sighted(ctx, c.Name, c.Root, f)
		if err != nil {
			return out, err
		}
		switch {
		case seen:
			out.Skipped++
			continue
		case f.Modified.After(settledBefore):
			out.Settling++
			continue
		case !time.Now().Before(until):
			out.Unfinished = true
			continue
		}
		landed, err := l.Land(ctx, landing.Arrival{
			Connector: c.Name, Zone: c.Zone, MediaType: mediaTypeOf(f.Path), ExternalKey: f.Path, Tags: c.Tags,
			Home: &landing.Home{Root: c.Root, Path: f.Path},
		})
		if err != nil {
			out.Failed++
			failed = append(failed, fmt.Errorf("land %s:%s: %w", c.Root, f.Path, err))
			continue
		}
		if landed.Changed {
			out.Landed++
		}
	}
	if len(failed) > 0 {
		return out, fmt.Errorf("%d of %d files did not land: %w", out.Failed, out.Files, errors.Join(failed...))
	}
	return out, nil
}

func mediaTypeOf(p string) string {
	ext := strings.ToLower(path.Ext(p))
	if t, ok := bookMediaTypes[ext]; ok {
		return t
	}
	if t := mime.TypeByExtension(ext); t != "" {
		return t
	}
	return "application/octet-stream"
}
