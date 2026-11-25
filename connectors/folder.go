package connectors

import (
	"context"
	"errors"
	"fmt"
	"mime"
	"path"
	"strings"

	"github.com/datapointchris/ingestion/landing"
)

// bookMediaTypes names the media type of each book format that the standard
// library's table and a slim container's lack of /etc/mime.types leave out.
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
}

// ScanFolder lands by reference each file in c's folder that c has not
// already landed as it is now. A file whose recorded size and modification
// time still match is skipped unread, so a rerun over an unchanged folder
// lands nothing and hashes nothing. A file that fails to land is named in the
// error, and the scan goes on to the rest.
func ScanFolder(ctx context.Context, l *landing.Lander, c Connector) (Scanned, error) {
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
		if seen {
			out.Skipped++
			continue
		}
		landed, err := l.Land(ctx, landing.Arrival{
			Connector: c.Name, Zone: c.Zone, MediaType: mediaTypeOf(f.Path), ExternalKey: f.Path, Tags: c.Tags,
			Home: &landing.Home{Root: c.Root, Path: f.Path},
		})
		if err != nil {
			failed = append(failed, fmt.Errorf("land %s:%s: %w", c.Root, f.Path, err))
			continue
		}
		if landed.Changed {
			out.Landed++
		}
	}
	return out, errors.Join(failed...)
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
