package landing

import (
	"context"
	"errors"
	"time"

	"github.com/datapointchris/ingestion/store"
)

// LeftoverAfter is how old a temporary file is before the report names it,
// since a younger one may be a write still running.
const LeftoverAfter = time.Hour

// Unpaired is every place the volume and the blob rows disagree, as git fsck
// names dangling and missing objects. It deletes nothing.
type Unpaired struct {
	// Files counts the blobs in the volume, and Rows the blob rows.
	Files int
	Rows  int
	// Unrowed is each blob in the volume that no row names, such as the
	// bytes of a landing whose transaction failed.
	Unrowed []StoredFile
	// Missing is each blob row whose bytes neither the volume nor a live home
	// holds.
	Missing []store.Blob
	// Leftovers is each file in the volume that is not a blob, such as a
	// temporary file older than LeftoverAfter.
	Leftovers []StoredFile
}

// StoredFile is one file in the volume. A blob is named by its SHA-256, and
// any other file by its path below the volume's sha256 directory.
type StoredFile struct {
	SHA256     string
	Path       string
	SizeBytes  int64
	ModifiedTS string
}

// Unpaired walks the volume and reads every blob row, and answers where the
// two disagree.
func (l *Lander) Unpaired(ctx context.Context) (Unpaired, error) {
	var u Unpaired
	files, err := l.blobs.walk()
	if err != nil {
		return u, err
	}
	rows, err := l.store.Blobs(ctx)
	if err != nil {
		return u, err
	}
	u.Rows = len(rows)
	rowed := map[string]bool{}
	for _, r := range rows {
		rowed[r.SHA256] = true
	}
	held := map[string]bool{}
	now := l.store.Now()
	for _, f := range files {
		file := StoredFile{SHA256: f.sha256, Path: f.temp, SizeBytes: f.size, ModifiedTS: store.Stamp(f.info.ModTime())}
		switch {
		case f.sha256 == "":
			if now.Sub(f.info.ModTime()) > LeftoverAfter {
				u.Leftovers = append(u.Leftovers, file)
			}
		case !rowed[f.sha256]:
			u.Files++
			u.Unrowed = append(u.Unrowed, file)
		default:
			u.Files++
			held[f.sha256] = true
		}
	}
	for _, r := range rows {
		if held[r.SHA256] {
			continue
		}
		f, _, err := l.Open(ctx, r.SHA256)
		if errors.Is(err, ErrBytesMissing) {
			u.Missing = append(u.Missing, r)
			continue
		}
		if err != nil {
			return u, err
		}
		_ = f.Close()
	}
	return u, nil
}
