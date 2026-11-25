package landing

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"path"
	"strings"
	"time"

	"github.com/datapointchris/ingestion/itemkey"
)

// FolderFile is one regular file below a named root.
type FolderFile struct {
	// Path is the file's place below its root, with forward slashes.
	Path     string
	Size     int64
	Modified time.Time
}

// Folder lists each regular file in dir below the named root, and in every
// directory below it, in path order. It skips each file or directory whose
// name starts with a dot, such as a copy's temporary file, and every symlink.
func (l *Lander) Folder(root, dir string) ([]FolderFile, error) {
	base, ok := l.homes[root]
	if !ok {
		return nil, fmt.Errorf("no home root is named %q", root)
	}
	opened, err := os.OpenRoot(base)
	if err != nil {
		return nil, fmt.Errorf("open home root %s: %w", root, err)
	}
	defer func() { _ = opened.Close() }()
	start := "."
	if dir != "" {
		start = path.Clean(dir)
	}
	var out []FolderFile
	err = fs.WalkDir(opened.FS(), start, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if p != start && strings.HasPrefix(d.Name(), ".") {
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if !d.Type().IsRegular() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		out = append(out, FolderFile{Path: p, Size: info.Size(), Modified: info.ModTime()})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list %s:%s: %w", root, start, err)
	}
	return out, nil
}

// Sighted says whether connector already landed f below root as it is now: a
// live home at its path records its size and modification time, and connector
// sighted the item its bytes key. Such a file lands nothing again, so a scan
// skips it unread.
func (l *Lander) Sighted(ctx context.Context, connector, root string, f FolderFile) (bool, error) {
	pathSum := sha256.Sum256([]byte(f.Path))
	homes, err := l.store.LiveHomesAt(ctx, root, hex.EncodeToString(pathSum[:]))
	if err != nil {
		return false, err
	}
	for _, h := range homes {
		if h.ModifiedTS == nil || *h.ModifiedTS != modifiedStamp(f.Modified) {
			continue
		}
		blob, err := l.store.Blob(ctx, h.BlobSHA256)
		if err != nil || blob.SizeBytes != f.Size {
			continue
		}
		key, err := itemkey.FromFileSHA256(h.BlobSHA256)
		if err != nil {
			return false, err
		}
		return l.store.Sighted(ctx, key, connector)
	}
	return false, nil
}
