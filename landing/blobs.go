package landing

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// tempPrefix starts the name of each file that a write has not yet renamed into place.
const tempPrefix = ".landing-"

var sha256Pattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// CheckSHA256 says why sum is not a SHA-256 in lowercase hex. A blob is stored
// and asked for under that name alone.
func CheckSHA256(sum string) error {
	if !sha256Pattern.MatchString(sum) {
		return fmt.Errorf("a blob is named by its SHA-256 in 64 lowercase hex, not %q", sum)
	}
	return nil
}

// Blobs is the volume that keeps bytes under their SHA-256, as
// <dir>/sha256/<first two>/<rest>. A file there is written once and never
// changes, so a reader needs no lock.
type Blobs struct {
	dir string
}

// OpenBlobs opens the volume at dir, creating it where it is missing.
func OpenBlobs(dir string) (*Blobs, error) {
	if err := os.MkdirAll(filepath.Join(dir, "sha256"), 0o750); err != nil {
		return nil, fmt.Errorf("create the blob volume: %w", err)
	}
	return &Blobs{dir: dir}, nil
}

// Written is what a write learned of the bytes it kept.
type Written struct {
	SHA256 string
	// MD5 names the bytes for a reader that keys by it, as sioyek keys a
	// book's annotations. It secures nothing.
	MD5  string
	Size int64
}

// Path is where the bytes named sum live.
func (b *Blobs) Path(sum string) string {
	return filepath.Join(b.dir, "sha256", sum[:2], sum[2:])
}

// Has says whether the volume holds the bytes named sum.
func (b *Blobs) Has(sum string) bool {
	_, err := os.Stat(b.Path(sum))
	return err == nil
}

// ErrDigestMismatch is bytes whose SHA-256 is not the one their writer named.
var ErrDigestMismatch = errors.New("the bytes' SHA-256 is not the one named")

// Write keeps r's bytes under their SHA-256. It writes a temporary file of a
// unique name, syncs it, renames it into place and syncs its directory, so a
// row naming the bytes is never written before they are on disk. Where want
// names a SHA-256, bytes of another are refused before the rename. A failed
// write removes its temporary file.
func (b *Blobs) Write(r io.Reader, want string) (Written, error) {
	root := filepath.Join(b.dir, "sha256")
	tmp, err := os.CreateTemp(root, tempPrefix+"*")
	if err != nil {
		return Written{}, fmt.Errorf("start a blob: %w", err)
	}
	kept := false
	defer func() {
		if !kept {
			_ = tmp.Close()
			_ = os.Remove(tmp.Name())
		}
	}()
	sum, digest := sha256.New(), md5.New()
	size, err := io.Copy(io.MultiWriter(tmp, sum, digest), r)
	if err != nil {
		return Written{}, fmt.Errorf("write a blob: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return Written{}, fmt.Errorf("sync a blob: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return Written{}, fmt.Errorf("close a blob: %w", err)
	}
	w := Written{SHA256: hex.EncodeToString(sum.Sum(nil)), MD5: hex.EncodeToString(digest.Sum(nil)), Size: size}
	if want != "" && want != w.SHA256 {
		return Written{}, fmt.Errorf("%w: named %s, read %s", ErrDigestMismatch, want, w.SHA256)
	}
	target := b.Path(w.SHA256)
	if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
		return Written{}, fmt.Errorf("create a blob's directory: %w", err)
	}
	// Bytes already there are these same bytes, so the rename replaces them
	// with themselves, and repairs a file that was damaged since.
	if err := os.Rename(tmp.Name(), target); err != nil {
		return Written{}, fmt.Errorf("move a blob into place: %w", err)
	}
	kept = true
	if err := syncDir(filepath.Dir(target)); err != nil {
		return Written{}, err
	}
	return w, nil
}

// Hash reads r whole and answers its SHA-256, MD5 and size, keeping nothing.
func Hash(r io.Reader) (Written, error) {
	sum, digest := sha256.New(), md5.New()
	size, err := io.Copy(io.MultiWriter(sum, digest), r)
	if err != nil {
		return Written{}, err
	}
	return Written{SHA256: hex.EncodeToString(sum.Sum(nil)), MD5: hex.EncodeToString(digest.Sum(nil)), Size: size}, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open a blob's directory: %w", err)
	}
	defer func() { _ = d.Close() }()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("sync a blob's directory: %w", err)
	}
	return nil
}

// stored is one file in the volume: a blob under its SHA-256, or a
// temporary file that no rename finished.
type stored struct {
	sha256 string
	temp   string
	size   int64
	info   os.FileInfo
}

// walk lists every file under the volume's sha256 directory.
func (b *Blobs) walk() ([]stored, error) {
	root := filepath.Join(b.dir, "sha256")
	var out []stored
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		if strings.HasPrefix(d.Name(), tempPrefix) {
			out = append(out, stored{temp: rel, size: info.Size(), info: info})
			return nil
		}
		sum := strings.ReplaceAll(rel, string(filepath.Separator), "")
		if CheckSHA256(sum) != nil || filepath.Dir(rel) != sum[:2] {
			out = append(out, stored{temp: rel, size: info.Size(), info: info})
			return nil
		}
		out = append(out, stored{sha256: sum, size: info.Size(), info: info})
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	return out, err
}
