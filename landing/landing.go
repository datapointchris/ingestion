// Package landing lands each arrival of an artifact. It keeps the bytes under
// their SHA-256, or references the file where it already lives under a named
// root. It writes the item under the key that itemkey derived, and one sighting
// per connector. This is a claim check over content-addressed storage, as an
// OCI registry keeps layers: a row names bytes by their hash, and the bytes are
// on disk before the row is written.
//
// An arrival that repeats one already landed writes nothing, so a connector may
// deliver an artifact again without a second item, blob or sighting.
package landing

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"

	"github.com/datapointchris/ingestion/itemkey"
	"github.com/datapointchris/ingestion/store"
)

// ErrBytesMissing is a blob row whose bytes neither the volume nor a live home
// holds.
var ErrBytesMissing = errors.New("the blob's bytes are missing")

var namePattern = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)

// Config is what landing writes to.
type Config struct {
	Store *store.Store
	Blobs *Blobs
	// Homes maps each named root, such as books, to the directory it names. A
	// file under a root is referenced in place rather than copied.
	Homes map[string]string
}

// Lander lands arrivals.
type Lander struct {
	store *store.Store
	blobs *Blobs
	homes map[string]string
}

// New is a lander over cfg. It refuses a root whose name is not lowercase
// letters, digits and hyphens, or whose directory is not absolute.
func New(cfg Config) (*Lander, error) {
	for name, dir := range cfg.Homes {
		if !namePattern.MatchString(name) {
			return nil, fmt.Errorf("a home's root is named in lowercase letters, digits and hyphens, not %q", name)
		}
		if !filepath.IsAbs(dir) {
			return nil, fmt.Errorf("home root %s names %q, which is not an absolute directory", name, dir)
		}
	}
	return &Lander{store: cfg.Store, blobs: cfg.Blobs, homes: cfg.Homes}, nil
}

// Home is a file that already lives under a named root.
type Home struct {
	Root string
	// Path is the file's place below its root, with forward slashes, such as
	// a/b.pdf. It must stay below the root.
	Path string
}

// Arrival is one connector's delivery of one artifact.
type Arrival struct {
	// Connector names the connector that delivered the artifact.
	Connector string
	// Zone says who wrote the content: the connector's declared default, or
	// what the push route read from the request.
	Zone store.Zone
	// Key is the item's key, as itemkey derived it from the artifact. Empty
	// lands the bytes as file/<first 16 hex of their SHA-256>.
	Key       string
	MediaType string
	// ExternalKey is the connector's own key for the artifact. Empty keeps
	// the sighting's, or records the item's key on a first sighting.
	ExternalKey    string
	Tags           []string
	CursorPosition string
	Title          string
	URL            string
	// Parent is the key of the item this one was split from, such as the
	// Reddit export that carried a save.
	Parent string
	// ParentFlowID is the flow of the write that delivered the artifact.
	// Empty takes the parent's flow, where a parent is named.
	ParentFlowID string
	// Body is the artifact's bytes. An item that already holds bytes keeps
	// them, and its Body is not read. Nil lands no bytes.
	Body io.Reader
	// Home names a file that already lives under a named root. Its bytes are
	// hashed where they lie, and referenced rather than copied.
	Home *Home
}

// Landed is what an arrival wrote.
type Landed struct {
	Key string
	// SHA256 names the arrival's bytes, and is "" where it read none.
	SHA256        string
	ItemAdded     bool
	SightingAdded bool
	BlobAdded     bool
	HomeAdded     bool
	// Changed is false where the arrival repeated one already landed, and
	// wrote nothing.
	Changed bool
}

// Land lands the arrival a. Bytes go to the volume, or are hashed in place,
// before any row names them. The item, its blob row, its home and its
// sighting are then written in one transaction. An item takes the strictest
// zone of its sightings and its parent.
func (l *Lander) Land(ctx context.Context, a Arrival) (Landed, error) {
	if err := l.check(a); err != nil {
		return Landed{}, err
	}
	if a.URL != "" {
		canonical, err := itemkey.CanonicalURL(a.URL)
		if err != nil {
			return Landed{}, err
		}
		a.URL = canonical
	}
	key := a.Key
	var written *Written
	switch {
	case a.Home != nil:
		w, err := l.hashHome(*a.Home)
		if err != nil {
			return Landed{}, err
		}
		written = &w
	case a.Body != nil:
		holds := false
		if key != "" {
			item, found, err := l.store.ItemRow(ctx, key)
			if err != nil {
				return Landed{}, err
			}
			holds = found && item.BlobSHA256 != nil
		}
		if !holds {
			w, err := l.blobs.Write(a.Body, "")
			if err != nil {
				return Landed{}, err
			}
			written = &w
		}
	}
	if key == "" {
		var err error
		if key, err = itemkey.FromFileSHA256(written.SHA256); err != nil {
			return Landed{}, err
		}
	}
	var landed Landed
	err := l.store.Update(ctx, func(tx *store.Tx) error {
		var err error
		landed, err = record(ctx, tx, a, key, written)
		return err
	})
	return landed, err
}

func (l *Lander) check(a Arrival) error {
	switch {
	case !namePattern.MatchString(a.Connector):
		return fmt.Errorf("a connector is named in lowercase letters, digits and hyphens, not %q", a.Connector)
	case a.Zone != store.Outside && a.Zone != store.Owner:
		return fmt.Errorf("a zone is outside or owner, not %q", a.Zone)
	case a.MediaType == "":
		return errors.New("an arrival names its media type")
	case a.Body != nil && a.Home != nil:
		return errors.New("an arrival carries its bytes or names their home, not both")
	case a.Key == "" && a.Body == nil && a.Home == nil:
		return errors.New("an arrival with no key carries the bytes its key is derived from")
	}
	if a.Key != "" {
		if err := itemkey.Check(a.Key); err != nil {
			return err
		}
	}
	if a.Parent != "" {
		if err := itemkey.Check(a.Parent); err != nil {
			return fmt.Errorf("the parent: %w", err)
		}
		if a.Parent == a.Key {
			return errors.New("an item is not its own parent")
		}
	}
	if a.Home != nil {
		if _, ok := l.homes[a.Home.Root]; !ok {
			return fmt.Errorf("no home root is named %q", a.Home.Root)
		}
		if !filepath.IsLocal(filepath.FromSlash(a.Home.Path)) || filepath.ToSlash(filepath.Clean(filepath.FromSlash(a.Home.Path))) != a.Home.Path {
			return fmt.Errorf("a home's path is a clean path below its root, not %q", a.Home.Path)
		}
	}
	return nil
}

// hashHome reads the file at h, which os.Root keeps below its root through
// every symlink, and answers what its bytes are.
func (l *Lander) hashHome(h Home) (Written, error) {
	f, err := l.openHome(h.Root, h.Path)
	if err != nil {
		return Written{}, err
	}
	defer func() { _ = f.Close() }()
	return Hash(f)
}

func (l *Lander) openHome(name, path string) (*os.File, error) {
	dir, ok := l.homes[name]
	if !ok {
		return nil, fmt.Errorf("no home root is named %q", name)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, fmt.Errorf("open home root %s: %w", name, err)
	}
	defer func() { _ = root.Close() }()
	f, err := root.Open(filepath.FromSlash(path))
	if err != nil {
		return nil, fmt.Errorf("open %s:%s: %w", name, path, err)
	}
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		_ = f.Close()
		return nil, fmt.Errorf("%s:%s is not a regular file", name, path)
	}
	return f, nil
}

// record writes what a adds to the store, and nothing where it adds nothing.
func record(ctx context.Context, tx *store.Tx, a Arrival, key string, w *Written) (Landed, error) {
	landed := Landed{Key: key}
	existing, found, err := tx.Item(ctx, key)
	if err != nil {
		return landed, err
	}
	var parent store.Item
	if a.Parent != "" {
		var ok bool
		if parent, ok, err = tx.Item(ctx, a.Parent); err != nil {
			return landed, err
		} else if !ok {
			return landed, fmt.Errorf("parent %s: %w", a.Parent, store.ErrNoItem)
		}
	}
	if w != nil {
		landed.SHA256 = w.SHA256
		_, had, err := tx.Blob(ctx, w.SHA256)
		if err != nil {
			return landed, err
		}
		if !had {
			if err := tx.PutBlob(ctx, store.Blob{SHA256: w.SHA256, MD5: w.MD5, SizeBytes: w.Size, MediaType: a.MediaType}); err != nil {
				return landed, err
			}
			landed.BlobAdded = true
		}
	}
	if a.Home != nil {
		if landed.HomeAdded, err = recordHome(ctx, tx, *a.Home, w.SHA256); err != nil {
			return landed, err
		}
	}

	item := existing
	if !found {
		item = store.Item{Key: key, MediaType: a.MediaType, Zone: a.Zone}
		if flow := a.ParentFlowID; flow != "" {
			item.ParentFlowID = &flow
		} else if a.Parent != "" {
			item.ParentFlowID = &parent.FlowID
		}
	}
	merged := item
	merged.Zone = store.Stricter(item.Zone, a.Zone)
	if a.Parent != "" {
		merged.Zone = store.Stricter(merged.Zone, parent.Zone)
		merged.ParentKey = fill(merged.ParentKey, a.Parent)
	}
	if w != nil {
		merged.BlobSHA256 = fill(merged.BlobSHA256, w.SHA256)
	}
	merged.Title = fill(merged.Title, a.Title)
	merged.URL = fill(merged.URL, a.URL)
	if !found || !sameItem(merged, existing) {
		if err := tx.PutItem(ctx, merged); err != nil {
			return landed, err
		}
		landed.ItemAdded = !found
		landed.Changed = true
	}

	sighting, had, err := tx.Sighting(ctx, key, a.Connector)
	if err != nil {
		return landed, err
	}
	next := sighting
	if !had {
		next = store.Sighting{ItemKey: key, Connector: a.Connector, Zone: a.Zone, ExternalKey: key, Tags: []string{}}
	}
	next.Zone = store.Stricter(next.Zone, a.Zone)
	next.Tags = union(next.Tags, a.Tags)
	if a.ExternalKey != "" {
		next.ExternalKey = a.ExternalKey
	}
	if a.CursorPosition != "" {
		position := a.CursorPosition
		next.CursorPosition = &position
	}
	if !had || !sameSighting(next, sighting) {
		if err := tx.PutSighting(ctx, next); err != nil {
			return landed, err
		}
		landed.SightingAdded = !had
		landed.Changed = true
	}
	landed.Changed = landed.Changed || landed.BlobAdded || landed.HomeAdded
	return landed, nil
}

// recordHome records that the bytes sum live at h, and marks gone any other
// bytes recorded at the same path, because the file there changed. It says
// whether the home is new.
func recordHome(ctx context.Context, tx *store.Tx, h Home, sum string) (bool, error) {
	pathSum := sha256.Sum256([]byte(h.Path))
	pathSHA := hex.EncodeToString(pathSum[:])
	live, err := tx.LiveHomesAt(ctx, h.Root, pathSHA)
	if err != nil {
		return false, err
	}
	here := false
	for _, home := range live {
		if home.BlobSHA256 == sum {
			here = true
			continue
		}
		if err := tx.PutHome(ctx, home, true); err != nil {
			return false, err
		}
	}
	if here {
		return false, nil
	}
	return true, tx.PutHome(ctx, store.Home{BlobSHA256: sum, Root: h.Root, PathSHA256: pathSHA, Path: h.Path}, false)
}

// Keep writes bytes that a worker made, such as an extractor's output, under
// sum, and records their row where none is. It refuses bytes whose SHA-256 is
// not sum with ErrDigestMismatch, and says whether the row is new.
func (l *Lander) Keep(ctx context.Context, r io.Reader, sum, mediaType string) (store.Blob, bool, error) {
	if err := CheckSHA256(sum); err != nil {
		return store.Blob{}, false, err
	}
	w, err := l.blobs.Write(r, sum)
	if err != nil {
		return store.Blob{}, false, err
	}
	blob := store.Blob{SHA256: w.SHA256, MD5: w.MD5, SizeBytes: w.Size, MediaType: mediaType}
	added := false
	err = l.store.Update(ctx, func(tx *store.Tx) error {
		held, had, err := tx.Blob(ctx, sum)
		if err != nil || had {
			blob = held
			return err
		}
		added = true
		return tx.PutBlob(ctx, blob)
	})
	if err != nil {
		return store.Blob{}, false, err
	}
	if added {
		blob, err = l.store.Blob(ctx, sum)
	}
	return blob, added, err
}

// Open opens the bytes named sum, from the volume or else from a live home
// whose file still holds the blob's size. It answers store.ErrNoBlob where
// no row names sum, and ErrBytesMissing where no copy of the bytes is left.
func (l *Lander) Open(ctx context.Context, sum string) (*os.File, store.Blob, error) {
	blob, err := l.store.Blob(ctx, sum)
	if err != nil {
		return nil, blob, err
	}
	if f, err := os.Open(l.blobs.Path(sum)); err == nil {
		return f, blob, nil
	}
	homes, err := l.store.LiveHomes(ctx, sum)
	if err != nil {
		return nil, blob, err
	}
	for _, h := range homes {
		f, err := l.openHome(h.Root, h.Path)
		if err != nil {
			continue
		}
		if info, err := f.Stat(); err == nil && info.Size() == blob.SizeBytes {
			return f, blob, nil
		}
		_ = f.Close()
	}
	return nil, blob, fmt.Errorf("%s: %w", sum, ErrBytesMissing)
}

// fill is have, or want where have is unset and want is not empty.
func fill(have *string, want string) *string {
	if have != nil || want == "" {
		return have
	}
	return &want
}

func union(have, add []string) []string {
	out := slices.Clone(have)
	for _, t := range add {
		if !slices.Contains(out, t) {
			out = append(out, t)
		}
	}
	slices.Sort(out)
	if out == nil {
		out = []string{}
	}
	return out
}

func same(a, b *string) bool { return (a == nil && b == nil) || (a != nil && b != nil && *a == *b) }

func sameItem(a, b store.Item) bool {
	return a.Zone == b.Zone && a.MediaType == b.MediaType && same(a.BlobSHA256, b.BlobSHA256) &&
		same(a.ParentKey, b.ParentKey) && same(a.Title, b.Title) && same(a.URL, b.URL)
}

func sameSighting(a, b store.Sighting) bool {
	return a.Zone == b.Zone && a.ExternalKey == b.ExternalKey && same(a.CursorPosition, b.CursorPosition) &&
		slices.Equal(a.Tags, b.Tags)
}
