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
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/datapointchris/ingestion/itemkey"
	"github.com/datapointchris/ingestion/store"
)

// ErrBytesMissing is a blob row whose bytes neither the volume nor a live home
// holds.
var ErrBytesMissing = errors.New("the blob's bytes are missing")

// ErrKeyMismatch is an arrival naming a key other than the one its link or its
// bytes derive.
var ErrKeyMismatch = errors.New("the key is not the one its link or bytes derive")

// ErrInvalidArrival is an arrival that LandIn refuses before it writes, for
// what the arrival itself holds.
var ErrInvalidArrival = errors.New("the arrival is not one that lands")

var namePattern = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)

// Config is what landing writes to.
type Config struct {
	Store *store.Store
	Blobs *Blobs
	// Homes maps each named root, such as books, to the directory it names. A
	// file under a root is referenced in place rather than copied.
	Homes map[string]string
	// QueueParse queues the parse of an item new to the store, in the write
	// that adds it. Nil queues none.
	QueueParse func(context.Context, *store.Tx, store.Item) error
	// Sighted runs in the write that adds a sighting to an item the store
	// already held, under the item's key, such as the gates run again. Nil
	// runs nothing.
	Sighted func(ctx context.Context, tx *store.Tx, key string) error
}

// Lander lands arrivals.
type Lander struct {
	store      *store.Store
	blobs      *Blobs
	homes      map[string]string
	queueParse func(context.Context, *store.Tx, store.Item) error
	sighted    func(context.Context, *store.Tx, string) error
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
	return &Lander{store: cfg.Store, blobs: cfg.Blobs, homes: cfg.Homes, queueParse: cfg.QueueParse, sighted: cfg.Sighted}, nil
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
	// Key is the item's key, as itemkey makes it. Empty takes the key that URL
	// derives, or else the bytes' key, file/<first 16 hex of their SHA-256>.
	// Land refuses a reddit, youtube or url key that differs from URL's, and a
	// file key that differs from the bytes'. A home's file is keyed by its bytes.
	Key       string
	MediaType string
	// ExternalKey is the connector's own key for the artifact. Empty keeps
	// the sighting's, or records the item's key on a first sighting.
	ExternalKey string
	Tags        []string
	// Fields are values read from the source's payload, such as a Reddit
	// save's subreddit. A later arrival's value replaces the sighting's.
	Fields map[string]string
	// Metrics are the source's counts by name, such as a video's views. A
	// later arrival's value replaces the sighting's.
	Metrics        map[string]int64
	CursorPosition string
	Title          string
	// URL is the item's own URL, such as a Reddit post's permalink.
	URL string
	// SourceTime is when the artifact was made at its source, such as a
	// Reddit post's creation, and zero where the source gives none. An item
	// keeps the first that any arrival gives it.
	SourceTime time.Time
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
// before any row names them. The item, its blob row, its home, its sighting
// and a new item's parse task are then written in one transaction. A new
// sighting of an item the store held runs the sighted hook in that
// transaction. An item takes the strictest zone of its sightings and its
// parent.
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
	key, err := linkKey(a)
	if err != nil {
		return Landed{}, err
	}
	var written *Written
	var modified string
	switch {
	case a.Home != nil:
		w, mod, err := l.hashHome(*a.Home)
		if err != nil {
			return Landed{}, err
		}
		written, modified = &w, mod
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
	if key, err = bytesKey(key, a.Home != nil, written); err != nil {
		return Landed{}, err
	}
	var landed Landed
	err = l.store.Update(ctx, func(tx *store.Tx) error {
		var err error
		landed, err = l.write(ctx, tx, a, key, written, modified)
		return err
	})
	return landed, err
}

// LandIn lands the arrival a in the write tx, such as the one that ends a
// poll's lease. a carries no bytes and names no home, since bytes reach the
// volume before any row names them. It refuses an arrival for what it holds
// with ErrInvalidArrival, before it writes.
func (l *Lander) LandIn(ctx context.Context, tx *store.Tx, a Arrival) (Landed, error) {
	key, err := l.keyIn(a)
	if err != nil {
		return Landed{}, fmt.Errorf("%w: %w", ErrInvalidArrival, err)
	}
	if a.URL != "" {
		a.URL, _ = itemkey.CanonicalURL(a.URL)
	}
	return l.write(ctx, tx, a, key, nil, "")
}

// keyIn is the key that LandIn lands a under, once a holds what LandIn takes.
func (l *Lander) keyIn(a Arrival) (string, error) {
	if a.Body != nil || a.Home != nil {
		return "", errors.New("an arrival landed in a write already open carries no bytes and names no home")
	}
	if err := l.check(a); err != nil {
		return "", err
	}
	if a.URL != "" {
		canonical, err := itemkey.CanonicalURL(a.URL)
		if err != nil {
			return "", err
		}
		a.URL = canonical
	}
	return linkKey(a)
}

// write records a under key in tx, then queues a new item's parse, or runs
// the sighted hook where a adds a sighting to an item the store held.
func (l *Lander) write(ctx context.Context, tx *store.Tx, a Arrival, key string, w *Written, modified string) (Landed, error) {
	landed, err := record(ctx, tx, a, key, w, modified)
	switch {
	case err != nil:
		return landed, err
	case landed.ItemAdded && l.queueParse != nil:
		item, _, err := tx.Item(ctx, key)
		if err != nil {
			return landed, err
		}
		return landed, l.queueParse(ctx, tx, item)
	case landed.SightingAdded && !landed.ItemAdded && l.sighted != nil:
		return landed, l.sighted(ctx, tx, key)
	}
	return landed, nil
}

// linkKey is the key a names, or where it names none, the key its URL
// derives. A home's file is keyed by its bytes, so its URL derives nothing. It
// refuses a key of a link's kind that the URL does not derive.
func linkKey(a Arrival) (string, error) {
	if a.URL == "" || a.Home != nil {
		return a.Key, nil
	}
	derived, err := itemkey.FromURL(a.URL)
	if err != nil {
		return "", err
	}
	if a.Key == "" {
		return derived, nil
	}
	kind, _, _ := strings.Cut(a.Key, "/")
	if (kind == itemkey.Reddit || kind == itemkey.YouTube || kind == itemkey.URL) && !strings.Contains(a.Key, "#") && a.Key != derived {
		return "", fmt.Errorf("%w: %s derives %s, not %s", ErrKeyMismatch, a.URL, derived, a.Key)
	}
	return a.Key, nil
}

// bytesKey is key, or the key of the bytes w names where key is empty. It
// refuses a home's key, or a file key, that the bytes do not derive.
func bytesKey(key string, home bool, w *Written) (string, error) {
	if w == nil {
		return key, nil
	}
	derived, err := itemkey.FromFileSHA256(w.SHA256)
	if err != nil {
		return "", err
	}
	kind, _, _ := strings.Cut(key, "/")
	switch {
	case key == "":
		return derived, nil
	case (home || kind == itemkey.File && !strings.Contains(key, "#")) && key != derived:
		return "", fmt.Errorf("%w: the bytes derive %s, not %s", ErrKeyMismatch, derived, key)
	}
	return key, nil
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
	case a.Key == "" && a.URL == "" && a.Body == nil && a.Home == nil:
		return errors.New("an arrival with no key carries the link or the bytes its key is derived from")
	}
	if a.Key != "" {
		if err := itemkey.Check(a.Key); err != nil {
			return err
		}
	}
	if err := store.CheckMetrics(a.Metrics); err != nil {
		return err
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
// every symlink. It answers what its bytes are, and the file's modification
// time from before the read, so a write during the read leaves a time that
// differs and the next reader hashes the file again.
func (l *Lander) hashHome(h Home) (Written, string, error) {
	f, info, err := l.openHome(h.Root, h.Path)
	if err != nil {
		return Written{}, "", err
	}
	defer func() { _ = f.Close() }()
	w, err := Hash(f)
	return w, modifiedStamp(info.ModTime()), err
}

func (l *Lander) openHome(name, path string) (*os.File, os.FileInfo, error) {
	dir, ok := l.homes[name]
	if !ok {
		return nil, nil, fmt.Errorf("no home root is named %q", name)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, nil, fmt.Errorf("open home root %s: %w", name, err)
	}
	defer func() { _ = root.Close() }()
	f, err := root.Open(filepath.FromSlash(path))
	if err != nil {
		return nil, nil, fmt.Errorf("open %s:%s: %w", name, path, err)
	}
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		_ = f.Close()
		return nil, nil, fmt.Errorf("%s:%s is not a regular file", name, path)
	}
	return f, info, nil
}

// modifiedStamp is a file's modification time as a home records it, to the
// nanosecond.
func modifiedStamp(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

// record writes what a adds to the store, and nothing where it adds nothing.
func record(ctx context.Context, tx *store.Tx, a Arrival, key string, w *Written, modified string) (Landed, error) {
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
	homeChanged := false
	if a.Home != nil {
		if landed.HomeAdded, homeChanged, err = recordHome(ctx, tx, *a.Home, w.SHA256, modified); err != nil {
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
	if !a.SourceTime.IsZero() {
		merged.SourceTS = fill(merged.SourceTS, store.Stamp(a.SourceTime))
	}
	if !found || !reflect.DeepEqual(merged, existing) {
		if err := tx.PutItem(ctx, merged); err != nil {
			return landed, err
		}
		landed.ItemAdded = !found
		landed.Changed = true
	}
	if found && merged.Zone != existing.Zone {
		if err := raiseChildren(ctx, tx, key, merged.Zone); err != nil {
			return landed, err
		}
	}

	sighting, had, err := tx.Sighting(ctx, key, a.Connector)
	if err != nil {
		return landed, err
	}
	next := sighting
	if !had {
		next = store.Sighting{
			ItemKey: key, Connector: a.Connector, Zone: a.Zone, ExternalKey: key, Tags: []string{}, Fields: map[string]string{},
			Metrics: map[string]int64{},
		}
	}
	next.Zone = store.Stricter(next.Zone, a.Zone)
	next.Tags = union(next.Tags, a.Tags)
	if len(a.Fields) > 0 {
		next.Fields = maps.Clone(next.Fields)
		if next.Fields == nil {
			next.Fields = map[string]string{}
		}
		maps.Copy(next.Fields, a.Fields)
	}
	if len(a.Metrics) > 0 {
		next.Metrics = maps.Clone(next.Metrics)
		if next.Metrics == nil {
			next.Metrics = map[string]int64{}
		}
		maps.Copy(next.Metrics, a.Metrics)
	}
	if a.ExternalKey != "" {
		next.ExternalKey = a.ExternalKey
	}
	if a.CursorPosition != "" {
		position := a.CursorPosition
		next.CursorPosition = &position
	}
	if !had || !reflect.DeepEqual(next, sighting) {
		if err := tx.PutSighting(ctx, next); err != nil {
			return landed, err
		}
		landed.SightingAdded = !had
		landed.Changed = true
	}
	landed.Changed = landed.Changed || landed.BlobAdded || homeChanged
	return landed, nil
}

// raiseChildren makes every item below key at least as strict as zone. A
// child's zone is derived from its parent's, so a parent that a later sighting
// made outside leaves no child reading as the owner's words.
func raiseChildren(ctx context.Context, tx *store.Tx, key string, zone store.Zone) error {
	parents := []string{key}
	for len(parents) > 0 {
		children, err := tx.Children(ctx, parents[0])
		parents = parents[1:]
		if err != nil {
			return err
		}
		for _, child := range children {
			if store.Stricter(child.Zone, zone) == child.Zone {
				continue
			}
			child.Zone = store.Stricter(child.Zone, zone)
			if err := tx.PutItem(ctx, child); err != nil {
				return err
			}
			parents = append(parents, child.Key)
		}
	}
	return nil
}

// recordHome records that the bytes sum live at h with the modification time
// modified, and marks gone any other bytes recorded at the same path, because
// the file there changed. It says whether the home is new, and whether it
// wrote anything.
func recordHome(ctx context.Context, tx *store.Tx, h Home, sum, modified string) (added, changed bool, err error) {
	pathSum := sha256.Sum256([]byte(h.Path))
	pathSHA := hex.EncodeToString(pathSum[:])
	live, err := tx.LiveHomesAt(ctx, h.Root, pathSHA)
	if err != nil {
		return false, false, err
	}
	var here *store.Home
	for _, home := range live {
		if home.BlobSHA256 == sum {
			here = &home
			continue
		}
		if err := tx.PutHome(ctx, home, true); err != nil {
			return false, false, err
		}
		changed = true
	}
	if here == nil {
		home := store.Home{BlobSHA256: sum, Root: h.Root, PathSHA256: pathSHA, Path: h.Path, ModifiedTS: &modified}
		return true, true, tx.PutHome(ctx, home, false)
	}
	if here.ModifiedTS != nil && *here.ModifiedTS == modified {
		return false, changed, nil
	}
	here.ModifiedTS = &modified
	return false, true, tx.PutHome(ctx, *here, false)
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

// Bytes is an open copy of a blob's bytes, which the caller closes.
type Bytes struct {
	*os.File
	Blob store.Blob
	// Home is where the bytes were read, and nil for the volume's copy. A
	// home's file can change after it is read, and the volume's never does.
	Home *store.Home
}

// Open opens the bytes named sum: the volume's copy, or else a live home whose
// file still holds them. A home's file with the blob's size and the
// modification time recorded for it is taken as the blob, as git takes a file
// whose stat matches its index. Any other is hashed again first. It answers
// store.ErrNoBlob where no row names sum, and ErrBytesMissing where no copy of
// the bytes is left.
func (l *Lander) Open(ctx context.Context, sum string) (Bytes, error) {
	blob, err := l.store.Blob(ctx, sum)
	if err != nil {
		return Bytes{Blob: blob}, err
	}
	if f, err := os.Open(l.blobs.Path(sum)); err == nil {
		return Bytes{File: f, Blob: blob}, nil
	}
	homes, err := l.store.LiveHomes(ctx, sum)
	if err != nil {
		return Bytes{Blob: blob}, err
	}
	for _, h := range homes {
		f, info, err := l.openHome(h.Root, h.Path)
		if err != nil {
			continue
		}
		if holds(f, info, h, blob) {
			return Bytes{File: f, Blob: blob, Home: &h}, nil
		}
		_ = f.Close()
	}
	return Bytes{Blob: blob}, fmt.Errorf("%s: %w", sum, ErrBytesMissing)
}

// holds says whether f, the file at home h, still holds blob's bytes, and
// leaves f at its start.
func holds(f *os.File, info os.FileInfo, h store.Home, blob store.Blob) bool {
	if info.Size() != blob.SizeBytes {
		return false
	}
	if h.ModifiedTS != nil && *h.ModifiedTS == modifiedStamp(info.ModTime()) {
		return true
	}
	w, err := Hash(f)
	if err != nil || w.SHA256 != blob.SHA256 {
		return false
	}
	_, err = f.Seek(0, io.SeekStart)
	return err == nil
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
