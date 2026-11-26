package landing

import (
	"context"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/datapointchris/ingestion/store"
)

// SourceTimeReader answers when the artifact in body was made at its source,
// such as a Reddit post's creation, and zero where body holds no such time.
type SourceTimeReader func(body io.Reader) (time.Time, error)

var sourceTimeReaders = map[string]SourceTimeReader{}

// RegisterSourceTime records read as the reader of mediaType's source time. A
// source whose arrivals carry a SourceTime registers the reader that finds it
// in their bytes, so FillSourceTimes gives it to an item that landed without.
func RegisterSourceTime(mediaType string, read SourceTimeReader) {
	sourceTimeReaders[mediaType] = read
}

// sourceTimeBatch bounds how many items one write of FillSourceTimes fills.
const sourceTimeBatch = 100

// FillSourceTimes gives each item that holds bytes and no source time the time
// that its media type's reader finds in its bytes, a batch to a write. It
// answers how many items it read and how many it filled. An item whose bytes
// are gone or hold no time keeps none, and the next fill reads it again.
func (l *Lander) FillSourceTimes(ctx context.Context) (read, filled int, err error) {
	mediaTypes := make([]string, 0, len(sourceTimeReaders))
	for m := range sourceTimeReaders {
		mediaTypes = append(mediaTypes, m)
	}
	slices.Sort(mediaTypes)
	after := ""
	for {
		items, err := l.store.ItemsWithNoSourceTime(ctx, mediaTypes, after, sourceTimeBatch)
		if err != nil {
			return read, filled, fmt.Errorf("read the items with no source time: %w", err)
		}
		found := map[string]time.Time{}
		for _, item := range items {
			if at, ok := l.sourceTimeOf(ctx, item); ok {
				found[item.Key] = at
			}
		}
		n, err := l.putSourceTimes(ctx, found)
		read, filled = read+len(items), filled+n
		if err != nil {
			return read, filled, err
		}
		if len(items) < sourceTimeBatch {
			return read, filled, nil
		}
		after = items[len(items)-1].Key
	}
}

// sourceTimeOf is the source time in item's bytes, and false where its bytes
// are gone or its reader finds none.
func (l *Lander) sourceTimeOf(ctx context.Context, item store.Item) (time.Time, bool) {
	b, err := l.Open(ctx, *item.BlobSHA256)
	if err != nil {
		return time.Time{}, false
	}
	defer func() { _ = b.Close() }()
	at, err := sourceTimeReaders[item.MediaType](b)
	return at, err == nil && !at.IsZero()
}

// putSourceTimes writes each time in found to its item where the item still
// holds none, and answers how many it wrote.
func (l *Lander) putSourceTimes(ctx context.Context, found map[string]time.Time) (int, error) {
	if len(found) == 0 {
		return 0, nil
	}
	n := 0
	err := l.store.Update(ctx, func(tx *store.Tx) error {
		n = 0
		for key, at := range found {
			item, ok, err := tx.Item(ctx, key)
			if err != nil {
				return err
			}
			if !ok || item.SourceTS != nil {
				continue
			}
			item.SourceTS = new(store.Stamp(at))
			if err := tx.PutItem(ctx, item); err != nil {
				return err
			}
			n++
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("fill the source times: %w", err)
	}
	return n, nil
}
