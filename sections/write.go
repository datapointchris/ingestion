package sections

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/datapointchris/ingestion/extract"
	"github.com/datapointchris/ingestion/itemkey"
	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
)

// QueueAfterParse queues the structure task of a PDF in the write that ends
// its pdf-text parse done, so every parsed PDF is profiled.
func QueueAfterParse(ctx context.Context, tx *store.Tx, task store.Task) error {
	if task.Capability != extract.Capability(extract.PDFText) {
		return nil
	}
	_, err := tx.Enqueue(ctx, store.Task{ItemKey: task.ItemKey, Stage: store.StageStructure, Capability: Capability})
	return err
}

// Replayable is the structure stage as a replay runs it again, such as with a
// new pdf-structure: a task for each PDF. Its end writes the sections again,
// beside the old.
func Replayable() server.Replayable {
	return server.Replayable{Capability: func(item store.Item) (string, bool) {
		return Capability, item.MediaType == extract.MediaTypePDF
	}}
}

// OutlineVersion is the producer version of the sections that the outline rule
// writes from structure and parse, each of which is part of it. So a new
// profile or a new parse writes its sections beside the old.
func OutlineVersion(structure, parse store.Output) string {
	return RuleVersion + "/" + structure.Producer + "@" + structure.ProducerVersion + "/" + parse.Producer + "@" + parse.ProducerVersion
}

// AfterStructure runs in the write that ends a PDF's structure task done. It
// reads the newest done profile and the newest done pdf-text parse, and
// writes the sections that Outline finds from them, once per version. Where
// the item holds no chapter yet, each chapter becomes a child item, numbered
// from 1, which takes the item's zone, source time, side and read decisions.
// A later version writes its sections beside the old, and makes no child.
// Where the item holds no source time, it takes the PDF's creation date.
func AfterStructure(ctx context.Context, tx *store.Tx, task store.Task) error {
	if task.ItemKey == nil {
		return nil
	}
	key := *task.ItemKey
	item, ok, err := tx.Item(ctx, key)
	if err != nil || !ok {
		return err
	}
	outputs, err := tx.Outputs(ctx, key)
	if err != nil {
		return err
	}
	structure := store.NewestDone(outputs, store.StageStructure)
	if structure == nil {
		return nil
	}
	profile, err := decodeProfile(structure.Answer)
	if err != nil {
		return fmt.Errorf("read %s's profile from %s %s: %w", key, structure.Producer, structure.ProducerVersion, err)
	}
	if item.SourceTS == nil {
		if made, ok := CreationTime(profile.CreationDate); ok {
			stamp := store.Stamp(made)
			item.SourceTS = &stamp
			if err := tx.PutItem(ctx, item); err != nil {
				return err
			}
		}
	}
	parse, err := tx.Parse(ctx, key, store.ParsePick{Extractor: extract.PDFText.Name})
	switch {
	case errors.Is(err, store.ErrNoParse):
		// The parse's end queues the structure again, which writes them.
		return nil
	case err != nil:
		return err
	}
	rows := Outline(profile, parse.Units, OutlineVersion(*structure, parse.Output))
	children, err := chaptersOf(ctx, tx, key)
	if err != nil {
		return err
	}
	var made []store.Item
	n := 0
	for i := range rows {
		rows[i].ItemKey = key
		if rows[i].Kind != store.SectionChapter || len(children) > 0 {
			continue
		}
		n++
		child, err := itemkey.Child(key, n)
		if err != nil {
			return err
		}
		rows[i].ChildKey = &child
		made = append(made, store.Item{
			Key: child, MediaType: store.MediaTypeChapter, Zone: item.Zone, ParentKey: &item.Key, ParentFlowID: &item.FlowID,
			Title: rows[i].Title, SourceTS: item.SourceTS,
		})
	}
	wrote, err := tx.PutSections(ctx, rows)
	if err != nil || !wrote {
		return err
	}
	for _, child := range made {
		if err := tx.PutItem(ctx, child); err != nil {
			return err
		}
	}
	return route.PassToChapters(ctx, tx, key)
}

// chaptersOf reads the chapter items made from the item under key.
func chaptersOf(ctx context.Context, tx *store.Tx, key string) ([]store.Item, error) {
	children, err := tx.Children(ctx, key)
	var chapters []store.Item
	for _, c := range children {
		if c.MediaType == store.MediaTypeChapter {
			chapters = append(chapters, c)
		}
	}
	return chapters, err
}

// pdfDate is a PDF date, D:YYYYMMDDHHmmSSOHH'mm', each part after the year
// optional, with D: left out as some writers do.
var pdfDate = regexp.MustCompile(`^(?:D:)?(\d{4})(\d{2})?(\d{2})?(\d{2})?(\d{2})?(\d{2})?(?:([Zz+-])(?:(\d{2})'?(?:(\d{2})'?)?)?)?$`)

// CreationTime is the instant that a PDF's creationDate names, and false
// where it names none or is not a date. A date with no zone, or with Z, is
// read as UTC.
func CreationTime(creationDate *string) (time.Time, bool) {
	if creationDate == nil {
		return time.Time{}, false
	}
	m := pdfDate.FindStringSubmatch(*creationDate)
	if m == nil {
		return time.Time{}, false
	}
	part := func(i, unset int) int {
		if m[i] == "" {
			return unset
		}
		n, _ := strconv.Atoi(m[i])
		return n
	}
	year, month, day, hour, minute, second := part(1, 0), part(2, 1), part(3, 1), part(4, 0), part(5, 0), part(6, 0)
	offset := 0
	if m[7] == "+" || m[7] == "-" {
		offset = part(8, 0)*3600 + part(9, 0)*60
		if m[7] == "-" {
			offset = -offset
		}
	}
	t := time.Date(year, time.Month(month), day, hour, minute, second, 0, time.FixedZone("", offset))
	if year < 1 || t.Month() != time.Month(month) || t.Day() != day || t.Hour() != hour || t.Minute() != minute || t.Second() != second {
		return time.Time{}, false
	}
	return t.UTC(), true
}
