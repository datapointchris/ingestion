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

// parsedBy is the extractor whose items pass the structure stage. Its parse's
// end queues the stage, a replay selects its media type, and the outline rule
// reads its parse.
var parsedBy = extract.PDFText

// QueueAfterParse queues the structure task of a PDF in the write that ends
// its pdf-text parse done, so every parsed PDF is profiled.
func QueueAfterParse(ctx context.Context, tx *store.Tx, task store.Task) error {
	if task.Capability != extract.Capability(parsedBy) {
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
		return Capability, item.MediaType == parsedBy.MediaType
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
// writes the sections that Outline finds from them through Write. Where the
// item holds no source time, it takes the PDF's creation date.
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
	parse, err := tx.Parse(ctx, key, store.ParsePick{Extractor: parsedBy.Name})
	switch {
	case errors.Is(err, store.ErrNoParse):
		// The parse's end queues the structure again, which writes them.
		return nil
	case err != nil:
		return err
	}
	return Write(ctx, tx, item, Outline(profile, parse.Units, OutlineVersion(*structure, parse.Output)))
}

// Write writes one producer version's sections of book, once, and the
// chapter items they name. Where the book holds no chapter yet, each chapter
// row becomes a child item, numbered from 1, which takes the book's zone,
// source time, side and read decisions. Where it holds chapters, a chapter
// row names the item that an earlier version made for a chapter with the same
// start page and title. A row that matches none names no item, so a later
// version renames no chapter and makes none.
func Write(ctx context.Context, tx *store.Tx, book store.Item, rows []store.Section) error {
	if len(rows) == 0 {
		return nil
	}
	chapters, err := tx.Chapters(ctx, book.Key)
	if err != nil {
		return err
	}
	held, err := chapterKeys(ctx, tx, book.Key)
	if err != nil {
		return err
	}
	var made []store.Item
	n := 0
	for i := range rows {
		rows[i].ItemKey = book.Key
		if rows[i].Kind != store.SectionChapter {
			continue
		}
		if len(chapters) > 0 {
			if child, ok := held[startOf(rows[i])]; ok {
				rows[i].ChildKey = &child
				delete(held, startOf(rows[i]))
			}
			continue
		}
		n++
		child, err := itemkey.Child(book.Key, n)
		if err != nil {
			return err
		}
		rows[i].ChildKey = &child
		made = append(made, store.Item{
			Key: child, MediaType: store.MediaTypeChapter, Zone: book.Zone, ParentKey: &book.Key, ParentFlowID: &book.FlowID,
			Title: rows[i].Title, SourceTS: book.SourceTS,
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
	return route.PassToChapters(ctx, tx, book.Key)
}

// chapterStart is where a chapter starts and what it is called, which a later
// version's chapter must share to name the same chapter item.
type chapterStart struct {
	page  int
	title string
}

func startOf(s store.Section) chapterStart {
	start := chapterStart{page: s.PDFPageStart}
	if s.Title != nil {
		start.title = *s.Title
	}
	return start
}

// chapterKeys maps each start of a chapter that the book's sections name an
// item for to that item's key.
func chapterKeys(ctx context.Context, tx *store.Tx, key string) (map[chapterStart]string, error) {
	sections, err := tx.Sections(ctx, key)
	if err != nil {
		return nil, err
	}
	keys := map[chapterStart]string{}
	for _, s := range sections {
		if s.ChildKey != nil {
			keys[startOf(s)] = *s.ChildKey
		}
	}
	return keys, nil
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
