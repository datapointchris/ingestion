package deliver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/datapointchris/ingestion/store"
)

// LearningResources is the deliverer that creates each book as a resource in
// learning, through learning's POST /api/v1/resources with the item's key as
// the resource's source_key and its file's path as the resource's file_path.
// It creates each book's chapter there too, as a chapter resource part of its
// book's resource, with its pages.
const LearningResources = "learning-resources"

// LearningResourcesScopes is what the learning client asks its token for: the
// one create that the deliverer writes through.
var LearningResourcesScopes = []string{"learning.resources.write"}

func init() {
	Register(Registration{
		Name: LearningResources, Scopes: LearningResourcesScopes, Takes: learningTakes, FilePath: true, Chapters: true,
		Open: NewLearningResources,
	})
}

// pdfMediaType is the one media type that learning-resources writes as a
// book, whose copy is a PDF.
const pdfMediaType = "application/pdf"

var errNotPDF = errors.New("it is not a PDF, and learning holds a book's file only as its pdf copy")

func learningTakes(item store.Item) error {
	if item.MediaType != pdfMediaType {
		return errNotPDF
	}
	return nil
}

// learningTimeout bounds one create, so a learning that hangs frees its worker
// slot and the task tries again.
const learningTimeout = 30 * time.Second

// NewLearningResources is the learning-resources deliverer for the learning
// API at api. It calls learning through client, which carries the token of a
// service client holding LearningResourcesScopes.
func NewLearningResources(api string, client *http.Client) Deliverer {
	return learningResources{create: strings.TrimRight(api, "/") + "/api/v1/resources", client: client}
}

type learningResources struct {
	create string
	client *http.Client
}

// learningResource is the create's body, following learning's ResourceCreate.
// It names the type by name, so it needs no type id. It sends no zone, so the
// item's zone does not reach learning. A book sends its copy and path, and a
// chapter sends its book's id and its pages instead.
type learningResource struct {
	Title            string  `json:"title"`
	ResourceType     string  `json:"resource_type"`
	BookCopy         string  `json:"book_copy,omitempty"`
	FilePath         string  `json:"file_path,omitempty"`
	SourceKey        string  `json:"source_key"`
	PartOf           int64   `json:"part_of,omitempty"`
	PDFPageStart     int     `json:"pdf_page_start,omitempty"`
	PDFPageEnd       int     `json:"pdf_page_end,omitempty"`
	PrintedPageStart *string `json:"printed_page_start,omitempty"`
	PrintedPageEnd   *string `json:"printed_page_end,omitempty"`
}

// bookResource is the create of the book whose pdf copy is the file at path.
// Its title is the item's, else the file's name.
func bookResource(item Item) (learningResource, error) {
	if item.Path == nil || *item.Path == "" {
		return learningResource{}, errors.New("the delivery holds no file path, which a learning book needs")
	}
	title := strings.TrimSuffix(path.Base(*item.Path), path.Ext(*item.Path))
	if t := learningTitle(item.Title); t != "" {
		title = t
	}
	return learningResource{Title: title, ResourceType: "book", BookCopy: "pdf", FilePath: *item.Path, SourceKey: item.Key}, nil
}

// learningTitle is title as the destination stores it, which holds no NUL,
// and "" where it holds no other character.
func learningTitle(title *string) string {
	if title == nil {
		return ""
	}
	return strings.TrimSpace(strings.ReplaceAll(*title, "\x00", ""))
}

// chapterResource is the create of the chapter part of its book's resource.
// learning takes a chapter's printed pages only as a pair, so where the book
// prints one end and not the other, it sends neither. Its title is the
// item's, else its pages, as an outline entry with no title leaves it.
func chapterResource(item Item) (learningResource, error) {
	c := item.Chapter
	book, err := strconv.ParseInt(c.BookDestinationID, 10, 64)
	if err != nil || book <= 0 {
		return learningResource{}, fmt.Errorf("the chapter's book is %q in learning, which names no resource", c.BookDestinationID)
	}
	r := learningResource{
		Title: learningTitle(item.Title), ResourceType: "chapter", SourceKey: item.Key, PartOf: book,
		PDFPageStart: c.PDFPageStart, PDFPageEnd: c.PDFPageEnd,
	}
	if c.PrintedPageStart != nil && *c.PrintedPageStart != "" && c.PrintedPageEnd != nil && *c.PrintedPageEnd != "" {
		r.PrintedPageStart, r.PrintedPageEnd = c.PrintedPageStart, c.PrintedPageEnd
	}
	switch {
	case r.Title != "":
	case r.PrintedPageStart != nil:
		r.Title = fmt.Sprintf("Pages %s–%s", *r.PrintedPageStart, *r.PrintedPageEnd)
	default:
		r.Title = fmt.Sprintf("PDF pages %d–%d", r.PDFPageStart, r.PDFPageEnd)
	}
	return r, nil
}

// learningCreated is learning's 200 and 201 answer, its ResourceCreated.
type learningCreated struct {
	ID      int64 `json:"id"`
	Adopted bool  `json:"adopted"`
}

// learningConflict is learning's 409 answer, its CreateConflictResponse:
// source_key where the key holds another body or a deleted resource, and
// file_path where the path is held by resources that cannot take the key.
// ResourceID is the lowest holder's id.
type learningConflict struct {
	Conflict   string           `json:"conflict"`
	ResourceID int64            `json:"resource_id"`
	Deleted    bool             `json:"deleted"`
	Holders    []learningHolder `json:"holders"`
}

type learningHolder struct {
	ResourceID int64   `json:"resource_id"`
	FilePath   string  `json:"file_path"`
	SourceKey  *string `json:"source_key"`
}

// Deliver creates item as a book whose pdf copy is the file at its path, or,
// where it is a book's chapter, as a chapter part of its book's resource.
// learning answers 201 for a new resource. It answers 200 for a resource it
// already held: one under no key that held exactly the path and took the key,
// adopted, or one that the key already names. Either is delivered-to-existing.
// Where learning answers 409 naming a resource, the delivery is refused with
// that resource. A 5xx, 408 or 429 leaves it pending to try again, and any
// other answer fails the task, since sending the same request cannot mend it.
// The title, path, book and pages are the delivery's as first queued, so a
// repeat sends the same body, and learning answers it as the key's own.
func (l learningResources) Deliver(ctx context.Context, item Item) Outcome {
	create := bookResource
	if item.Chapter != nil {
		create = chapterResource
	}
	resource, err := create(item)
	if err != nil {
		return permanent(err.Error())
	}
	body, err := json.Marshal(resource)
	if err != nil {
		return permanent(err.Error())
	}
	ctx, cancel := context.WithTimeout(ctx, learningTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, l.create, bytes.NewReader(body))
	if err != nil {
		return permanent(err.Error())
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := l.client.Do(req)
	if err != nil {
		return pending(fmt.Sprintf("the create could not reach learning: %v", err))
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return pending(fmt.Sprintf("learning's %d answer broke off: %v", resp.StatusCode, err))
	}
	switch resp.StatusCode {
	case http.StatusOK, http.StatusCreated:
		var created learningCreated
		if json.Unmarshal(raw, &created) != nil || created.ID <= 0 {
			return pending(fmt.Sprintf("learning answered %d naming no resource: %s", resp.StatusCode, quoted(raw)))
		}
		id := strconv.FormatInt(created.ID, 10)
		if resp.StatusCode == http.StatusOK {
			return Outcome{Outcome: store.DeliveryDeliveredToExisting, DestinationID: &id}
		}
		return Outcome{Outcome: store.DeliveryDelivered, DestinationID: &id}
	case http.StatusConflict:
		var c learningConflict
		if json.Unmarshal(raw, &c) != nil || c.ResourceID <= 0 || (c.Conflict != "source_key" && c.Conflict != "file_path") {
			return pending("learning answered 409 naming no resource: " + quoted(raw))
		}
		id := strconv.FormatInt(c.ResourceID, 10)
		why := learningRefusal(c, resource.FilePath)
		return Outcome{Outcome: store.DeliveryRefused, DestinationID: &id, Error: &why}
	}
	if resp.StatusCode >= 500 || resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode == http.StatusTooManyRequests {
		return pending(fmt.Sprintf("learning answered %d: %s", resp.StatusCode, quoted(raw)))
	}
	return permanent(fmt.Sprintf("learning answered %d, which the same request cannot mend: %s", resp.StatusCode, quoted(raw)))
}

// learningRefusal says which of learning's resources refused a create of the
// file at sent, and why.
func learningRefusal(c learningConflict, sent string) string {
	if c.Conflict == "source_key" {
		if c.Deleted {
			return fmt.Sprintf("learning's resource %d holds this key and is deleted", c.ResourceID)
		}
		return fmt.Sprintf("learning's resource %d holds this key from another body", c.ResourceID)
	}
	ids := make([]string, len(c.Holders))
	for i, h := range c.Holders {
		ids[i] = strconv.FormatInt(h.ResourceID, 10)
	}
	switch {
	case c.Deleted:
		return fmt.Sprintf("learning's deleted resource %s holds this file_path", strings.Join(ids, ", "))
	case len(c.Holders) > 1:
		return fmt.Sprintf("learning's resources %s each hold this file_path", strings.Join(ids, ", "))
	case len(c.Holders) == 1 && c.Holders[0].FilePath != sent:
		return fmt.Sprintf("learning's resource %d holds the directory %s, which holds this file", c.ResourceID, c.Holders[0].FilePath)
	case len(c.Holders) == 1:
		return fmt.Sprintf("learning's resource %d holds this file_path under source_key %s", c.ResourceID, orNone(c.Holders[0].SourceKey))
	}
	return fmt.Sprintf("learning's resource %d holds this file_path", c.ResourceID)
}
