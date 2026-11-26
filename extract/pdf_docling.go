package extract

import (
	"time"

	"github.com/datapointchris/ingestion/store"
)

// DoclingVersion is the Docling that release/python-requirements.txt pins,
// and that the worker image installs for pdf-docling.
const DoclingVersion = "2.153.0"

// DoclingTimeout bounds one run of pdf-docling. A 1,245-page PDF, the longest
// that sections selected when this was measured, took 34 minutes under
// `/usr/bin/time -v pdf-docling < <the PDF>` on a busy desk of 16 CPUs, 1.6 s
// a page. A node with half those CPUs takes about twice that, and the bound
// leaves more than twice that again.
const DoclingTimeout = 3 * time.Hour

// PDFDocling reads a PDF with Docling's standard pipeline, one unit per PDF
// page, so unit n is PDF page n, a blank page included. A unit's text is the
// page's markdown, with each code block fenced and each table's rows kept,
// and its label is the page's printed label, as PDFText reads it. Docling
// drops every formula, so it reads only the books whose profile
// sections.FontShares.CodeHeavyMathLight picks.
//
// It is secondary, so landing never queues it. The write that ends such a
// book's structure task queues it, and its parse writes beside PDFText's. It
// runs in SlotGroupHeavy, so a worker runs it alone among that group's parses
// unless [worker.slot_limits] says otherwise. Its
// version names Docling's and DoclingPins, so a parse by other libraries or
// weights writes beside the old one. A change to the program's own reading
// raises the leading number.
var PDFDocling = func() Extractor {
	e := Program("docling", "1+docling-"+DoclingVersion+"-"+DoclingPins, MediaTypePDF, ProgramDir+"/pdf-docling")
	e.Positional, e.Secondary, e.Timeout, e.SlotLimit, e.SlotGroup = true, true, DoclingTimeout, 1, SlotGroupHeavy
	return e
}()

// DoclingPins is the first 12 hex digits of the SHA-256 of every pin in
// release/python-requirements.txt, then every model pin in
// release/download-docling-models, one a line. Each shapes Docling's
// markdown, and a test fails until this equals them.
const DoclingPins = "87e1925f632b"

func init() { store.PreferParse(PDFDocling.Name) }
