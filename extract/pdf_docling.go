package extract

import "time"

// DoclingVersion is the Docling that release/python-requirements.txt pins,
// and that the worker image installs for pdf-docling.
const DoclingVersion = "2.153.0"

// DoclingTimeout bounds one run of pdf-docling. Docling reads about 1.6 s a
// page on a busy desk of 16 CPUs. The longest book that sections selects, 1,245
// pages, took 34 minutes there, and a node with half its CPUs takes about
// twice that.
const DoclingTimeout = 3 * time.Hour

// PDFDocling reads a PDF with Docling's standard pipeline, one unit per PDF
// page, so unit n is PDF page n, a blank page included. A unit's text is the
// page's markdown, with each code block fenced and each table's rows kept,
// and its label is the page's printed label, as PDFText reads it. Docling
// drops every formula, so it reads only the books whose profile
// sections.FontShares.CodeHeavyMathLight picks.
//
// It is secondary, so landing never queues it. The write that ends such a
// book's structure task queues it, and its parse writes beside PDFText's. Its
// version names Docling's. A new revision of a model that the image pins
// raises its leading number, so a parse by other weights writes beside the
// old one.
var PDFDocling = func() Extractor {
	e := Program("docling", "1+docling-"+DoclingVersion, MediaTypePDF, ProgramDir+"/pdf-docling")
	e.Positional, e.Secondary, e.Timeout = true, true, DoclingTimeout
	return e
}()
