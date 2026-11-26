package extract

// PyMuPDFVersion is the PyMuPDF that release/python-requirements.txt pins,
// and that the worker image installs for the PDF programs.
const PyMuPDFVersion = "1.28.2"

// MediaTypePDF is a PDF's media type, as a folder connector lands one.
const MediaTypePDF = "application/pdf"

// PDFText reads a PDF's text with PyMuPDF, one unit per PDF page, so unit n is
// PDF page n, a blank page included. A unit's label is the page's printed
// label where the PDF declares page labels, and null where it declares none.
// Its version names PyMuPDF's, so a library upgrade writes its pages beside
// the old ones.
var PDFText = func() Extractor {
	e := Program("pdf-text", "1+pymupdf-"+PyMuPDFVersion, MediaTypePDF, ProgramDir+"/pdf-text")
	e.Positional = true
	return e
}()
