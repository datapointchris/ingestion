package extract

// TrafilaturaVersion is the trafilatura that release/python-requirements.txt
// pins, and that the worker image installs for article-trafilatura.
const TrafilaturaVersion = "2.4.0"

// MediaTypeHTML is a web page's media type, as a feed's entry lands one.
const MediaTypeHTML = "text/html"

// ArticleTrafilatura reads a web page's main text with trafilatura, one unit
// per paragraph, heading, list item or table row, in the page's order. It
// keeps the page's tables and leaves out its comment section. It runs in no
// slot group, so a page never waits behind a book or a memo.
//
// Its version names trafilatura's and TrafilaturaPins, so a parse by other
// libraries writes beside the old one. A change to the program's own reading
// raises the leading number. It fetches the page of an item that landed no
// bytes, as a pushed link does.
var ArticleTrafilatura = func() Extractor {
	e := Program("article-trafilatura", "1+trafilatura-"+TrafilaturaVersion+"-"+TrafilaturaPins, MediaTypeHTML, ProgramDir+"/article-trafilatura")
	e.FetchesPage = true
	return e
}()

// TrafilaturaPins is the first 12 hex digits of the SHA-256 of the pin of each
// library in trafilaturaLibraries' closure in release/python-requirements.txt,
// in the lock's order, one a line. Each shapes the text that trafilatura
// finds, and a test fails until this equals them.
const TrafilaturaPins = "f242083fb35b"

// trafilaturaLibraries are the libraries that article-trafilatura imports.
var trafilaturaLibraries = []string{"trafilatura"}
