// Package schemas embeds the JSON Schema of each record the outbox ships, each
// API answer and each declaration file, so the service checks a declaration
// against the same file an editor and a data catalog read.
package schemas

import "embed"

// Files is every *.schema.json in this directory.
//
//go:embed *.schema.json
var Files embed.FS

// Base is the URL each file's $id sits under, as Base + its file name.
const Base = "https://github.com/datapointchris/ingestion/schemas/"
