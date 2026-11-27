package extract

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/datapointchris/ingestion/isolatedprocess"
)

// ProgramDir is where the worker image installs each extractor program.
const ProgramDir = "/usr/local/libexec/ingestion/extractors"

// ProgramTimeout is a program extractor's Timeout unless its registration
// sets another.
const ProgramTimeout = 30 * time.Minute

// lineMax bounds one unit's line on stdout.
const lineMax = 64 << 20

// Program is an extractor that runs the program at path with args, such as
// one whose library is Python. The program reads the item's bytes on stdin,
// and writes one JSON line per unit on stdout, in order:
//
//	{"label": "Chapter 1", "text": "It was a bright cold day…"}
//
// label may be null or left out, and any other field is refused. A run that
// exits non-zero fails its task, with the end of its stderr kept, and so does
// a run past ProgramTimeout. The program runs through isolatedprocess, so it
// holds none of the worker's credentials.
func Program(name, version, mediaType, path string, args ...string) Extractor {
	return Extractor{
		Name: name, Version: version, MediaType: mediaType, Program: path, Timeout: ProgramTimeout,
		Extract: func(ctx context.Context, body io.Reader) (Read, error) {
			var units []Unit
			_, err := isolatedprocess.Run(ctx, isolatedprocess.Command{
				Path: path, Args: args, Stdin: body,
				Stdout: func(r io.Reader) (err error) { units, err = readUnits(r); return err },
			})
			if err != nil {
				return Read{}, err
			}
			return Read{Units: units}, nil
		},
	}
}

// readUnits reads one unit from each line of r, skipping blank lines.
func readUnits(r io.Reader) ([]Unit, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64<<10), lineMax)
	var units []Unit
	for line := 1; scanner.Scan(); line++ {
		if len(bytes.TrimSpace(scanner.Bytes())) == 0 {
			continue
		}
		var u struct {
			Label *string `json:"label"`
			Text  *string `json:"text"`
		}
		decoder := json.NewDecoder(bytes.NewReader(scanner.Bytes()))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&u); err != nil {
			return nil, fmt.Errorf("line %d, which is not a unit: %w", line, err)
		}
		if u.Text == nil {
			return nil, fmt.Errorf("line %d, which names no text", line)
		}
		units = append(units, Unit{Label: u.Label, Text: *u.Text})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("a stdout it could not read: %w", err)
	}
	return units, nil
}
