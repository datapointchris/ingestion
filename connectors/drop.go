package connectors

import (
	"bytes"
	"context"
	"fmt"

	"github.com/datapointchris/ingestion/landing"
)

// Drop is one file a person dropped into a drop connector.
type Drop struct {
	Connector Connector
	// Name is the file's own name: one filename, with no directory.
	Name      string
	MediaType string
	Body      []byte
}

// DropHandler lands one drop, and answers each arrival it landed, the file's
// own item first.
type DropHandler func(ctx context.Context, l *landing.Lander, d Drop) ([]landing.Landed, error)

// dropHandlers holds each registered drop handler by its format name.
var dropHandlers = map[string]DropHandler{}

// RegisterDropHandler registers h under format, which a drop connector names
// as its format. A package whose files split into items of their own, such as
// an export holding many saves, registers its handler from its own init.
// Registering a format twice panics, as a duplicate is a build error.
func RegisterDropHandler(format string, h DropHandler) {
	if _, taken := dropHandlers[format]; taken {
		panic("connectors: drop format " + format + " is registered twice")
	}
	dropHandlers[format] = h
}

// LandDrop lands d through the handler its connector's format names, or as
// one item where the connector names none.
func LandDrop(ctx context.Context, l *landing.Lander, d Drop) ([]landing.Landed, error) {
	if d.Connector.Format != "" {
		h, ok := dropHandlers[d.Connector.Format]
		if !ok {
			return nil, fmt.Errorf("format %s names no drop handler this build registers", d.Connector.Format)
		}
		return h(ctx, l, d)
	}
	landed, err := l.Land(ctx, landing.Arrival{
		Connector: d.Connector.Name, Zone: d.Connector.Zone, MediaType: d.MediaType, ExternalKey: d.Name,
		Tags: d.Connector.Tags, Body: bytes.NewReader(d.Body),
	})
	if err != nil {
		return nil, err
	}
	return []landing.Landed{landed}, nil
}
