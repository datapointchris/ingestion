package connectors

import (
	"bytes"
	"context"

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

// dropHandlers holds the handler for each source whose file splits into items
// of its own, by the source's name. A source with no entry lands its file as
// one item.
var dropHandlers = map[string]DropHandler{}

// LandDrop lands d through its connector's source's handler.
func LandDrop(ctx context.Context, l *landing.Lander, d Drop) ([]landing.Landed, error) {
	if h, ok := dropHandlers[d.Connector.Source]; ok {
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
