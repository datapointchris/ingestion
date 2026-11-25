package connectors

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/datapointchris/ingestion/landing"
	"github.com/datapointchris/ingestion/store"
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
// own item first. It marks with Refused an error that the file itself causes.
type DropHandler func(ctx context.Context, l *landing.Lander, d Drop) ([]landing.Landed, error)

// dropFormat is a registered drop handler and the zone of every item it lands.
type dropFormat struct {
	zone store.Zone
	land DropHandler
}

// dropFormats holds each registered drop format by its name.
var dropFormats = map[string]dropFormat{}

// RegisterDropHandler registers h under format, which a drop connector names
// as its format. Every item that h lands takes zone. Where a connector names
// format, it declares zone, or the set is refused. A package whose files split into items
// of their own, such as an export holding many saves, registers its handler
// from its own init. Registering a format twice panics, as a duplicate is a
// build error.
func RegisterDropHandler(format string, zone store.Zone, h DropHandler) {
	if _, taken := dropFormats[format]; taken {
		panic("connectors: drop format " + format + " is registered twice")
	}
	dropFormats[format] = dropFormat{zone: zone, land: h}
}

// Refused marks err as the refusal of a dropped file for what it holds, so
// the same file sent again lands nothing more. The drop route answers a
// refusal as the caller's error, and any other error as the server's own.
func Refused(err error) error { return refusedError{err} }

// IsRefused says whether Refused marked err.
func IsRefused(err error) bool { return errors.As(err, new(refusedError)) }

type refusedError struct{ error }

func (e refusedError) Unwrap() error { return e.error }

// LandDrop lands d through the handler its connector's format names, or as
// one item where the connector names none.
func LandDrop(ctx context.Context, l *landing.Lander, d Drop) ([]landing.Landed, error) {
	if d.Connector.Format != "" {
		f, ok := dropFormats[d.Connector.Format]
		if !ok {
			return nil, Refused(fmt.Errorf("format %s names no drop handler this build registers", d.Connector.Format))
		}
		return f.land(ctx, l, d)
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
