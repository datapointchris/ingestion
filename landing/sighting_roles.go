package landing

import (
	"fmt"

	"github.com/datapointchris/ingestion/store"
)

// SightingRoles names which of a source's sighting fields and metrics answer
// what a reader asks of any item, whatever its source: who published it, how
// long it runs, and how it was received. A role that the source's sightings
// do not carry is "".
type SightingRoles struct {
	// Channel is the field that names who published the item, and
	// ChannelFormat how a reader shows its value, %s standing for the value.
	Channel, ChannelFormat string
	// ChannelFields are the fields that a drop decided by the channel stands
	// for. A source whose channel a drop names under the field's own name,
	// such as a subreddit, leaves it empty.
	ChannelFields []string
	// Length is the metric of how long the item runs, in seconds. Views,
	// Likes and Comments are the counts of how it was received.
	Length, Views, Likes, Comments string
}

var sightingRoles []SightingRoles

// RegisterSightingRoles records the roles of a source's sighting fields and
// metrics, so ReadRoles reads them off any item that the source sighted.
func RegisterSightingRoles(r SightingRoles) { sightingRoles = append(sightingRoles, r) }

// RoleValues is what an item's sightings answer through the registered roles,
// each nil or empty where no sighting carries it.
type RoleValues struct {
	Channel                         *string
	ChannelFields                   []string
	LengthS, Views, Likes, Comments *int64
}

// ReadRoles reads each role off sightings, the first sighting that carries a
// role answering it.
func ReadRoles(sightings []store.Sighting) RoleValues {
	var out RoleValues
	metric := func(into **int64, s store.Sighting, name string) {
		if v, ok := s.Metrics[name]; ok && name != "" && *into == nil {
			*into = &v
		}
	}
	for _, s := range sightings {
		for _, r := range sightingRoles {
			if v := s.Fields[r.Channel]; v != "" && r.Channel != "" && out.Channel == nil {
				channel := fmt.Sprintf(r.ChannelFormat, v)
				out.Channel, out.ChannelFields = &channel, r.ChannelFields
			}
			metric(&out.LengthS, s, r.Length)
			metric(&out.Views, s, r.Views)
			metric(&out.Likes, s, r.Likes)
			metric(&out.Comments, s, r.Comments)
		}
	}
	return out
}
