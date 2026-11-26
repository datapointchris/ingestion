package ypl

import (
	"context"
	"errors"
	"net/http"

	"github.com/datapointchris/ingestion/deletion"
)

// Deleter removes a landed save from the playlist that held it, through ypl's
// playlist-item delete. Each delete costs the day's quota 50 units.
type Deleter struct{ client *Client }

// NewDeleter is the deleter that writes through client.
func NewDeleter(client *Client) Deleter { return Deleter{client: client} }

// Delete removes the sighting's playlist item. ypl answers 404 for a playlist
// that YouTube does not hold, and its item is then gone, so that counts as
// deleted. Where ypl asks the delete to wait, as while the day's quota is
// spent, the outcome carries that wait.
func (d Deleter) Delete(ctx context.Context, s deletion.Sighting) deletion.Outcome {
	playlist := s.Fields[FieldPlaylistID]
	if playlist == "" || s.ExternalKey == "" || s.ExternalKey == s.ItemKey {
		reason := "the sighting names no playlist item to delete, as a playlist's poll records its " + FieldPlaylistID + " and item id"
		return deletion.Outcome{Error: &reason}
	}
	err := d.client.DeletePlaylistItem(ctx, playlist, s.ExternalKey)
	if err == nil || refusedWith(err, http.StatusNotFound) {
		return deletion.Outcome{}
	}
	reason := err.Error()
	out := deletion.Outcome{Error: &reason}
	var refusal *Error
	if errors.As(err, &refusal) {
		out.RetryAfter = refusal.RetryAfter
	}
	return out
}
