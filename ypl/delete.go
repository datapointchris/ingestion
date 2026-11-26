package ypl

import (
	"context"
	"errors"
	"net/http"

	"github.com/datapointchris/ingestion/deletion"
)

// Deleter removes a landed save from the playlist that held it, through ypl's
// playlist-item delete.
type Deleter struct{ client *Client }

// NewDeleter is the deleter that writes through client.
func NewDeleter(client *Client) Deleter { return Deleter{client: client} }

// Delete removes the sighting's playlist item. ypl answers an item already
// gone as deleted, so only its answer that the route ran counts as done.
// Where ypl asks the delete to wait, as while the day's quota is spent, the
// outcome carries that wait. A refusal that no wait clears is final: any 4xx
// but 408 and 429, such as YouTube refusing a write to a playlist that the
// channel cannot edit.
func (d Deleter) Delete(ctx context.Context, s deletion.Sighting) deletion.Outcome {
	playlist := s.Fields[FieldPlaylistID]
	if playlist == "" || s.ExternalKey == "" || s.ExternalKey == s.ItemKey {
		reason := "the sighting names no playlist item to delete, as a playlist's poll records its " + FieldPlaylistID + " and item id"
		return deletion.Outcome{Error: &reason, Final: true}
	}
	err := d.client.DeletePlaylistItem(ctx, playlist, s.ExternalKey)
	if err == nil {
		return deletion.Outcome{}
	}
	reason := err.Error()
	out := deletion.Outcome{Error: &reason}
	var refusal *Error
	if errors.As(err, &refusal) {
		out.RetryAfter = refusal.RetryAfter
		out.Final = refusal.Status >= http.StatusBadRequest && refusal.Status < http.StatusInternalServerError &&
			refusal.Status != http.StatusRequestTimeout && refusal.Status != http.StatusTooManyRequests
	}
	return out
}
