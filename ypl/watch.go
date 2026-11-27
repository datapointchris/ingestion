package ypl

import (
	"context"
	"fmt"
	"net/http"
	"slices"

	"github.com/datapointchris/ingestion/deliver"
	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/store"
)

// Watch is the deliverer that adds a video to the playlist that a person
// picked when they sorted it to watch, through ypl's playlist-item insert.
// YouTube's Data API cannot write the Watch Later list, so a watch names a
// playlist of the channel's own. It opens on the source's client, which holds
// ypl.playlist-items.write, so it needs no [deliver] table of its own.
var Watch = deliver.Registration{
	Name:     route.WatchDeliverer,
	Takes:    watchTakes,
	Playlist: true,
	Open: func(api string, client *http.Client) deliver.Deliverer {
		return Watcher{client: NewClient(api, client)}
	},
}

// watchTakes answers why a watch cannot add item, which holds only a video.
func watchTakes(item store.Item) error {
	if item.MediaType != MediaTypeVideo {
		return fmt.Errorf("a watch adds a YouTube video to a playlist, not %s", item.MediaType)
	}
	_, err := videoID(item.Key)
	return err
}

// Watcher adds each video it delivers to its delivery's playlist.
type Watcher struct{ client *Client }

// Deliver inserts the video into the playlist that the delivery kept. An
// insert spends ypl's Data API quota, and YouTube keeps a second copy of a
// video that a playlist holds already. The delivery's row, which the service
// never queues again once settled, keeps a later sort from adding it twice.
// An attempt past the first follows one whose insert may have landed with its
// answer lost, so it reads the playlist first and settles on the item it
// finds there. A refusal that no wait clears fails the task, and a wait that
// ypl asks for, as while the day's quota is spent, spends no attempt.
func (w Watcher) Deliver(ctx context.Context, item deliver.Item) deliver.Outcome {
	video, err := videoID(item.Key)
	if err != nil || item.Playlist == nil || *item.Playlist == "" {
		reason := "a watch's delivery names its video and the playlist that the person picked"
		return deliver.Outcome{Outcome: store.DeliveryPending, Error: &reason, Permanent: true}
	}
	if item.Attempt > 1 {
		held, err := w.client.PlaylistItems(ctx, *item.Playlist)
		if err != nil {
			return watchFailed(err)
		}
		if i := slices.IndexFunc(held.Items, func(p PlaylistItem) bool { return p.VideoID == video }); i >= 0 {
			return deliver.Outcome{Outcome: store.DeliveryDelivered, DestinationID: &held.Items[i].ItemID}
		}
	}
	added, err := w.client.AddPlaylistItem(ctx, *item.Playlist, video)
	if err != nil {
		return watchFailed(err)
	}
	return deliver.Outcome{Outcome: store.DeliveryDelivered, DestinationID: &added.ItemID}
}

// watchFailed is the outcome of a call to ypl that err ended, as classify reads it.
func watchFailed(err error) deliver.Outcome {
	reason := err.Error()
	f := classify(err)
	out := deliver.Outcome{Outcome: store.DeliveryPending, Error: &reason, Permanent: f.Final}
	if f.Asked {
		out.RetryAfter = f.Wait
	}
	return out
}
