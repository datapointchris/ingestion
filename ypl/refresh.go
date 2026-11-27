package ypl

import (
	"context"

	"github.com/datapointchris/ingestion/metricsrefresh"
)

// What a refresh passes over, by the name its log line counts it under.
const (
	// countNotAVideo is a key that names no video, which nothing of this
	// source lands.
	countNotAVideo = "not_a_video"
	// countUnreported is a video that ypl left out of its answer, such as a
	// deleted or private one, which keeps the metrics it has.
	countUnreported = "unreported"
	// countUnread is a video in a request that failed, which a retry of the
	// refresh reads.
	countUnread = "unread"
)

// countsKeptOverZero are the counts that ypl answers as 0 where YouTube hides
// them, as an owner can hide likes. A refresh leaves out each one that reads
// 0, so a count that YouTube hides keeps the one measured before.
var countsKeptOverZero = []string{MetricViews, MetricLikes, MetricComments}

// Refresher reads again the metrics of the videos that a refresh names,
// through ypl's details read, MaxVideos to a request.
type Refresher struct{ client *Client }

// NewRefresher is the refresher that reads ypl through client.
func NewRefresher(client *Client) Refresher { return Refresher{client: client} }

// Refresh reads the details of each video in keys, and answers each one's
// views, likes, comments and length by its key. Where a request fails, the
// answer keeps every video that the requests before it read.
func (r Refresher) Refresh(ctx context.Context, keys []string) metricsrefresh.Outcome {
	out := metricsrefresh.Outcome{Metrics: map[string]map[string]int64{}, Counts: map[string]int{}}
	keyOf := map[string]string{}
	var ids []string
	for _, key := range keys {
		id, err := videoID(key)
		if err != nil {
			out.Counts[countNotAVideo]++
			continue
		}
		if _, held := keyOf[id]; !held {
			keyOf[id] = key
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return out
	}
	videos, err := r.client.Videos(ctx, ids)
	for _, v := range videos {
		if key, ok := keyOf[v.ID]; ok {
			out.Metrics[key] = refreshed(v)
		}
	}
	if err != nil {
		reason := err.Error()
		f := classify(err)
		out.Error, out.Wait, out.NoAnswer, out.Final = &reason, f.Wait, f.Wait > 0 && !f.Asked, f.Final
		out.Counts[countUnread] = len(ids) - len(out.Metrics)
		return out
	}
	out.Counts[countUnreported] = len(ids) - len(out.Metrics)
	return out
}

// refreshed is v's metrics as a refresh keeps them, with no count in
// countsKeptOverZero that reads 0.
func refreshed(v Video) map[string]int64 {
	metrics := v.metrics()
	for _, name := range countsKeptOverZero {
		if metrics[name] == 0 {
			delete(metrics, name)
		}
	}
	return metrics
}
