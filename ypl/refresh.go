package ypl

import (
	"context"
	"errors"
	"net/http"
	"net/url"

	"golang.org/x/oauth2"

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
)

// Refresher reads again the metrics of the videos that a refresh names,
// through ypl's details read, MaxVideos to a request. ypl charges one quota
// unit for each page of 50 ids.
type Refresher struct{ client *Client }

// NewRefresher is the refresher that reads ypl through client.
func NewRefresher(client *Client) Refresher { return Refresher{client: client} }

// Refresh reads the details of each video in keys, and answers each one's
// views, likes, comments and length by its key.
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
	if err != nil {
		return refreshFailed(err)
	}
	for _, v := range videos {
		if key, ok := keyOf[v.ID]; ok {
			out.Metrics[key] = v.metrics()
		}
	}
	out.Counts[countUnreported] = len(ids) - len(out.Metrics)
	return out
}

// refreshFailed is a refresh that err stopped. Where ypl sent a Retry-After,
// as while its day's quota is spent, or was busy, paused or limiting the
// caller, the refresh waits. It waits too where ypl gave no answer, as while
// it restarts or a gateway before it fails. A refusal of the request itself,
// any 4xx but 408 and 429, is final. Any other failure, such as ypl failing to
// read YouTube or the identity provider refusing the token, reads again
// boundedly.
func refreshFailed(err error) metricsrefresh.Outcome {
	reason := err.Error()
	out := metricsrefresh.Outcome{Error: &reason}
	var refusal *Error
	var token *oauth2.RetrieveError
	var unanswered *url.Error
	switch {
	case errors.As(err, &token):
	case errors.As(err, &refusal) && refusal.RetryAfter > 0:
		out.Wait = refusal.RetryAfter
	case errors.As(err, &refusal) && (waitCodes[refusal.Code] || refusal.Status == http.StatusTooManyRequests):
		out.Wait = busyRetry
	case errors.As(err, &refusal) && refusal.Code == "" &&
		(refusal.Status == http.StatusRequestTimeout || refusal.Status >= http.StatusInternalServerError):
		out.Wait = unansweredRetry
	case errors.As(err, &refusal):
		out.Final = refusal.Status >= http.StatusBadRequest && refusal.Status < http.StatusInternalServerError &&
			refusal.Status != http.StatusRequestTimeout
	case errors.As(err, &unanswered):
		out.Wait = unansweredRetry
	}
	return out
}
