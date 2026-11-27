package ypl

import (
	"context"
	"net/http"

	"github.com/datapointchris/ingestion/comments"
)

// CommentsSource reads a video's top comments through ypl, for the comments
// stage.
var CommentsSource = comments.Source{
	MediaType: MediaTypeVideo,
	Open:      func(api string, client *http.Client) comments.Reader { return commentsReader{NewClient(api, client)} },
}

type commentsReader struct{ client *Client }

// Comments reads the top comments of the video keyed youtube/<id>.
func (r commentsReader) Comments(ctx context.Context, key string) (comments.Read, error) {
	id, err := videoID(key)
	if err != nil {
		return comments.Read{}, err
	}
	answer, err := r.client.Comments(ctx, id)
	if err != nil {
		return comments.Read{}, err
	}
	read := comments.Read{Disabled: answer.CommentsDisabled}
	for _, c := range answer.Comments {
		read.Threads = append(read.Threads, comments.Thread{Text: c.Text, Likes: c.LikeCount, Replies: c.ReplyCount})
	}
	return read, nil
}
