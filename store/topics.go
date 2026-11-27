package store

import (
	"context"
	"fmt"
)

// Topic is one topic of a memo, as topics holds it.
type Topic struct {
	// ItemKey is the memo's key, and ChildKey the topic's item.
	ItemKey  string `json:"item_key"`
	N        int    `json:"n"`
	ChildKey string `json:"child_key"`
	// FirstUnit and LastUnit are the units of the memo's parse that the topic
	// holds, from 1.
	FirstUnit int    `json:"first_unit"`
	LastUnit  int    `json:"last_unit"`
	Title     string `json:"title"`
	Idea      string `json:"idea"`
	// Producer and ProducerVersion name the topics output that answered, and
	// InputProducer and InputVersion the parse that it split.
	Producer        string `json:"producer"`
	ProducerVersion string `json:"producer_version"`
	InputProducer   string `json:"input_producer"`
	InputVersion    string `json:"input_version"`
	CreatedTS       string `json:"created_ts"`
}

// PutTopics writes a memo's topics, once. Where the memo already holds
// topics, they stay as they are, and PutTopics answers false.
func (t *Tx) PutTopics(ctx context.Context, topics []Topic) (bool, error) {
	if len(topics) == 0 {
		return false, nil
	}
	memo := topics[0].ItemKey
	var held int
	if err := t.tx.QueryRowContext(ctx, `SELECT count(*) FROM topics WHERE item_key = ?`, memo).Scan(&held); err != nil {
		return false, fmt.Errorf("count %s's topics: %w", memo, err)
	}
	if held > 0 {
		return false, nil
	}
	for i, topic := range topics {
		if topic.ItemKey != memo || topic.N != i+1 {
			return false, fmt.Errorf("one write holds one memo's topics, numbered from 1, not %s #%d", topic.ItemKey, topic.N)
		}
		topic.CreatedTS = t.stamp()
		if err := t.put(ctx, table("topics"), topic); err != nil {
			return false, err
		}
	}
	return true, nil
}

// Topics reads the topics of the memo under key, in order.
func (t *Tx) Topics(ctx context.Context, key string) ([]Topic, error) {
	return readInto[Topic](ctx, t.tx, table("topics"), `item_key = ? ORDER BY n`, key)
}
