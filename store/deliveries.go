package store

import (
	"context"
	"fmt"
	"slices"
)

// Settled says whether a delivery's outcome is final: the destination holds
// the item, or refused it naming the record that holds it. Nothing delivers a
// settled delivery again.
func (d Delivery) Settled() bool { return d.Outcome != DeliveryPending }

// Delivery reads the item's delivery to destination, and false where the store
// holds none.
func (t *Tx) Delivery(ctx context.Context, key, destination string) (Delivery, bool, error) {
	return first[Delivery](ctx, t.tx, table("deliveries"), `item_key = ? AND destination = ?`, key, destination)
}

// Deliveries reads each of the item's deliveries, one per destination.
func (t *Tx) Deliveries(ctx context.Context, key string) ([]Delivery, error) {
	return readInto[Delivery](ctx, t.tx, table("deliveries"), `item_key = ? ORDER BY destination`, key)
}

// QueueDelivery queues the task that writes item to destination through
// deliverer, and records the delivery pending where it is new, with the
// item's title, URL and source time, path and chapter as every attempt sends
// them. path is the item's file below the deliverer's root, and nil for a
// deliverer that writes no file. chapter is where a book's chapter sits in its
// book's record, and nil for every other item. It queues nothing where the
// delivery is settled, because a destination can forget a record it held, so
// a second write could save the item again. It queues nothing where a deliver
// task for the item and destination is queued or leased. It answers whether
// it queued.
func (t *Tx) QueueDelivery(ctx context.Context, item Item, path *string, chapter *ChapterInBook, destination, deliverer string) (bool, error) {
	key := item.Key
	held, ok, err := t.Delivery(ctx, key, destination)
	if err != nil || (ok && held.Settled()) {
		return false, err
	}
	inFlight, err := t.deliverTasks(ctx, key, `state IN ('queued', 'leased') AND destination = ?`, destination)
	if err != nil || len(inFlight) > 0 {
		return false, err
	}
	if !ok {
		d := Delivery{
			ItemKey: key, Destination: destination, Outcome: DeliveryPending, Title: item.Title, URL: item.URL, SourceTS: item.SourceTS,
			Path: path,
		}
		if chapter != nil {
			d.BookDestinationID, d.PDFPageStart, d.PDFPageEnd = &chapter.BookDestinationID, &chapter.PDFPageStart, &chapter.PDFPageEnd
			d.PrintedPageStart, d.PrintedPageEnd = chapter.PrintedPageStart, chapter.PrintedPageEnd
		}
		if err := t.PutDelivery(ctx, d); err != nil {
			return false, err
		}
	}
	_, err = t.Enqueue(ctx, Task{ItemKey: &key, Destination: &destination, Stage: StageDeliver, Capability: DeliverCapability(deliverer)})
	return err == nil, err
}

// WithdrawDeliveries ends each queued deliver task of the item under key whose
// destination is not keep, and says on its pending delivery why it stopped.
// keep is the destination that the item's decisions now send it to, and "" for
// none, as after a drop. A task already leased runs on, and its report records
// what the destination answered.
func (t *Tx) WithdrawDeliveries(ctx context.Context, key, keep string) error {
	queued, err := t.deliverTasks(ctx, key, `state = 'queued' AND destination != ?`, keep)
	if err != nil {
		return err
	}
	for _, task := range queued {
		why := fmt.Sprintf("withdrawn: the item's newest placement no longer sends it to %s", *task.Destination)
		if _, err := t.tx.ExecContext(ctx, `UPDATE tasks SET state = 'done', error = ?, updated_ts = ? WHERE id = ?`,
			why, t.stamp(), task.ID); err != nil {
			return fmt.Errorf("withdraw task %s: %w", task.ID, err)
		}
		d, ok, err := t.Delivery(ctx, key, *task.Destination)
		if err != nil {
			return err
		}
		if ok && !d.Settled() {
			d.LastError = &why
			if err := t.PutDelivery(ctx, d); err != nil {
				return err
			}
		}
	}
	return nil
}

func (t *Tx) deliverTasks(ctx context.Context, key, where string, args ...any) ([]Task, error) {
	return readInto[Task](ctx, t.tx, table("tasks"), `item_key = ? AND stage = ? AND `+where,
		append([]any{key, StageDeliver}, args...)...)
}

// DeliveryAttempt is how one run of a deliver task ended: the outcome, the
// destination's own id for the record that holds the item, and the
// destination's answer where it is pending or refused.
type DeliveryAttempt struct {
	Outcome       string
	DestinationID *string
	Error         *string
}

// RecordDelivery writes how the deliver task's run ended into its delivery,
// counting the attempt. A pending attempt keeps the id an earlier one found.
func (t *Tx) RecordDelivery(ctx context.Context, task Task, a DeliveryAttempt) error {
	if task.ItemKey == nil || task.Destination == nil || task.Stage != StageDeliver {
		return fmt.Errorf("task %s is no deliver task, so it records no delivery", task.ID)
	}
	if !slices.Contains([]string{DeliveryPending, DeliveryDelivered, DeliveryDeliveredToExisting, DeliveryRefused}, a.Outcome) {
		return fmt.Errorf("a delivery ends pending, delivered, delivered-to-existing or refused, not %q", a.Outcome)
	}
	d, _, err := t.Delivery(ctx, *task.ItemKey, *task.Destination)
	if err != nil {
		return err
	}
	d.ItemKey, d.Destination, d.Outcome, d.LastError = *task.ItemKey, *task.Destination, a.Outcome, a.Error
	d.Attempts++
	if a.DestinationID != nil {
		d.DestinationID = a.DestinationID
	}
	return t.PutDelivery(ctx, d)
}

// UnsettledKeysReadAs reads the key of each item whose newest read decision
// has verdict and that holds no settled delivery: each item that a changed set
// may now send to a destination.
func (t *Tx) UnsettledKeysReadAs(ctx context.Context, verdict string) ([]string, error) {
	rows, err := t.tx.QueryContext(ctx, `
SELECT i.key FROM items i
WHERE (SELECT d.verdict FROM decisions d WHERE d.item_key = i.key AND d.gate = ? ORDER BY d.sequence DESC LIMIT 1) = ?
	AND NOT EXISTS (SELECT 1 FROM deliveries x WHERE x.item_key = i.key AND x.outcome != ?)
ORDER BY i.key`, GateRead, verdict, DeliveryPending)
	if err != nil {
		return nil, fmt.Errorf("select the items read %s that hold no settled delivery: %w", verdict, err)
	}
	defer func() { _ = rows.Close() }()
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, rows.Err()
}
