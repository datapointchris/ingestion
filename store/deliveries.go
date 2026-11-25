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

// QueueDelivery queues the task that writes the item under key to destination
// through deliverer, and records the delivery pending where it is new. It
// queues nothing where the delivery is settled, because a destination can
// forget a record it held, so a second write could save the item again. It
// queues nothing where a deliver task for the item and destination is queued
// or leased. It answers whether it queued.
func (t *Tx) QueueDelivery(ctx context.Context, key, destination, deliverer string) (bool, error) {
	held, ok, err := t.Delivery(ctx, key, destination)
	if err != nil || (ok && held.Settled()) {
		return false, err
	}
	inFlight, err := readInto[Task](ctx, t.tx, table("tasks"),
		`item_key = ? AND stage = ? AND destination = ? AND state IN ('queued', 'leased')`, key, StageDeliver, destination)
	if err != nil || len(inFlight) > 0 {
		return false, err
	}
	if !ok {
		if err := t.PutDelivery(ctx, Delivery{ItemKey: key, Destination: destination, Outcome: DeliveryPending}); err != nil {
			return false, err
		}
	}
	_, err = t.Enqueue(ctx, Task{ItemKey: &key, Destination: &destination, Stage: StageDeliver, Capability: DeliverCapability(deliverer)})
	return err == nil, err
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

// UndeliveredAsIsKeys reads the key of each item whose newest read decision
// reads it as is and that holds no delivery: one whose placement named no
// destination a deliverer writes to when it was read.
func (t *Tx) UndeliveredAsIsKeys(ctx context.Context) ([]string, error) {
	rows, err := t.tx.QueryContext(ctx, `
SELECT i.key FROM items i
WHERE (SELECT d.verdict FROM decisions d WHERE d.item_key = i.key AND d.gate = 'read' ORDER BY d.sequence DESC LIMIT 1) = 'as-is'
	AND NOT EXISTS (SELECT 1 FROM deliveries x WHERE x.item_key = i.key)
ORDER BY i.key`)
	if err != nil {
		return nil, fmt.Errorf("select the items read as is that hold no delivery: %w", err)
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
