// Package beehivetest builds store state that only a controller can otherwise
// write. For fixtures; not for production code.
//
// Reach for it last. A controller test that needs only the object it is handed
// should call Reconcile directly against a fake ControllerClient — no store, no
// beehive, and the assertion lands on what the pass decided. This package is
// for the case that stops covering: a pass that reads another kind's status out
// of a real store, which a fixture has no other way to write.
package beehivetest

import (
	"context"
	"encoding/json"

	"github.com/amorey/beehive"
	"github.com/amorey/beehive/internal/storeapi"
	"github.com/amorey/beehive/internal/testseam"
)

// Client writes one kind's status. Valid for as long as the Beehive it was
// built from.
type Client[Status any] struct {
	writer testseam.Writer
	gk     beehive.GroupKind
}

// NewClient returns a client for gk's status. Needs no registered controller
// and no running beehive. Panics on a nil Beehive.
func NewClient[Status any](bh *beehive.Beehive, gk beehive.GroupKind) *Client[Status] {
	w, ok := testseam.Open(bh)
	if !ok {
		panic("beehivetest: NewClient needs a non-nil *beehive.Beehive")
	}
	return &Client[Status]{writer: w, gk: gk}
}

// UpdateStatus records status for id, as ControllerClient.UpdateStatus does.
// Never stamps observed_generation: the handshake stays beehive's, so an object
// given a fixture status is still unsettled.
func (c *Client[Status]) UpdateStatus(ctx context.Context, id beehive.ObjectID, status Status) error {
	b, err := json.Marshal(status)
	if err != nil {
		return err
	}
	return c.writer.UpdateStatus(ctx, c.gk, id, b)
}

// DeleteCondition removes id's condition of that type, as
// ControllerClient.DeleteCondition does. A missing condition is a no-op.
func (c *Client[Status]) DeleteCondition(ctx context.Context, id beehive.ObjectID, conditionType string) error {
	return c.writer.DeleteCondition(ctx, c.gk, id, conditionType)
}

// SetCondition writes id's condition of that type, as
// ControllerClient.SetCondition does. The store stamps the times.
func (c *Client[Status]) SetCondition(ctx context.Context, id beehive.ObjectID, cond beehive.Condition) error {
	return c.SetConditions(ctx, id, []beehive.Condition{cond})
}

// SetConditions writes every named condition together, as
// ControllerClient.SetConditions does: one version bump, a type named twice is
// ErrDuplicateConditionType, and an empty slice writes nothing.
func (c *Client[Status]) SetConditions(ctx context.Context, id beehive.ObjectID, conds []beehive.Condition) error {
	if len(conds) == 0 {
		return nil
	}
	// Unconfirmed and the stamps are set by the store on read and ignored on
	// write, so they are not copied.
	out := make([]storeapi.Condition, len(conds))
	for i, cond := range conds {
		out[i] = storeapi.Condition{
			Type:     cond.Type,
			Status:   string(cond.Status),
			Reason:   cond.Reason,
			Message:  cond.Message,
			Liveness: cond.Liveness,
		}
	}
	return c.writer.SetConditions(ctx, c.gk, id, out...)
}
