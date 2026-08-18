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
