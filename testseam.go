package beehive

import (
	"context"

	"github.com/amorey/beehive/internal/storeapi"
	"github.com/amorey/beehive/internal/testseam"
)

func init() {
	testseam.Open = func(v any) (testseam.Writer, bool) {
		bh, ok := v.(*Beehive)
		if !ok {
			return nil, false
		}
		return fixtureWriter{bh}, true
	}
}

// fixtureWriter is beehivetest's handle on a Beehive. It writes the columns the
// spec/status split reserves for controllers, without a pass to hold.
type fixtureWriter struct{ bh *Beehive }

func (w fixtureWriter) SetConditions(ctx context.Context, gk GroupKind, id ObjectID, conds ...storeapi.Condition) error {
	if err := w.bh.store.Conditions().Set(ctx, gk, id, conds...); err != nil {
		return err
	}
	w.bh.signalKindWritten(ctx, gk)
	return nil
}

func (w fixtureWriter) UpdateStatus(ctx context.Context, gk GroupKind, id ObjectID, status []byte) error {
	if err := w.bh.store.Objects().UpdateStatus(ctx, gk, id, status,
		migratorStatusVersion(w.bh.migratorFor(gk))); err != nil {
		return err
	}
	w.bh.signalKindWritten(ctx, gk)
	return nil
}
