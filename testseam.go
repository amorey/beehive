package beehive

import (
	"context"

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

func (w fixtureWriter) UpdateStatus(ctx context.Context, gk GroupKind, id ObjectID, status []byte) error {
	return w.bh.store.Objects().UpdateStatus(ctx, gk, id, status,
		migratorStatusVersion(w.bh.migratorFor(gk)))
}
