// Package testseam hands beehivetest the writes only a controller's client can
// otherwise make. Package beehive sets Open; nothing else may.
package testseam

import (
	"context"

	"github.com/amorey/beehive/internal/storeapi"
)

// Writer is what beehivetest needs from a *beehive.Beehive. The status blob
// arrives marshalled: keeping this interface non-generic leaves the type
// parameter in beehivetest.
type Writer interface {
	UpdateStatus(ctx context.Context, gk storeapi.GroupKind, id storeapi.ObjectID, status []byte) error
}

// Open is set by package beehive's init and read by beehivetest. The parameter
// is any because this package cannot import beehive without a cycle.
var Open func(bh any) (Writer, bool)
