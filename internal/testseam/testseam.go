// Package testseam hands beehivetest the writes only a controller's client can
// otherwise make. Package beehive sets Open; nothing else may.
package testseam

import (
	"context"

	"github.com/amorey/beehive/internal/storeapi"
)

// Writer is one kind's status half, already scoped to a GroupKind. The status
// blob arrives marshalled, which leaves the type parameter in beehivetest.
type Writer interface {
	DeleteCondition(ctx context.Context, id storeapi.ObjectID, conditionType string) error
	SetConditions(ctx context.Context, id storeapi.ObjectID, conds ...storeapi.Condition) error
	UpdateStatus(ctx context.Context, id storeapi.ObjectID, status []byte) error
}

// Open is set by package beehive's init and read by beehivetest. The beehive is
// an any because this package cannot import beehive without a cycle.
var Open func(bh any, gk storeapi.GroupKind) Writer
