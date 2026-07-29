// Copyright 2026 Andres Morey
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package beehive

import "github.com/amorey/beehive/internal/storeapi"

// Store is the durable-store contract Beehive depends on internally. It is
// non-generic and deals only in raw rows: the generic-to-non-generic boundary
// lives one layer up, in the typedController adapter.
//
// Mutators return the freshly written row so callers see the store-assigned
// id, resource_version, and timestamps without a re-read.
type Store = storeapi.Store

// RawObject is the untyped row below the generic boundary. Spec and Status are
// opaque JSON bytes; everything else is Beehive-owned metadata that mirrors the
// objects table. The reconciler and client decode Spec/Status into typed
// Object[Spec, Status] values; the Store never inspects them.
type RawObject = storeapi.RawObject

// RawEvent is the untyped event-log row below the generic boundary — one
// aggregated run. The client decodes it into the public Event.
type RawEvent = storeapi.Event

// ErrNotFound is returned by Store reads when no object matches.
var ErrNotFound = storeapi.ErrNotFound

// ErrConcurrentNestedTx is returned by a nested Within entered from a goroutine other
// than the one owning the enclosing frame. Deep nesting on one goroutine is fine; two
// goroutines nesting on the same transaction is not.
var ErrConcurrentNestedTx = storeapi.ErrConcurrentNestedTx

// ErrObservedGenerationFuture is returned by UpdateStatus when the caller reports
// a generation greater than the object's current one — a convergence-handshake
// violation (a controller must pass the generation it received in Reconcile).
var ErrObservedGenerationFuture = storeapi.ErrObservedGenerationFuture

// ErrSchemaVersionDowngrade is returned by ObjectsUpdateSpec/UpdateStatus when the
// caller's schema version is lower than the one stamped on the row — the
// write-side twin of the read path's refusal to decode data a newer build wrote.
var ErrSchemaVersionDowngrade = storeapi.ErrSchemaVersionDowngrade

// Relation is the kind of edge in the edges table.
type Relation = storeapi.Relation

const (
	RelationOwnedBy   = storeapi.RelationOwnedBy
	RelationDependsOn = storeapi.RelationDependsOn
)

// ObjectWrite names one object whose version is above a scan's cursor, with no
// row attached. It is what a scan of the store's write log returns, which is the
// only change-notification path there is.
type ObjectWrite = storeapi.ObjectWrite

// ChangeType classifies a Change.
type ChangeType = storeapi.ChangeType

const (
	Added    = storeapi.Added
	Modified = storeapi.Modified
	Deleted  = storeapi.Deleted
)
