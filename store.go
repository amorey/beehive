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

// ObjectsSubscription is a closeable subscription to a kind's change stream,
// returned by the store's ObjectsWatch/ObjectsWatchList. The client decodes its
// raw changes into the typed ObjectChange[Spec, Status] surface.
type ObjectsSubscription = storeapi.ObjectsSubscription

// EventsSubscription is a subscription to one object's event log, returned by the
// store's EventsWatch. The client decodes its raw runs into public Events.
type EventsSubscription = storeapi.EventsSubscription

// ObjectWritesSubscription is a subscription to the store-wide write stream,
// returned by the store's ObjectWritesSubscribe. It is internal machinery for the
// dependency waker, which needs only identity — hence batches of blob-free
// ObjectWrites.
type ObjectWritesSubscription = storeapi.ObjectWritesSubscription

// ObjectWrite names one written object and how it changed, with no row attached.
// The id is the object's — a write has no identity of its own.
type ObjectWrite = storeapi.ObjectWrite

// ObjectWriteBatch is one delivery from the store-wide write stream: the writes
// that were ready together, plus how far behind them the backlog reaches.
type ObjectWriteBatch = storeapi.ObjectWriteBatch

// ChangeType classifies a Change.
type ChangeType = storeapi.ChangeType

const (
	Added    = storeapi.Added
	Modified = storeapi.Modified
	Deleted  = storeapi.Deleted
)
