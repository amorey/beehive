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

// Referrer is an object pointing at a target through a ref edge, with the
// GroupKind needed to route a requeue.
type Referrer = storeapi.Referrer

// Relation is the kind of edge in the refs table.
type Relation = storeapi.Relation

const (
	RelationOwnedBy   = storeapi.RelationOwnedBy
	RelationDependsOn = storeapi.RelationDependsOn
)

// Watcher is a closeable subscription to a kind's change stream, returned by the
// store's Watch/WatchList. The client decodes its raw events into the typed
// WatchEvent[Spec, Status] surface.
type Watcher = storeapi.Watcher

// EventWatcher is a subscription to one object's event log, returned by the
// store's WatchEvents. The client decodes its raw runs into public Events.
type EventWatcher = storeapi.EventWatcher

// WatchEventType classifies a WatchEvent.
type WatchEventType = storeapi.WatchEventType

const (
	WatchEventAdded    = storeapi.WatchEventAdded
	WatchEventModified = storeapi.WatchEventModified
	WatchEventDeleted  = storeapi.WatchEventDeleted
)
