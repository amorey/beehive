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

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/amorey/beehive/internal/storeapi"
)

// WatchEvent reports a change to a watched object.
type WatchEvent[Spec, Status any] struct {
	Type   WatchEventType
	Object *Object[Spec, Status]
}

// Client is the user-facing API for a single resource kind: the surface for
// creating, reading, updating, deleting, and watching objects.
type Client[Spec, Status any] interface {
	Create(ctx context.Context, spec Spec, opts ...Option) (*Object[Spec, Status], error)
	CreateOrUpdate(ctx context.Context, slug string, spec Spec) (*Object[Spec, Status], error)
	Update(ctx context.Context, id ObjectID, spec Spec) (*Object[Spec, Status], error)
	Get(ctx context.Context, id ObjectID, loads ...LoadOption) (*Object[Spec, Status], error)
	GetBySlug(ctx context.Context, slug string, loads ...LoadOption) (*Object[Spec, Status], error)
	List(ctx context.Context, loads ...LoadOption) ([]*Object[Spec, Status], error)
	Delete(ctx context.Context, id ObjectID) error
	Watch(ctx context.Context, id ObjectID) (<-chan WatchEvent[Spec, Status], error)
	WatchList(ctx context.Context) (<-chan WatchEvent[Spec, Status], error)

	// GetOwner returns id's owner, if any. ok reports presence: false (with a nil
	// error) when the object simply has no owner. The lazy counterpart to
	// LoadOwner() — fetch the owner only when it is actually needed.
	GetOwner(ctx context.Context, id ObjectID) (Ref, bool, error)
	// ListDependencies returns the objects id depends on (its outgoing depends_on
	// edges). The lazy counterpart to LoadDependencies().
	ListDependencies(ctx context.Context, id ObjectID) ([]Ref, error)
	// ListDependents returns the objects that depend on id (incoming depends_on).
	// The lazy counterpart to LoadDependents().
	ListDependents(ctx context.Context, id ObjectID) ([]Ref, error)
	// ListOwned returns the objects id owns (its incoming owned_by edges). The
	// lazy counterpart to LoadOwned().
	ListOwned(ctx context.Context, id ObjectID) ([]Ref, error)
}

// NewClient returns a Client for the given resource kind. Spec and Status must
// match the controller registered for gk.
func NewClient[Spec, Status any](bh *Beehive, gk GroupKind) Client[Spec, Status] {
	return &clientImpl[Spec, Status]{bh: bh, gk: gk}
}

type clientImpl[Spec, Status any] struct {
	bh *Beehive
	gk GroupKind
}

// decode turns a store row into the typed object, applying this kind's migrator
// (if any) at the decode boundary. Every read path routes through it so the
// client and the reconciler share one migrator per kind.
func (c *clientImpl[Spec, Status]) decode(raw *RawObject) (*Object[Spec, Status], error) {
	return rawToTyped[Spec, Status](raw, c.bh.migratorFor(c.gk))
}

func (c *clientImpl[Spec, Status]) Create(ctx context.Context, spec Spec, opts ...Option) (*Object[Spec, Status], error) {
	b, err := json.Marshal(spec)
	if err != nil {
		return nil, err
	}
	co := &createOptions{}
	for _, o := range opts {
		if err := o(co); err != nil {
			return nil, err
		}
	}

	var raw *RawObject
	// Within keeps the insert and its owner ref atomic, so a crash between them
	// can't leave an ownerless child the GC path would never collect.
	err = c.bh.store.Within(ctx, func(ctx context.Context) error {
		raw, err = c.bh.store.CreateObject(ctx, &RawObject{
			Group:       c.gk.Group,
			Kind:        c.gk.Kind,
			Slug:        co.slug,
			Spec:        b,
			SpecVersion: migratorSpecVersion(c.bh.migratorFor(c.gk)),
			Finalizers:  co.finalizers,
		})
		if err != nil {
			return err
		}
		// The child owns the edge (child -> owner) so the owner's GC walk finds it
		// via ListIncomingRefs(owner, RelationOwnedBy).
		if co.owner != nil {
			return c.bh.store.AddRef(ctx, raw.ID, *co.owner, RelationOwnedBy)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	obj, err := c.decode(raw)
	if err != nil {
		return nil, err
	}
	// The store emitted the Added event inside CreateObject, before this enqueue,
	// so a fast controller can't produce a Modified event ahead of the Added.
	c.bh.enqueueIfRegistered(c.gk, raw.ID)
	return obj, nil
}

// CreateOrUpdate idempotently reconciles the object named by slug to spec: it
// updates the existing object carrying that slug, or creates one with that slug
// if none exists. Wrapping the read-then-write in Within makes the upsert atomic,
// so concurrent callers can't both insert the same slug — the second sees the
// first's row and updates instead. Re-applying the same spec is a no-op (UpdateSpec
// suppresses the generation bump on equal bytes).
//
// It drives the store mutators directly rather than composing Create/Update so the
// reconciler is woken only after Within commits and flushes the spec watch event:
// those methods enqueue internally, which inside the outer transaction would wake
// a reconciler that could publish a status event ahead of the still-buffered spec
// event. Here the single enqueue runs after Within returns, preserving the
// spec-event-before-wake ordering Create and Update keep on their own.
func (c *clientImpl[Spec, Status]) CreateOrUpdate(ctx context.Context, slug string, spec Spec) (*Object[Spec, Status], error) {
	b, err := json.Marshal(spec)
	if err != nil {
		return nil, err
	}
	var raw *RawObject
	err = c.bh.store.Within(ctx, func(ctx context.Context) error {
		specVersion := migratorSpecVersion(c.bh.migratorFor(c.gk))
		existing, err := c.bh.store.GetObjectBySlug(ctx, c.gk, slug)
		switch {
		case err == nil:
			raw, err = c.bh.store.UpdateSpec(ctx, c.gk, existing.ID, b, specVersion)
		case errors.Is(err, ErrNotFound):
			raw, err = c.bh.store.CreateObject(ctx, &RawObject{
				Group:       c.gk.Group,
				Kind:        c.gk.Kind,
				Slug:        &slug,
				Spec:        b,
				SpecVersion: specVersion,
			})
		}
		// A non-NotFound read error falls through both cases with raw unset; err
		// still carries it. Both write branches reassign err.
		return err
	})
	if err != nil {
		return nil, err
	}
	obj, err := c.decode(raw)
	if err != nil {
		return nil, err
	}
	c.bh.enqueueIfRegistered(c.gk, raw.ID)
	return obj, nil
}

func (c *clientImpl[Spec, Status]) Update(ctx context.Context, id ObjectID, spec Spec) (*Object[Spec, Status], error) {
	b, err := json.Marshal(spec)
	if err != nil {
		return nil, err
	}
	// UpdateSpec folds this client's kind into the write, so a foreign id is
	// rejected at the store (no separate read-then-write to keep atomic);
	// hideWrongKind keeps that foreign id invisible to this single-kind client.
	raw, err := c.bh.store.UpdateSpec(ctx, c.gk, id, b, migratorSpecVersion(c.bh.migratorFor(c.gk)))
	if err = c.hideWrongKind(err); err != nil {
		return nil, err
	}
	obj, err := c.decode(raw)
	if err != nil {
		return nil, err
	}
	c.bh.enqueueIfRegistered(c.gk, raw.ID)
	return obj, nil
}

func (c *clientImpl[Spec, Status]) Get(ctx context.Context, id ObjectID, loads ...LoadOption) (*Object[Spec, Status], error) {
	raw, err := c.scopedGet(ctx, id)
	if err != nil {
		return nil, err
	}
	obj, err := c.decode(raw)
	if err != nil {
		return nil, err
	}
	if err := loadObjectRelated(ctx, c.bh.store, obj, resolveLoads(loads)); err != nil {
		return nil, err
	}
	return obj, nil
}

// scopedGet loads id and confirms it belongs to this client's kind. A Client is
// the surface for a single resource kind, so an id naming an object of another
// kind must be invisible here — reads, updates, and deletes through this client
// must never touch another controller's rows. On the read path GetObject isn't
// kind-scoped, so the client checks here, reporting ErrNotFound for a foreign id.
func (c *clientImpl[Spec, Status]) scopedGet(ctx context.Context, id ObjectID) (*RawObject, error) {
	raw, err := c.bh.store.GetObject(ctx, id)
	if err != nil {
		return nil, err
	}
	if raw.Group != c.gk.Group || raw.Kind != c.gk.Kind {
		return nil, ErrNotFound
	}
	return raw, nil
}

// hideWrongKind keeps a foreign id invisible through this single-kind client: the
// scoped store writes reject another kind's object with ErrWrongKind, which the
// client reports as ErrNotFound (mirrors scopedGet on the read path). Any other
// error passes through unchanged.
func (c *clientImpl[Spec, Status]) hideWrongKind(err error) error {
	if errors.Is(err, ErrWrongKind) {
		return ErrNotFound
	}
	return err
}

func (c *clientImpl[Spec, Status]) GetBySlug(ctx context.Context, slug string, loads ...LoadOption) (*Object[Spec, Status], error) {
	raw, err := c.bh.store.GetObjectBySlug(ctx, c.gk, slug)
	if err != nil {
		return nil, err
	}
	obj, err := c.decode(raw)
	if err != nil {
		return nil, err
	}
	if err := loadObjectRelated(ctx, c.bh.store, obj, resolveLoads(loads)); err != nil {
		return nil, err
	}
	return obj, nil
}

// loadObjectRelated populates the related-data fields named by set on one object,
// recording each fetched lookup in obj.loaded so the accessors can tell loaded
// from absent. Both client reads and the reconcile decode boundary call it.
// Batched List has its own path (loadListRelated) to avoid an N+1.
func loadObjectRelated[Spec, Status any](ctx context.Context, store Store, obj *Object[Spec, Status], set LoadSet) error {
	if set&LoadOwnerBit != 0 {
		owner, ok, err := fetchOwnerRef(ctx, store, obj.ID)
		if err != nil {
			return err
		}
		if ok {
			obj.Owner = &owner
		}
		obj.loaded |= LoadOwnerBit
	}
	if set&LoadDependenciesBit != 0 {
		deps, err := store.ListOutgoingRefsByRelation(ctx, obj.ID, RelationDependsOn)
		if err != nil {
			return err
		}
		obj.Dependencies = deps
		obj.loaded |= LoadDependenciesBit
	}
	if set&LoadDependentsBit != 0 {
		dependents, err := store.ListIncomingRefs(ctx, obj.ID, RelationDependsOn)
		if err != nil {
			return err
		}
		obj.Dependents = dependents
		obj.loaded |= LoadDependentsBit
	}
	if set&LoadOwnedBit != 0 {
		owned, err := store.ListIncomingRefs(ctx, obj.ID, RelationOwnedBy)
		if err != nil {
			return err
		}
		obj.Owned = owned
		obj.loaded |= LoadOwnedBit
	}
	return nil
}

// fetchOwnerRef resolves id's single owned_by edge. Owner is single (WithOwner
// sets one), so the first row is the owner and ok is false when there is none.
func fetchOwnerRef(ctx context.Context, store Store, id ObjectID) (Ref, bool, error) {
	owners, err := store.ListOutgoingRefsByRelation(ctx, id, RelationOwnedBy)
	if err != nil {
		return Ref{}, false, err
	}
	if len(owners) == 0 {
		return Ref{}, false, nil
	}
	return owners[0], true, nil
}

func (c *clientImpl[Spec, Status]) List(ctx context.Context, loads ...LoadOption) ([]*Object[Spec, Status], error) {
	raws, err := c.bh.store.ListObjects(ctx, c.gk)
	if err != nil {
		return nil, err
	}
	// The migrator is invariant for the kind, so resolve it once rather than
	// re-locking the registry on every row.
	mig := c.bh.migratorFor(c.gk)
	objs := make([]*Object[Spec, Status], 0, len(raws))
	for _, raw := range raws {
		obj, err := rawToTyped[Spec, Status](raw, mig)
		if err != nil {
			// Quarantine, don't abort: one un-decodable row — an un-migratable shape
			// or a blob written by a newer build (downgrade) — is skipped and logged
			// so it can't break listing every other object of the kind.
			c.bh.log().Warn("beehive: skipping undecodable object in List",
				"group", c.gk.Group, "kind", c.gk.Kind, "id", raw.ID, "err", err)
			continue
		}
		objs = append(objs, obj)
	}
	if err := c.loadListRelated(ctx, objs, resolveLoads(loads)); err != nil {
		return nil, err
	}
	return objs, nil
}

// loadListRelated eager-loads the requested secondary lookups for a whole list
// in one batched store call per relation, scattering results back onto each
// object — the N+1-free counterpart to loadObjectRelated. A nil set is a no-op.
func (c *clientImpl[Spec, Status]) loadListRelated(ctx context.Context, objs []*Object[Spec, Status], set LoadSet) error {
	if set == 0 || len(objs) == 0 {
		return nil
	}
	ids := make([]ObjectID, len(objs))
	for i, o := range objs {
		ids[i] = o.ID
	}
	if set&LoadOwnerBit != 0 {
		byID, err := c.bh.store.GroupOutgoingRefsByID(ctx, ids, RelationOwnedBy)
		if err != nil {
			return err
		}
		for _, o := range objs {
			if owners := byID[o.ID]; len(owners) > 0 {
				owner := owners[0]
				o.Owner = &owner
			}
			o.loaded |= LoadOwnerBit
		}
	}
	if set&LoadDependenciesBit != 0 {
		byID, err := c.bh.store.GroupOutgoingRefsByID(ctx, ids, RelationDependsOn)
		if err != nil {
			return err
		}
		for _, o := range objs {
			o.Dependencies = byID[o.ID]
			o.loaded |= LoadDependenciesBit
		}
	}
	if set&LoadDependentsBit != 0 {
		byID, err := c.bh.store.GroupIncomingRefsByID(ctx, ids, RelationDependsOn)
		if err != nil {
			return err
		}
		for _, o := range objs {
			o.Dependents = byID[o.ID]
			o.loaded |= LoadDependentsBit
		}
	}
	if set&LoadOwnedBit != 0 {
		byID, err := c.bh.store.GroupIncomingRefsByID(ctx, ids, RelationOwnedBy)
		if err != nil {
			return err
		}
		for _, o := range objs {
			o.Owned = byID[o.ID]
			o.loaded |= LoadOwnedBit
		}
	}
	return nil
}

func (c *clientImpl[Spec, Status]) GetOwner(ctx context.Context, id ObjectID) (Ref, bool, error) {
	// scopedGet keeps a foreign/missing id invisible (ErrNotFound) before we
	// resolve edges, so this single-kind client never reports another kind's owner.
	if _, err := c.scopedGet(ctx, id); err != nil {
		return Ref{}, false, err
	}
	return fetchOwnerRef(ctx, c.bh.store, id)
}

func (c *clientImpl[Spec, Status]) ListDependencies(ctx context.Context, id ObjectID) ([]Ref, error) {
	if _, err := c.scopedGet(ctx, id); err != nil {
		return nil, err
	}
	return c.bh.store.ListOutgoingRefsByRelation(ctx, id, RelationDependsOn)
}

func (c *clientImpl[Spec, Status]) ListDependents(ctx context.Context, id ObjectID) ([]Ref, error) {
	if _, err := c.scopedGet(ctx, id); err != nil {
		return nil, err
	}
	return c.bh.store.ListIncomingRefs(ctx, id, RelationDependsOn)
}

func (c *clientImpl[Spec, Status]) ListOwned(ctx context.Context, id ObjectID) ([]Ref, error) {
	if _, err := c.scopedGet(ctx, id); err != nil {
		return nil, err
	}
	return c.bh.store.ListIncomingRefs(ctx, id, RelationOwnedBy)
}

func (c *clientImpl[Spec, Status]) Delete(ctx context.Context, id ObjectID) error {
	// RequestDeletion emits the Modified event itself, and only on a real state
	// change — an idempotent retry carries the same resource_version, so emitting
	// would show watchers a spurious diff. It folds this client's kind into the
	// write, so a foreign id can't be deleted through this client; hideWrongKind
	// keeps that foreign id invisible.
	_, _, err := c.bh.store.RequestDeletion(ctx, c.gk, id)
	if err = c.hideWrongKind(err); err != nil {
		return err
	}
	// Always advance GC: a retry or post-crash Delete must still hand the
	// deletion-pending object to the controller to clear finalizers. A
	// client-only kind has no controller, so collect runs synchronously rather
	// than waiting on the resync sweeper (which a disabled resync would never run
	// again after startup).
	c.bh.advanceGC(ctx, c.gk, id)
	return nil
}

func (c *clientImpl[Spec, Status]) WatchList(ctx context.Context) (<-chan WatchEvent[Spec, Status], error) {
	if !c.bh.isRegistered(c.gk) {
		return nil, fmt.Errorf("beehive: no controller registered for %s/%s", c.gk.Group, c.gk.Kind)
	}
	w, err := c.bh.store.WatchList(ctx, c.gk)
	if err != nil {
		return nil, err
	}
	return c.adaptWatcher(ctx, w), nil
}

func (c *clientImpl[Spec, Status]) Watch(ctx context.Context, id ObjectID) (<-chan WatchEvent[Spec, Status], error) {
	if !c.bh.isRegistered(c.gk) {
		return nil, fmt.Errorf("beehive: no controller registered for %s/%s", c.gk.Group, c.gk.Kind)
	}
	w, err := c.bh.store.Watch(ctx, c.gk, id)
	if err != nil {
		return nil, err
	}
	return c.adaptWatcher(ctx, w), nil
}

// adaptWatcher decodes a store Watcher's raw events (the snapshot's Added events
// followed by live changes — the store owns snapshotting, dedup, and id
// filtering) into typed WatchEvents. It forwards on the returned channel until
// ctx is cancelled, the watcher's stream ends, or an event fails to decode; the
// channel closes and the watcher is released on exit.
func (c *clientImpl[Spec, Status]) adaptWatcher(ctx context.Context, w Watcher) <-chan WatchEvent[Spec, Status] {
	out := make(chan WatchEvent[Spec, Status])
	// The migrator is invariant for the watcher's lifetime; resolve it once rather
	// than re-locking the registry on every event.
	mig := c.bh.migratorFor(c.gk)
	go func() {
		defer close(out)
		defer w.Close()
		for {
			select {
			case ev, ok := <-w.Events():
				if !ok {
					return
				}
				obj, err := rawToTyped[Spec, Status](ev.Object, mig)
				if err != nil {
					// Quarantine, don't tear down: skip a poison event and keep the
					// stream alive so one un-decodable object can't silently kill a
					// live watcher (mirrors List).
					c.bh.log().Warn("beehive: skipping undecodable object in watch",
						"group", c.gk.Group, "kind", c.gk.Kind, "id", ev.Object.ID, "err", err)
					continue
				}
				select {
				case out <- WatchEvent[Spec, Status]{Type: ev.Type, Object: obj}:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

// conditionsFromRaw maps the store's raw conditions to the public Condition
// type, dropping the storage-only bookkeeping (last-transition/updated/observed
// generation) that the user-facing type doesn't carry. Returns nil for none.
func conditionsFromRaw(raw []storeapi.Condition) []Condition {
	if len(raw) == 0 {
		return nil
	}
	out := make([]Condition, len(raw))
	for i, c := range raw {
		out[i] = Condition{
			Type:     c.Type,
			Status:   ConditionStatus(c.Status),
			Reason:   c.Reason,
			Message:  c.Message,
			Liveness: c.Liveness,
		}
	}
	return out
}

// convertBlob upgrades a stored JSON blob from its recorded schema version
// (from) to the build's current version, returning the bytes to unmarshal. It is
// the per-blob conversion rule shared by spec and status: a current of 0 (the
// kind isn't versioned, or there's no migrator) or from == current is identity;
// from < current runs convert; from > current is a downgrade — an older build
// reading data a newer one wrote — which we refuse rather than silently truncate,
// surfacing as a quarantine signal upstream.
func convertBlob(from, current int, raw []byte, convert func(int, json.RawMessage) (json.RawMessage, error)) ([]byte, error) {
	switch {
	case current == 0 || from == current:
		return raw, nil
	case from > current:
		return nil, fmt.Errorf("beehive: stored schema version %d is newer than this build's %d", from, current)
	default: // from < current
		return convert(from, raw)
	}
}

// rawToTyped decodes a RawObject into a typed Object[Spec, Status], converting
// each blob up from its stored schema version via m before unmarshalling (see
// convertBlob). A nil m means the kind has no migrator: both current versions are
// 0, so every blob is decoded as-is — byte-identical to the pre-migrator path.
func rawToTyped[Spec, Status any](raw *RawObject, m Migrator) (*Object[Spec, Status], error) {
	// The current-version "0 if nil" rule is shared with the write paths via these
	// helpers; the converters are only reached when from < current (never when m is
	// nil), so guarding them once here suffices.
	var convertSpec, convertStatus func(int, json.RawMessage) (json.RawMessage, error)
	if m != nil {
		convertSpec = m.ConvertSpec
		convertStatus = m.ConvertStatus
	}

	specBytes, err := convertBlob(raw.SpecVersion, migratorSpecVersion(m), raw.Spec, convertSpec)
	if err != nil {
		return nil, err
	}
	var spec Spec
	if err := json.Unmarshal(specBytes, &spec); err != nil {
		return nil, err
	}
	obj := &Object[Spec, Status]{
		ID:                  raw.ID,
		Group:               raw.Group,
		Kind:                raw.Kind,
		Slug:                raw.Slug,
		Spec:                spec,
		Generation:          raw.Generation,
		ObservedGeneration:  raw.ObservedGeneration,
		ObservedAt:          raw.ObservedAt,
		ResourceVersion:     raw.ResourceVersion,
		DeletionRequestedAt: raw.DeletionRequestedAt,
		Finalizers:          raw.Finalizers,
		Conditions:          conditionsFromRaw(raw.Conditions),
		CreatedAt:           raw.CreatedAt,
		UpdatedAt:           raw.UpdatedAt,
	}
	if raw.Status != nil {
		statusBytes, err := convertBlob(raw.StatusVersion, migratorStatusVersion(m), raw.Status, convertStatus)
		if err != nil {
			return nil, err
		}
		var status Status
		if err := json.Unmarshal(statusBytes, &status); err != nil {
			return nil, err
		}
		obj.Status = &status
	}
	return obj, nil
}
