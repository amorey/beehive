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

//go:build ignore

// Command lowpower is the configuration a long-running desktop application
// wants: every driver at a multi-minute cadence, so an idle process issues a
// query a minute or two rather than one a second, and every visible latency
// carried by the push that a commit makes.
//
// It is the same loop as examples/greeting with a delete on the end, and it is
// the one example that does *not* call Requeue after its create — the point
// here is that nothing but the commit pushes drove it. Every tick set below is
// minutes away, so a demo that finishes in milliseconds finished on pushes:
//
//	Create(spec)   -> the spec write enqueues its own object -> UpdateStatus
//	Delete(id)     -> the delete request enqueues its own object -> collected
//
// The cadences are backstops for a push lost to a crash, and for the kinds no
// push covers — a client-only kind still waits out WithGCInterval per level of
// a cascade. See docs/adr/2026-08-06-driver-cadences-are-configurable.md.
//
// Run it with `go run ./examples/lowpower/main.go`.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/amorey/beehive"
	"github.com/amorey/beehive/sqlite"
)

// PanelGroupKind identifies the resource. Empty Group == core group.
var PanelGroupKind = beehive.GroupKind{Group: "", Kind: "Panel"}

type PanelSpec struct{ Source string }
type PanelStatus struct{ Connected bool }

// PanelController connects a panel to its source, once.
type PanelController struct{}

func (c *PanelController) Reconcile(ctx context.Context, client beehive.ControllerClient[PanelStatus], obj *beehive.Object[PanelSpec, PanelStatus]) beehive.ReconcileResult {
	if obj.DeletionRequestedAt != nil || (obj.Status != nil && obj.Status.Connected) {
		return beehive.Settled(0)
	}
	if err := client.UpdateStatus(ctx, obj.ID, PanelStatus{Connected: true}); err != nil {
		return beehive.Fail(err)
	}
	return beehive.Settled(0)
}

func exitOnErr(err error) {
	if err != nil {
		log.Fatalf("%v", err)
	}
}

func main() {
	store, err := sqlite.OpenMemory()
	exitOnErr(err)
	defer store.Close()

	// The idle cost of this process is one query per driver per interval. Each
	// one below is a backstop, and the comment is what lengthening it buys time
	// against — never the latency of a local write.
	bh, err := beehive.New(store,
		// A watch reads on a commit wake; the floor covers a retention trim and
		// a step that failed past its retry ladder.
		beehive.WithWatchFloorInterval(10*time.Minute),
		// Recovery for a push lost between the commit and the dispatch.
		beehive.WithOwedPassInterval(5*time.Minute),
		// Collect for the kinds no push reaches — a client-only kind costs one
		// interval per level of a cascade.
		beehive.WithGCInterval(5*time.Minute),
		// The backstop under the dependency waker, which has no tick at all.
		beehive.WithStaleDependentsInterval(5*time.Minute),
		// A failing reconcile settles here instead of at 30s.
		beehive.WithMaxRetryInterval(5*time.Minute),
		// WithFullPassInterval stays off: it scales with the object count, and
		// nothing here depends on it.
	)
	exitOnErr(err)

	err = beehive.Register(bh, PanelGroupKind, &PanelController{})
	exitOnErr(err)

	stop, err := bh.Start(context.Background())
	exitOnErr(err)
	defer stopBeehive(stop)

	ctx := context.Background()
	client := beehive.NewClient[PanelSpec, PanelStatus](bh, PanelGroupKind)

	stream, err := client.WatchList(ctx)
	exitOnErr(err)

	started := time.Now()
	panel, err := client.Create(ctx, "connections", PanelSpec{Source: "localhost:5432"})
	exitOnErr(err)
	fmt.Printf("created Panel %d (source=%s)\n", panel.ID, panel.Spec.Source)

	// No Requeue, unlike every other example here: the spec write already
	// enqueued this object, and waiting on the tick instead would take 5m.
	waitFor(stream, panel.ID, func(o *beehive.Object[PanelSpec, PanelStatus]) bool {
		return o != nil && o.Status != nil && o.Status.Connected
	})
	fmt.Printf("connected after %s (the owed pass is %s away)\n", time.Since(started).Round(time.Millisecond), 5*time.Minute)

	started = time.Now()
	exitOnErr(client.Delete(ctx, panel.ID))
	waitFor(stream, panel.ID, func(o *beehive.Object[PanelSpec, PanelStatus]) bool { return o == nil })
	fmt.Printf("collected after %s (the GC sweep is %s away)\n", time.Since(started).Round(time.Millisecond), 5*time.Minute)
}

func stopBeehive(stop func(context.Context) error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := stop(ctx); err != nil {
		fmt.Printf("beehive: shutdown did not drain cleanly: %v\n", err)
	}
}

// waitFor drains the stream until id satisfies want. A Deleted carries a row
// image rather than a live object, so want sees nil for the collected panel.
func waitFor(stream *beehive.ObjectListStream[PanelSpec, PanelStatus], id beehive.ObjectID, want func(*beehive.Object[PanelSpec, PanelStatus]) bool) {
	timeout := time.After(30 * time.Second)
	for {
		select {
		case evt, open := <-stream.Changes:
			if !open {
				log.Fatalf("watch ended early: %v", stream.Err())
			}
			if evt.ID != id {
				continue
			}
			if evt.Type == beehive.Deleted {
				if want(nil) {
					return
				}
				continue
			}
			if want(evt.Object) {
				return
			}
		case <-timeout:
			log.Fatal("timed out: a push was lost, and the ticks here are minutes away")
		}
	}
}
