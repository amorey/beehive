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

// Command greeting is a dead-simple, self-contained Beehive program. It defines
// a "Greeting" resource whose controller reconciles a desired name into an
// observed greeting message — no external I/O, no finalizers, just the core
// declarative loop:
//
//	Create(spec) -> controller Reconcile -> UpdateStatus -> converged
//
// Run it with `go run ./examples/greeting/main.go`.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/amorey/beehive"
	"github.com/amorey/beehive/sqlite"
)

// GreetingGroupKind identifies the resource. Empty Group == core group.
var GreetingGroupKind = beehive.GroupKind{Group: "", Kind: "Greeting"}

// GreetingSpec is the desired state the user writes.
type GreetingSpec struct {
	Name string
}

// GreetingStatus is the observed state only the controller writes.
type GreetingStatus struct {
	Message string
}

// GreetingController reconciles a GreetingSpec into a GreetingStatus.
type GreetingController struct{}

func (gc *GreetingController) Reconcile(ctx context.Context, client beehive.ControllerClient[GreetingStatus], obj *beehive.Object[GreetingSpec, GreetingStatus]) beehive.ReconcileResult {
	want := "Hello, " + obj.Spec.Name
	if obj.Status != nil && obj.Status.Message == want {
		return beehive.Settled()
	}
	if err := client.UpdateStatus(ctx, GreetingStatus{Message: want}); err != nil {
		return beehive.Fail(err)
	}
	return beehive.Settled()
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

	// Production defaults throughout. A write schedules nothing, so the create below
	// is found by the owed-pass tick — at 30s, which would be a 30s demo. Rather than
	// speed the drivers up, the demo nudges the one object it made with Requeue; see
	// the Create call.
	bh, err := beehive.New(store)
	exitOnErr(err)

	err = beehive.Register(bh, GreetingGroupKind, &GreetingController{})
	exitOnErr(err)

	stop, err := bh.Start(context.Background())
	exitOnErr(err)
	defer stopBeehive(stop)

	ctx := context.Background()
	client := beehive.NewClient[GreetingSpec, GreetingStatus](bh, GreetingGroupKind)

	// Watch before creating, so the create itself arrives as an Added and the
	// controller's UpdateStatus as the Modified after it. (A watch started later
	// would still converge — its first poll reports current state — but it could
	// report the settled object in one event and never show the intermediate.)
	stream, err := client.WatchList(ctx)
	exitOnErr(err)

	obj, err := client.Create(ctx, "world", GreetingSpec{Name: "world"})
	exitOnErr(err)

	fmt.Printf("created Greeting id=%d name=%v\n", obj.ID, obj.Spec.Name)

	// Nothing scheduled that create, so without this the demo waits out the owed-pass
	// tick. Requeue is a latency hint, not a correctness requirement: drop it and the
	// same convergence happens, 30s later.
	exitOnErr(client.Requeue(ctx, obj.ID))

	waitForConvergence(obj.ID, stream)
}

func stopBeehive(stop func(context.Context) error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := stop(ctx); err != nil {
		fmt.Printf("beehive: shutdown did not drain cleanly: %v\n", err)
	}
}

// waitForConvergence drains the stream until it sees a status-bearing event for id.
func waitForConvergence(id int64, stream *beehive.ObjectListStream[GreetingSpec, GreetingStatus]) {
	for evt := range stream.Changes {
		// Object is nil on a Deleted whose row image no longer decodes; evt.ID
		// identifies the object either way.
		if evt.ID != id || evt.Object == nil || evt.Object.Status == nil {
			continue
		}
		fmt.Printf("converged: %s\n", evt.Object.Status.Message)
		return
	}
	if err := stream.Err(); err != nil {
		log.Fatalf("watch ended: %v", err)
	}
	log.Fatal("watch channel closed before convergence")
}
