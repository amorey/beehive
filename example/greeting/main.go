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
// Run it with `go run ./example/greeting/main.go`.
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
type GreetingController struct {
	client beehive.ControllerClient[GreetingStatus]
}

func (gc *GreetingController) Start(client beehive.ControllerClient[GreetingStatus]) error {
	gc.client = client
	return nil
}

func (gc *GreetingController) Stop(_ context.Context) error {
	return nil
}

func (gc *GreetingController) Reconcile(ctx context.Context, obj *beehive.Object[GreetingSpec, GreetingStatus]) (beehive.Result, error) {
	want := "Hello, " + obj.Spec.Name
	if obj.Status != nil && obj.Status.Message == want {
		return beehive.Result{}, nil
	}
	err := gc.client.UpdateStatus(ctx, obj.ID, obj.Generation, GreetingStatus{Message: want})
	return beehive.Result{}, err
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

	bh, err := beehive.New(store)
	exitOnErr(err)

	err = beehive.Register(bh, GreetingGroupKind, &GreetingController{})
	exitOnErr(err)

	err = bh.Start()
	exitOnErr(err)
	defer stopBeehive(bh)

	ctx := context.Background()
	client := beehive.NewClient[GreetingSpec, GreetingStatus](bh, GreetingGroupKind)

	// Subscribe before creating so we don't miss the controller's UpdateStatus event.
	watchCh, err := client.WatchList(ctx)
	exitOnErr(err)

	obj, err := client.Create(ctx, GreetingSpec{Name: "world"})
	exitOnErr(err)

	fmt.Printf("created Greeting id=%d name=%v\n", obj.ID, obj.Spec.Name)

	waitForConvergence(obj.ID, watchCh)
}

func stopBeehive(bh *beehive.Beehive) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	bh.Stop(ctx)
}

// waitForConvergence drains watchCh until it sees a status-bearing event for id.
func waitForConvergence(id int64, watchCh <-chan beehive.WatchEvent[GreetingSpec, GreetingStatus]) {
	for evt := range watchCh {
		if evt.Object.ID != id || evt.Object.Status == nil {
			continue
		}
		fmt.Printf("converged: %s\n", evt.Object.Status.Message)
		return
	}
	log.Fatal("watch channel closed before convergence")
}
