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

// Command conditions is a self-contained Beehive program that shows how a
// controller reports progress through Conditions rather than Status alone.
//
// It defines a "Server" resource whose controller scales replicas online one
// per reconcile pass. While replicas are still coming up it reports a
// "Progressing" condition; a "Ready" liveness condition tracks whether the
// live pool has reached the desired size. The controller requeues itself
// (Result.RequeueAfter) until the pool is full, then clears "Progressing" and
// flips "Ready" to True:
//
//	Create(spec) -> Reconcile (1/3, Progressing) -> ... -> Reconcile (3/3, Ready) -> converged
//
// Run it with `go run ./examples/conditions/main.go`. The watch loop prints the
// object's conditions after each change so you can watch them evolve.
package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/amorey/beehive"
	"github.com/amorey/beehive/sqlite"
)

// ServerGroupKind identifies the resource. Empty Group == core group.
var ServerGroupKind = beehive.GroupKind{Group: "", Kind: "Server"}

// ServerSpec is the desired state the user writes.
type ServerSpec struct {
	Replicas int
}

// ServerStatus is the observed state only the controller writes.
type ServerStatus struct {
	OnlineReplicas int
}

// Condition types and reasons the controller reports.
const (
	condReady       = "Ready"       // True once the live pool has reached the desired size
	condProgressing = "Progressing" // present only while replicas are still coming up
)

// ServerController brings a Server's replicas online over successive reconcile
// passes. The pool is live, in-process state, so "Ready" is reported as a
// Liveness condition: a previous process's claim of readiness is downgraded to
// "verifying" on restart until this controller re-confirms it.
type ServerController struct {
	mu     sync.Mutex
	online map[beehive.ObjectID]int // replicas this process has brought online
}

func (c *ServerController) Reconcile(ctx context.Context, client beehive.ControllerClient[ServerStatus], obj *beehive.Object[ServerSpec, ServerStatus]) (beehive.Result, error) {
	want := obj.Spec.Replicas

	// Bring one more replica online this pass, modeling a pool that warms up
	// incrementally rather than instantly.
	c.mu.Lock()
	have := c.online[obj.ID]
	if have < want {
		have++
		c.online[obj.ID] = have
	}
	c.mu.Unlock()

	ready := have >= want
	msg := fmt.Sprintf("%d/%d replicas online", have, want)

	if ready {
		// Pool is full: clear the transient progress condition and mark Ready.
		if err := client.DeleteCondition(ctx, obj.ID, condProgressing); err != nil {
			return beehive.Result{}, err
		}
		if err := client.SetCondition(ctx, obj.ID, beehive.Condition{
			Type:     condReady,
			Status:   beehive.ConditionTrue,
			Reason:   "AllReplicasOnline",
			Message:  msg,
			Liveness: true,
		}); err != nil {
			return beehive.Result{}, err
		}
	} else {
		if err := client.SetCondition(ctx, obj.ID, beehive.Condition{
			Type:    condProgressing,
			Status:  beehive.ConditionTrue,
			Reason:  "ScalingUp",
			Message: msg,
		}); err != nil {
			return beehive.Result{}, err
		}
		if err := client.SetCondition(ctx, obj.ID, beehive.Condition{
			Type:     condReady,
			Status:   beehive.ConditionFalse,
			Reason:   "ScalingUp",
			Message:  msg,
			Liveness: true,
		}); err != nil {
			return beehive.Result{}, err
		}
	}

	if err := client.UpdateStatus(ctx, obj.ID, obj.Generation, ServerStatus{OnlineReplicas: have}); err != nil {
		return beehive.Result{}, err
	}

	// Requeue ourselves until the pool is full; once ready, settle.
	if !ready {
		return beehive.Result{RequeueAfter: 200 * time.Millisecond}, nil
	}
	return beehive.Result{}, nil
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

	_, err = beehive.Register(bh, ServerGroupKind, &ServerController{online: map[beehive.ObjectID]int{}})
	exitOnErr(err)

	stop, err := bh.Start(context.Background())
	exitOnErr(err)
	defer stopBeehive(stop)

	ctx := context.Background()
	client := beehive.NewClient[ServerSpec, ServerStatus](bh, ServerGroupKind)

	// Subscribe before creating so we don't miss the controller's first writes.
	watchCh, err := client.WatchList(ctx)
	exitOnErr(err)

	obj, err := client.Create(ctx, ServerSpec{Replicas: 3})
	exitOnErr(err)

	fmt.Printf("created Server id=%d replicas=%d\n", obj.ID, obj.Spec.Replicas)

	waitForReady(obj.ID, watchCh)
}

func stopBeehive(stop func(context.Context) error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := stop(ctx); err != nil {
		fmt.Printf("beehive: shutdown did not drain cleanly: %v\n", err)
	}
}

// waitForReady prints each change to object id and returns once its Ready
// condition reports True.
func waitForReady(id int64, watchCh <-chan beehive.Change[ServerSpec, ServerStatus]) {
	for evt := range watchCh {
		if evt.Object.ID != id {
			continue
		}
		printConditions(evt.Object)
		if isReady(evt.Object) {
			fmt.Println("converged: server is Ready")
			return
		}
	}
	log.Fatal("watch channel closed before the server became Ready")
}

// isReady reports whether the object has a Ready condition set to True.
func isReady(obj *beehive.Object[ServerSpec, ServerStatus]) bool {
	for _, c := range obj.Conditions {
		if c.Type == condReady {
			return c.Status == beehive.ConditionTrue
		}
	}
	return false
}

// printConditions dumps an object's conditions in a stable, readable form.
func printConditions(obj *beehive.Object[ServerSpec, ServerStatus]) {
	if len(obj.Conditions) == 0 {
		fmt.Printf("rv=%d conditions: (none)\n", obj.ResourceVersion)
		return
	}
	fmt.Printf("rv=%d conditions:\n", obj.ResourceVersion)
	for _, c := range obj.Conditions {
		fmt.Printf("  %-12s %-7s reason=%s msg=%q\n", c.Type, c.Status, c.Reason, c.Message)
	}
}
