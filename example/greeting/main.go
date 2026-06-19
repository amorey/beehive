//go:build ignore

// Command greeting is a dead-simple, self-contained Beehive program used to
// drive the implementation top-down. It defines a "Greeting" resource whose
// controller reconciles a desired name into an observed greeting message —
// no external I/O, no finalizers, just the core declarative loop:
//
//	Create(spec) -> controller Reconcile -> UpdateStatus -> converged
//
// Run it with `go run ./example/greeting/main.go`. Until the lower layers are implemented the
// stubbed calls panic; each layer we fill in moves this program one step
// closer to printing "Hello, world".
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
