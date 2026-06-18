// Command example is a dead-simple, self-contained Beehive program used to
// drive the implementation top-down. It defines a "Greeting" resource whose
// controller reconciles a desired name into an observed greeting message —
// no external I/O, no finalizers, just the core declarative loop:
//
//	Create(spec) -> controller Reconcile -> UpdateStatus -> converged
//
// Run it with `go run ./example`. Until the lower layers are implemented the
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
	// Already converged: observed message matches desired name. Nothing to do.
	if obj.Status != nil && obj.Status.Message == "Hello, "+obj.Spec.Name {
		return beehive.Result{}, nil
	}
	return beehive.Result{}, gc.client.UpdateStatus(ctx, obj.ID, obj.Generation, GreetingStatus{
		Message: "Hello, " + obj.Spec.Name,
	})
}

func main() {
	store, err := sqlite.OpenMemory()
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer store.Close()

	bh, err := beehive.New(store)
	if err != nil {
		log.Fatalf("new beehive: %v", err)
	}
	if err := beehive.Register(bh, GreetingGroupKind, &GreetingController{}); err != nil {
		log.Fatalf("register: %v", err)
	}
	if err := bh.Start(); err != nil {
		log.Fatalf("start: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		bh.Stop(ctx)
	}()

	ctx := context.Background()
	client := beehive.NewClient[GreetingSpec, GreetingStatus](bh, GreetingGroupKind)

	obj, err := client.Create(ctx, GreetingSpec{Name: "world"})
	if err != nil {
		log.Fatalf("create: %v", err)
	}
	fmt.Printf("created Greeting id=%d name=%v\n", obj.ID, obj.Spec.Name)

	// Wait for the controller to converge. Level-triggered: we just poll the
	// observed state until the status appears.
	for {
		got, err := client.Get(ctx, obj.ID)
		if err != nil {
			log.Fatalf("get: %v", err)
		}
		if got.Status != nil {
			fmt.Printf("converged: %s\n", got.Status.Message)
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}
