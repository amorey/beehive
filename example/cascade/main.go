//go:build ignore

// Command cascade is a self-contained Beehive program that demonstrates
// finalizers and owner-driven delete cascades.
//
// It models two kinds. A "Cluster" owns a live connection, guarded by a
// connection finalizer so the connection is torn down cleanly before the row is
// removed. A "ClusterCache" is created WithOwner(cluster) and holds its own
// cache-flush finalizer. Deleting the Cluster cascades:
//
//	Delete(cluster)
//	  -> GC requests deletion of every owned ClusterCache (cascade)
//	  -> each cache flushes, clears its finalizer, and is removed
//	  -> only once no cache references it does the Cluster close its
//	     connection (gated on HasIncomingRefs), clear its finalizer, and get removed
//
// The Cluster's connection therefore outlives its caches: the owner is the last
// thing collected. Run it with `go run ./example/cascade/main.go`.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/amorey/beehive"
	"github.com/amorey/beehive/sqlite"
)

// Kinds. Empty Group == core group.
var (
	ClusterGroupKind      = beehive.GroupKind{Group: "", Kind: "Cluster"}
	ClusterCacheGroupKind = beehive.GroupKind{Group: "", Kind: "ClusterCache"}
)

// Finalizers gate physical deletion until each controller finishes its teardown.
const (
	connectionFinalizer = "example.beehive/connection"  // Cluster: close the connection
	cacheFlushFinalizer = "example.beehive/cache-flush" // ClusterCache: flush local state
)

const numCaches = 2

type ClusterSpec struct{ Endpoint string }
type ClusterStatus struct{ Connected bool }

type ClusterCacheSpec struct{ ClusterID beehive.ObjectID }
type ClusterCacheStatus struct{ Entries int }

// ClusterController opens a connection on create and, on deletion, keeps it open
// until no cache still references the Cluster — so the connection outlives the
// caches that use it — then closes it and clears the finalizer.
type ClusterController struct {
	client beehive.ControllerClient[ClusterStatus]
}

func (c *ClusterController) Start(client beehive.ControllerClient[ClusterStatus]) error {
	c.client = client
	return nil
}
func (c *ClusterController) Stop(_ context.Context) error { return nil }

func (c *ClusterController) Reconcile(ctx context.Context, obj *beehive.Object[ClusterSpec, ClusterStatus]) (beehive.Result, error) {
	if obj.DeletionRequestedAt != nil {
		// Hold the connection open while any cache still has a live claim on us.
		// HasIncomingRefs ignores caches that are themselves finalizing, so this clears
		// once the owned caches are gone — not merely marked for deletion.
		referenced, err := c.client.HasIncomingRefs(ctx, obj.ID)
		if err != nil {
			return beehive.Result{}, err
		}
		if referenced {
			fmt.Printf("Cluster %d: caches still attached; holding connection open\n", obj.ID)
			return beehive.Result{}, nil
		}
		fmt.Printf("Cluster %d: closed connection; releasing finalizer\n", obj.ID)
		return beehive.Result{}, c.client.DeleteFinalizer(ctx, obj.ID, connectionFinalizer)
	}

	if obj.Status == nil || !obj.Status.Connected {
		return beehive.Result{}, c.client.UpdateStatus(ctx, obj.ID, obj.Generation, ClusterStatus{Connected: true})
	}
	return beehive.Result{}, nil
}

// ClusterCacheController warms a cache on create and, on deletion, flushes it and
// clears its finalizer so GC can remove the row.
type ClusterCacheController struct {
	client beehive.ControllerClient[ClusterCacheStatus]
}

func (c *ClusterCacheController) Start(client beehive.ControllerClient[ClusterCacheStatus]) error {
	c.client = client
	return nil
}
func (c *ClusterCacheController) Stop(_ context.Context) error { return nil }

func (c *ClusterCacheController) Reconcile(ctx context.Context, obj *beehive.Object[ClusterCacheSpec, ClusterCacheStatus]) (beehive.Result, error) {
	if obj.DeletionRequestedAt != nil {
		fmt.Printf("ClusterCache %d: flushed local cache; releasing finalizer\n", obj.ID)
		return beehive.Result{}, c.client.DeleteFinalizer(ctx, obj.ID, cacheFlushFinalizer)
	}

	if obj.Status == nil {
		return beehive.Result{}, c.client.UpdateStatus(ctx, obj.ID, obj.Generation, ClusterCacheStatus{Entries: 42})
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

	exitOnErr(beehive.Register(bh, ClusterGroupKind, &ClusterController{}))
	exitOnErr(beehive.Register(bh, ClusterCacheGroupKind, &ClusterCacheController{}))

	exitOnErr(bh.Start())
	defer stopBeehive(bh)

	ctx := context.Background()
	clusterClient := beehive.NewClient[ClusterSpec, ClusterStatus](bh, ClusterGroupKind)
	cacheClient := beehive.NewClient[ClusterCacheSpec, ClusterCacheStatus](bh, ClusterCacheGroupKind)

	// Subscribe before creating so no lifecycle event is missed.
	clusterCh, err := clusterClient.WatchList(ctx)
	exitOnErr(err)
	cacheCh, err := cacheClient.WatchList(ctx)
	exitOnErr(err)

	// A Cluster guarded by a connection finalizer, owning two caches that each
	// guard a cache-flush finalizer.
	cluster, err := clusterClient.Create(ctx, ClusterSpec{Endpoint: "db.example:5432"},
		beehive.WithFinalizers(connectionFinalizer))
	exitOnErr(err)
	fmt.Printf("created Cluster %d (endpoint=%s, finalizers=%v)\n", cluster.ID, cluster.Spec.Endpoint, cluster.Finalizers)

	for i := 0; i < numCaches; i++ {
		cache, err := cacheClient.Create(ctx, ClusterCacheSpec{ClusterID: cluster.ID},
			beehive.WithOwner(cluster.ID), beehive.WithFinalizers(cacheFlushFinalizer))
		exitOnErr(err)
		fmt.Printf("created ClusterCache %d owned by Cluster %d (finalizers=%v)\n", cache.ID, cluster.ID, cache.Finalizers)
	}

	watchCascade(ctx, clusterClient, clusterCh, cacheCh, cluster.ID)
}

func stopBeehive(bh *beehive.Beehive) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	bh.Stop(ctx)
}

// watchCascade drives the demo from a single event loop: it waits for the
// Cluster and both caches to converge, deletes the Cluster, then prints the
// cascade until every row is removed.
func watchCascade(
	ctx context.Context,
	clusterClient beehive.Client[ClusterSpec, ClusterStatus],
	clusterCh <-chan beehive.WatchEvent[ClusterSpec, ClusterStatus],
	cacheCh <-chan beehive.WatchEvent[ClusterCacheSpec, ClusterCacheStatus],
	clusterID beehive.ObjectID,
) {
	warmed := map[beehive.ObjectID]bool{}
	connected := false
	deleted := false
	clusterRemoved := false
	cachesRemoved := 0

	deleteWhenReady := func() {
		if deleted || !connected || len(warmed) < numCaches {
			return
		}
		deleted = true
		fmt.Printf("\nall ready; deleting Cluster %d — watch the cascade:\n", clusterID)
		exitOnErr(clusterClient.Delete(ctx, clusterID))
	}

	timeout := time.After(10 * time.Second)
	for !clusterRemoved || cachesRemoved < numCaches {
		select {
		case ev := <-clusterCh:
			o := ev.Object
			if ev.Type == beehive.WatchEventDeleted {
				fmt.Printf("Cluster %d: removed\n", o.ID)
				clusterRemoved = true
				continue
			}
			if !deleted && o.Status != nil && o.Status.Connected && !connected {
				connected = true
				fmt.Printf("Cluster %d: connected to %s\n", o.ID, o.Spec.Endpoint)
				deleteWhenReady()
			}
		case ev := <-cacheCh:
			o := ev.Object
			if ev.Type == beehive.WatchEventDeleted {
				fmt.Printf("ClusterCache %d: removed\n", o.ID)
				cachesRemoved++
				continue
			}
			if !deleted && o.Status != nil && o.Status.Entries > 0 && !warmed[o.ID] {
				warmed[o.ID] = true
				fmt.Printf("ClusterCache %d: warmed (%d entries)\n", o.ID, o.Status.Entries)
				deleteWhenReady()
			}
		case <-timeout:
			log.Fatal("timed out waiting for the cascade to finish")
		}
	}

	fmt.Println("\ncascade complete: caches drained first, the cluster's connection last")
}
