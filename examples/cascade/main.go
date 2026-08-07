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
//	     connection (gated on HasIncomingEdges), clear its finalizer, and get removed
//
// The Cluster's connection therefore outlives its caches: the owner is the last
// thing collected. Run it with `go run ./examples/cascade/main.go`.
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
type ClusterController struct{}

func (c *ClusterController) Reconcile(ctx context.Context, client beehive.ControllerClient[ClusterStatus], obj *beehive.Object[ClusterSpec, ClusterStatus]) (beehive.Result, error) {
	if obj.DeletionRequestedAt != nil {
		// Hold the connection open while any cache still has a live claim on us.
		// HasIncomingEdges ignores caches that are themselves finalizing, so this clears
		// once the owned caches are gone — not merely marked for deletion.
		referenced, err := client.HasIncomingEdges(ctx, obj.ID)
		if err != nil {
			return beehive.Result{}, err
		}
		if referenced {
			fmt.Printf("Cluster %d: caches still attached; holding connection open\n", obj.ID)
			return beehive.Result{}, nil
		}
		fmt.Printf("Cluster %d: closed connection; releasing finalizer\n", obj.ID)
		return beehive.Result{}, client.DeleteFinalizer(ctx, obj.ID, connectionFinalizer)
	}

	if obj.Status == nil || !obj.Status.Connected {
		return beehive.Result{}, client.UpdateStatus(ctx, obj.ID, obj.Generation, ClusterStatus{Connected: true})
	}
	return beehive.Result{}, nil
}

// ClusterCacheController warms a cache on create and, on deletion, flushes it and
// clears its finalizer so GC can remove the row.
type ClusterCacheController struct{}

func (c *ClusterCacheController) Reconcile(ctx context.Context, client beehive.ControllerClient[ClusterCacheStatus], obj *beehive.Object[ClusterCacheSpec, ClusterCacheStatus]) (beehive.Result, error) {
	if obj.DeletionRequestedAt != nil {
		fmt.Printf("ClusterCache %d: flushed local cache; releasing finalizer\n", obj.ID)
		return beehive.Result{}, client.DeleteFinalizer(ctx, obj.ID, cacheFlushFinalizer)
	}

	if obj.Status == nil {
		return beehive.Result{}, client.UpdateStatus(ctx, obj.ID, obj.Generation, ClusterCacheStatus{Entries: 42})
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

	// Every driver here is periodic — nothing is pushed — and a cascade advances one
	// step per GC tick: mark the children, wait for their finalizers to clear,
	// collect them, then collect the owner they were blocking. At the production
	// defaults that is tens of seconds, so this demo turns the cadences down to a
	// human timescale. The watch poll that paces the printout below is fixed at 1s,
	// so GC is set just above it — a sweep faster than one poll would still cascade
	// correctly, but its steps would coalesce into a single delivery.
	// The reconciles that get each object to converged are nudged with Requeue below,
	// so GC is the only cadence this demo has to set.
	bh, err := beehive.New(store, beehive.WithGCInterval(1500*time.Millisecond))
	exitOnErr(err)

	_, err = beehive.Register(bh, ClusterGroupKind, &ClusterController{})
	exitOnErr(err)
	_, err = beehive.Register(bh, ClusterCacheGroupKind, &ClusterCacheController{})
	exitOnErr(err)

	stop, err := bh.Start(context.Background())
	exitOnErr(err)
	defer stopBeehive(stop)

	ctx := context.Background()
	clusterClient := beehive.NewClient[ClusterSpec, ClusterStatus](bh, ClusterGroupKind)
	cacheClient := beehive.NewClient[ClusterCacheSpec, ClusterCacheStatus](bh, ClusterCacheGroupKind)

	// Watch before creating, so each object's lifecycle reads in order from Added.
	// A poll coalesces, so a step shorter than the interval can still be skipped.
	_, clusterCh, err := clusterClient.WatchList(ctx)
	exitOnErr(err)
	_, cacheCh, err := cacheClient.WatchList(ctx)
	exitOnErr(err)

	// A Cluster guarded by a connection finalizer, owning two caches that each
	// guard a cache-flush finalizer.
	cluster, err := clusterClient.Create(ctx, "primary", ClusterSpec{Endpoint: "db.example:5432"},
		beehive.WithFinalizers(connectionFinalizer))
	exitOnErr(err)
	fmt.Printf("created Cluster %d (endpoint=%s, finalizers=%v)\n", cluster.ID, cluster.Spec.Endpoint, cluster.Finalizers)
	exitOnErr(clusterClient.Requeue(ctx, cluster.ID))

	for i := range numCaches {
		cache, err := cacheClient.Create(ctx, fmt.Sprintf("cache-%d", i), ClusterCacheSpec{ClusterID: cluster.ID},
			beehive.WithOwner(cluster.ID), beehive.WithFinalizers(cacheFlushFinalizer))
		exitOnErr(err)
		fmt.Printf("created ClusterCache %d owned by Cluster %d (finalizers=%v)\n", cache.ID, cluster.ID, cache.Finalizers)
		// A write schedules nothing, so nudge each object rather than waiting out the
		// owed-pass tick.
		exitOnErr(cacheClient.Requeue(ctx, cache.ID))
	}

	watchCascade(ctx, clusterClient, clusterCh, cacheCh, cluster.ID)
}

func stopBeehive(stop func(context.Context) error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := stop(ctx); err != nil {
		fmt.Printf("beehive: shutdown did not drain cleanly: %v\n", err)
	}
}

// watchCascade drives the demo from a single event loop: it waits for the
// Cluster and both caches to converge, deletes the Cluster, then prints the
// cascade until every row is removed.
func watchCascade(
	ctx context.Context,
	clusterClient beehive.Client[ClusterSpec, ClusterStatus],
	clusterCh <-chan beehive.ObjectChange[ClusterSpec, ClusterStatus],
	cacheCh <-chan beehive.ObjectChange[ClusterCacheSpec, ClusterCacheStatus],
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

	timeout := time.After(30 * time.Second)
	for !clusterRemoved || cachesRemoved < numCaches {
		select {
		case ev := <-clusterCh:
			// Object is nil on Failed, and on a Deleted whose row image no
			// longer decodes; ev.ID identifies the object either way.
			if ev.Type == beehive.Failed {
				log.Fatalf("cluster watch ended: %v", ev.Err)
			}
			if ev.Type == beehive.Deleted {
				fmt.Printf("Cluster %d: removed\n", ev.ID)
				clusterRemoved = true
				continue
			}
			o := ev.Object
			if !deleted && o.Status != nil && o.Status.Connected && !connected {
				connected = true
				fmt.Printf("Cluster %d: connected to %s\n", o.ID, o.Spec.Endpoint)
				deleteWhenReady()
			}
		case ev := <-cacheCh:
			if ev.Type == beehive.Failed {
				log.Fatalf("cache watch ended: %v", ev.Err)
			}
			if ev.Type == beehive.Deleted {
				fmt.Printf("ClusterCache %d: removed\n", ev.ID)
				cachesRemoved++
				continue
			}
			o := ev.Object
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
