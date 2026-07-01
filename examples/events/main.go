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

// Command events demonstrates the Events API with a cluster connection-health
// panel. A "Cluster" resource has a health prober (an ordinary app goroutine, not
// beehive machinery) that records one event per connection probe; consecutive
// identical outcomes coalesce into runs, so a flapping cluster produces the
// aggregated, newest-first timeline a panel renders:
//
//	Create(spec) -> prober RecordEvent×N -> Client.ListEvents -> render
//
// Run it with `go run ./examples/events/main.go`.
package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/amorey/beehive"
	"github.com/amorey/beehive/sqlite"
)

// ClusterGroupKind identifies the resource. Empty Group == core group.
var ClusterGroupKind = beehive.GroupKind{Group: "kstack.sh", Kind: "Cluster"}

// ClusterSpec is the desired state the user writes.
type ClusterSpec struct {
	Endpoint string
}

// ClusterStatus is the observed state only the controller writes.
type ClusterStatus struct {
	Reachable bool
}

// ProbeDetail is the structured payload the prober attaches to a failure event.
type ProbeDetail struct {
	Endpoint  string `json:"endpoint"`
	LatencyMs int    `json:"latencyMs"`
}

// ClusterController reconciles a Cluster. The connection health itself is reported
// as events by the prober below, so reconcile has nothing to do here beyond
// acknowledging deletion.
type ClusterController struct{}

func (cc *ClusterController) Reconcile(ctx context.Context, client beehive.ControllerClient[ClusterStatus], obj *beehive.Object[ClusterSpec, ClusterStatus]) (beehive.Result, error) {
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

	// Register returns the kind's ControllerClient — the prober (app-owned
	// background work) uses it to record events out of band.
	prober, err := beehive.Register(bh, ClusterGroupKind, &ClusterController{})
	exitOnErr(err)

	stop, err := bh.Start(context.Background())
	exitOnErr(err)
	defer stopBeehive(stop)

	ctx := context.Background()
	client := beehive.NewClient[ClusterSpec, ClusterStatus](bh, ClusterGroupKind)

	cluster, err := client.Create(ctx, ClusterSpec{Endpoint: "10.0.0.1:443"})
	exitOnErr(err)
	fmt.Printf("created Cluster id=%d endpoint=%s\n\n", cluster.ID, cluster.Spec.Endpoint)

	// Simulate a flapping connection: one RecordEvent per probe outcome.
	probe := func(typ beehive.EventType, reason, message string, detail any, n int) {
		for range n {
			exitOnErr(prober.RecordEvent(ctx, cluster.ID, beehive.EventSpec{
				Category: "connection", Type: typ, Reason: reason, Message: message, Detail: detail,
			}))
		}
	}
	probe(beehive.EventNormal, "Connected", "", nil, 16)
	probe(beehive.EventWarning, "TLSHandshake", "x509: certificate expired", nil, 5)
	probe(beehive.EventNormal, "Connected", "", nil, 7)
	probe(beehive.EventWarning, "ProbeFailed", "i/o timeout", ProbeDetail{Endpoint: "10.0.0.1:443", LatencyMs: 5000}, 18)
	probe(beehive.EventNormal, "Connected", "", nil, 4)

	panel, err := client.ListEvents(ctx, cluster.ID, beehive.WithEventCategory("connection"))
	exitOnErr(err)

	fmt.Println("connection-health panel (newest first):")
	renderPanel(panel)
}

// renderPanel prints each run as one line: last-seen time, ✓/✗, reason, count,
// [first–last] window, and the sampled message.
func renderPanel(events []beehive.Event) {
	for _, e := range events {
		mark := "✓"
		if e.Type == beehive.EventWarning {
			mark = "✗"
		}
		line := fmt.Sprintf("  %s  %s %-14s ×%-3d %s–%s",
			e.LastAt.Format("15:04:05"), mark, e.Reason, e.Count,
			e.FirstAt.Format("15:04:05"), e.LastAt.Format("15:04:05"))
		if e.Message != "" {
			line += "   " + strconv.Quote(e.Message)
		}
		fmt.Println(line)
	}
}

func stopBeehive(stop func(context.Context) error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := stop(ctx); err != nil {
		fmt.Printf("beehive: shutdown did not drain cleanly: %v\n", err)
	}
}
