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
// panel. A "Cluster" resource is probed by its own controller: each reconcile
// pass records one event per connection probe, and consecutive identical
// outcomes coalesce into runs, so a flapping cluster produces the aggregated,
// newest-first timeline a panel renders:
//
//	Create(spec) -> reconcile AddEvent×N -> Client.WatchEvents -> render, then resume
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

// ProbeDetail is the structured payload a failure event carries.
type ProbeDetail struct {
	Endpoint  string `json:"endpoint"`
	LatencyMs int    `json:"latencyMs"`
}

// probe is one scripted outcome, repeated n times.
type probe struct {
	typ     beehive.EventType
	reason  string
	message string
	detail  any
	n       int
}

// ClusterController reconciles a Cluster by probing its connection and recording
// what it saw. Each pass consumes one scripted burst; a real prober would derive
// the outcome from the endpoint, which is level-triggered as this script is not.
type ClusterController struct {
	bursts [][]probe
	passes int
	// Closed after the first burst commits. Events are written by a reconcile
	// worker, so main needs to know when there is a panel to render.
	probed chan struct{}
}

func (cc *ClusterController) Reconcile(ctx context.Context, client beehive.ControllerClient[ClusterStatus], obj *beehive.Object[ClusterSpec, ClusterStatus]) beehive.ReconcileResult {
	if cc.passes >= len(cc.bursts) {
		return beehive.Settled(0)
	}
	for _, p := range cc.bursts[cc.passes] {
		for range p.n {
			if err := client.AddEvent(ctx, obj.ID, beehive.EventSpec{
				Category: "connection", Type: p.typ, Reason: p.reason, Message: p.message, Detail: p.detail,
			}); err != nil {
				return beehive.Fail(err)
			}
		}
	}
	cc.passes++
	if cc.passes == 1 {
		close(cc.probed)
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

	bh, err := beehive.New(store)
	exitOnErr(err)

	// A flapping connection, scripted: the first burst is the panel below, the
	// second is the live probe the resumed watch reads.
	ctrl := &ClusterController{
		probed: make(chan struct{}),
		bursts: [][]probe{{
			{beehive.EventNormal, "Connected", "", nil, 16},
			{beehive.EventWarning, "TLSHandshake", "x509: certificate expired", nil, 5},
			{beehive.EventNormal, "Connected", "", nil, 7},
			{beehive.EventWarning, "ProbeFailed", "i/o timeout", ProbeDetail{Endpoint: "10.0.0.1:443", LatencyMs: 5000}, 18},
			{beehive.EventNormal, "Connected", "", nil, 4},
		}, {
			{beehive.EventWarning, "ProbeFailed", "i/o timeout", nil, 1},
		}},
	}
	_, err = beehive.Register(bh, ClusterGroupKind, ctrl)
	exitOnErr(err)

	stop, err := bh.Start(context.Background())
	exitOnErr(err)
	defer stopBeehive(stop)

	ctx := context.Background()
	client := beehive.NewClient[ClusterSpec, ClusterStatus](bh, ClusterGroupKind)

	cluster, err := client.Create(ctx, "primary", ClusterSpec{Endpoint: "10.0.0.1:443"})
	exitOnErr(err)
	fmt.Printf("created Cluster id=%d endpoint=%s\n\n", cluster.ID, cluster.Spec.Endpoint)

	// The create schedules the pass that writes the events; beehive promises the
	// pass, not when it runs. Waiting is this example's own business — without it
	// the snapshot below is a snapshot of nothing.
	<-ctrl.probed

	stream, err := client.WatchEvents(ctx, cluster.ID, beehive.WithEventCategory("connection"))
	exitOnErr(err)

	fmt.Println("connection-health panel (newest first):")
	renderPanel(stream.Runs)

	// A panel reconnects, and resumes from what it last rendered rather than
	// re-reading the log. The probe below arrives on its own commit.
	resumed, err := client.WatchEvents(ctx, cluster.ID,
		beehive.WithEventCategory("connection"),
		beehive.WithEventsResumeFrom(stream.ResourceVersion))
	exitOnErr(err)

	exitOnErr(client.Requeue(ctx, cluster.ID))
	fmt.Println("\nlive, resumed above the snapshot:")
	renderPanel([]beehive.Event{<-resumed.Events})
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
