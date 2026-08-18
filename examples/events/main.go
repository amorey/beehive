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
// panel. A "Cluster" resource is probed by its own controller, which records one
// event per connection outcome; consecutive identical outcomes coalesce into
// runs, so a flapping cluster produces the aggregated, newest-first timeline a
// panel renders:
//
//	Create(spec) -> reconcile AddEvent×N -> Client.WatchEvents -> render, then resume
//
// Run it with `go run ./examples/events/main.go`.
package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/amorey/beehive"
	"github.com/amorey/beehive/sqlite"
)

// ClusterGroupKind identifies the resource. Empty Group == core group.
var ClusterGroupKind = beehive.GroupKind{Group: "kstack.sh", Kind: "Cluster"}

// ClusterSpec is the desired state the user writes. Probes stands in for a real
// endpoint: it is what the controller would observe if it dialled one.
type ClusterSpec struct {
	Endpoint string
	Probes   []Probe
}

// Probe is one connection outcome, seen Count times in a row.
type Probe struct {
	Type    beehive.EventType `json:"type"`
	Reason  string            `json:"reason"`
	Message string            `json:"message,omitempty"`
	Detail  *ProbeDetail      `json:"detail,omitempty"`
	Count   int               `json:"count"`
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

// ClusterController records what probing the cluster observed. Stateless and
// level-triggered: it reads the outcomes from the spec it was handed, and the
// handshake is what keeps a repeat pass from appending them twice.
type ClusterController struct{}

func (cc *ClusterController) Reconcile(ctx context.Context, client beehive.ControllerClient[ClusterStatus], obj *beehive.Object[ClusterSpec, ClusterStatus]) beehive.ReconcileResult {
	if settled(obj) {
		return beehive.Settled(0)
	}
	// One transaction for the burst: the store has a single write connection, so
	// an event apiece would be an event's worth of commits.
	err := client.Within(ctx, func(ctx context.Context) error {
		for _, p := range obj.Spec.Probes {
			var detail any
			if p.Detail != nil {
				detail = p.Detail
			}
			for range p.Count {
				if err := client.AddEvent(ctx, obj.ID, beehive.EventSpec{
					Category: "connection", Type: p.Type, Reason: p.Reason, Message: p.Message, Detail: detail,
				}); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return beehive.Fail(err)
	}
	return beehive.Settled(0)
}

// settled reports whether this generation's events are already in the log:
// beehive records the generation a Settled pass observed.
func settled(obj *beehive.Object[ClusterSpec, ClusterStatus]) bool {
	return obj.ObservedGeneration != nil && *obj.ObservedGeneration >= obj.Generation
}

// waitSettled blocks until id's controller has observed its current generation.
// The checkpoint any consumer can wait on, and here it is what says the panel
// below has something to render.
func waitSettled(ctx context.Context, client beehive.Client[ClusterSpec, ClusterStatus], id beehive.ObjectID) {
	stream, err := client.Watch(ctx, id)
	exitOnErr(err)
	if stream.Object != nil && settled(stream.Object) {
		return
	}
	for change := range stream.Changes {
		if change.Object != nil && settled(change.Object) {
			return
		}
	}
	exitOnErr(cmp.Or(stream.Err(), errors.New("the watch ended before the cluster settled")))
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

	err = beehive.Register(bh, ClusterGroupKind, &ClusterController{})
	exitOnErr(err)

	stop, err := bh.Start(context.Background())
	exitOnErr(err)
	defer stopBeehive(stop)

	ctx := context.Background()
	client := beehive.NewClient[ClusterSpec, ClusterStatus](bh, ClusterGroupKind)

	// A flapping connection, as the controller would have observed it.
	cluster, err := client.Create(ctx, "primary", ClusterSpec{
		Endpoint: "10.0.0.1:443",
		Probes: []Probe{
			{Type: beehive.EventNormal, Reason: "Connected", Count: 16},
			{Type: beehive.EventWarning, Reason: "TLSHandshake", Message: "x509: certificate expired", Count: 5},
			{Type: beehive.EventNormal, Reason: "Connected", Count: 7},
			{Type: beehive.EventWarning, Reason: "ProbeFailed", Message: "i/o timeout", Count: 18,
				Detail: &ProbeDetail{Endpoint: "10.0.0.1:443", LatencyMs: 5000}},
			{Type: beehive.EventNormal, Reason: "Connected", Count: 4},
		},
	})
	exitOnErr(err)
	fmt.Printf("created Cluster id=%d endpoint=%s\n\n", cluster.ID, cluster.Spec.Endpoint)

	// The events are written by the pass the create schedules, so wait for that
	// pass to settle before reading a snapshot of them.
	waitSettled(ctx, client, cluster.ID)

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

	_, err = client.Update(ctx, cluster.ID, ClusterSpec{
		Endpoint: "10.0.0.1:443",
		Probes:   []Probe{{Type: beehive.EventWarning, Reason: "ProbeFailed", Message: "i/o timeout", Count: 1}},
	})
	exitOnErr(err)
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
