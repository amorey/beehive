package beehive

import "time"

const defaultMaxRetryInterval = 30 * time.Second

// reconciler drives the reconcile loop for a single registered controller.
// It owns the work queue, exponential backoff, and periodic resync timer.
type reconciler struct {
	gk               GroupKind
	resyncInterval   time.Duration
	maxRetryInterval time.Duration
}
