package beehive

import "io"

// Store is the durable-store contract that Beehive depends on internally.
type Store interface {
	io.Closer
}
