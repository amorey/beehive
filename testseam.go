package beehive

import "github.com/amorey/beehive/internal/testseam"

// beehivetest writes through the same kindWriter a pass does. See
// docs/adr/2026-08-18-a-beehivetest-client-writes-status.md.
func init() {
	testseam.Open = func(bh any, gk GroupKind) testseam.Writer {
		return kindWriter{bh.(*Beehive), gk}
	}
}
