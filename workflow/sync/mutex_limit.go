package sync

import "context"

type mutexLimit struct{}

var _ limitProvider = &mutexLimit{}

// get always reports StrictFIFO: a size-1 lock has no batch to admit, so the
// strategy cannot change its behaviour.
func (*mutexLimit) get(_ context.Context, _ string) (int, QueueingStrategy, bool, error) {
	return 1, StrictFIFO, false, nil
}
