package sync

import (
	"context"
	"fmt"
	"time"

	"github.com/argoproj/argo-workflows/v4/util/sqldb"
)

type semaphore interface {
	acquire(ctx context.Context, holderKey string, tx *sqldb.SessionProxy) (bool, error)
	// reacquire re-establishes a recorded holder at controller startup.
	//
	// For an in-memory lock it force-registers the holder, ignoring the current
	// limit, so the in-memory count reflects persisted reality even when recorded
	// holders exceed a (since lowered) limit - new acquisitions then correctly
	// wait until the count drains below the limit, rather than dropping a holder
	// (a double-acquire) or poisoning the lock over a routine limit change.
	//
	// For a database-backed lock the database is the single source of truth:
	// reacquire mutates nothing and only asserts the recorded hold still exists
	// there. An error means the hold could not be verified - either the held row
	// is gone (e.g. expired while the controller was down) or the database could
	// not be queried - and the caller fails the holding workflow.
	reacquire(ctx context.Context, holderKey string, tx *sqldb.SessionProxy) error
	checkAcquire(ctx context.Context, holderKey string, tx *sqldb.SessionProxy) (bool, bool, string)
	tryAcquire(ctx context.Context, holderKey string, tx *sqldb.SessionProxy) (bool, string, error)
	release(ctx context.Context, key string) bool
	addToQueue(ctx context.Context, holderKey string, priority int32, creationTime time.Time) error
	removeFromQueue(ctx context.Context, holderKey string) error
	getCurrentHolders(ctx context.Context) ([]string, error)
	getCurrentPending(ctx context.Context) ([]string, error)
	getLimit(ctx context.Context) int // Testing only
	probeWaiting(ctx context.Context)
	lock(ctx context.Context) bool
	unlock(ctx context.Context)
}

// QueueingStrategy determines whether a waiter that cannot be admitted blocks
// the waiters behind it.
type QueueingStrategy string

const (
	// StrictFIFO admits only the head of the priority queue, so a waiter that
	// cannot be admitted blocks every waiter behind it even when slots are free.
	StrictFIFO QueueingStrategy = "StrictFIFO"
	// BestEffortFIFO admits waiters in priority order up to the number of free
	// slots, so a waiter that cannot be admitted does not block waiters behind it
	// that do fit. Priority still decides who is admitted sooner, but waiters
	// admitted together may acquire in any order among themselves.
	BestEffortFIFO QueueingStrategy = "BestEffortFIFO"
)

// ParseQueueingStrategy converts a configured value into a QueueingStrategy.
// An empty value selects the default, StrictFIFO, which preserves the behaviour
// of releases before the strategy was configurable.
func ParseQueueingStrategy(value string) (QueueingStrategy, error) {
	switch QueueingStrategy(value) {
	case "":
		return StrictFIFO, nil
	case StrictFIFO:
		return StrictFIFO, nil
	case BestEffortFIFO:
		return BestEffortFIFO, nil
	default:
		return "", fmt.Errorf("invalid queueing strategy %q, must be %q or %q", value, StrictFIFO, BestEffortFIFO)
	}
}

// expose for overriding in tests
var nowFn = time.Now
