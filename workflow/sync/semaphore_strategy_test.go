package sync

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/argoproj/argo-workflows/v4/util/logging"
)

// newStrategySemaphore creates an in-memory semaphore with an explicit queueing
// strategy, recording every key passed to nextWorkflow.
func newStrategySemaphore(ctx context.Context, t *testing.T, limit int, strategy QueueingStrategy) (*prioritySemaphore, *[]string) {
	t.Helper()
	var notified []string
	sem, err := newInternalSemaphore(ctx, "default/ConfigMap/my-config/bar", func(key string) {
		notified = append(notified, key)
	}, func(context.Context, string) (int, QueueingStrategy, error) { return limit, strategy, nil }, 0)
	require.NoError(t, err)
	return sem, &notified
}

func TestParseQueueingStrategy(t *testing.T) {
	// Absent configuration must select the pre-existing behaviour.
	strategy, err := ParseQueueingStrategy("")
	require.NoError(t, err)
	assert.Equal(t, StrictFIFO, strategy)

	strategy, err = ParseQueueingStrategy("StrictFIFO")
	require.NoError(t, err)
	assert.Equal(t, StrictFIFO, strategy)

	strategy, err = ParseQueueingStrategy("BestEffortFIFO")
	require.NoError(t, err)
	assert.Equal(t, BestEffortFIFO, strategy)

	// Anything else is rejected rather than silently defaulted, so a typo such as
	// "besteffortfifo" cannot quietly leave a cluster on the wrong strategy.
	_, err = ParseQueueingStrategy("besteffortfifo")
	require.Error(t, err)
	_, err = ParseQueueingStrategy("FIFO")
	require.Error(t, err)
}

// TestStrictFIFOBlocksNonHead pins the defining property of StrictFIFO: a waiter
// that is not at the front is refused even though slots are free.
func TestStrictFIFOBlocksNonHead(t *testing.T) {
	ctx := logging.TestContext(t.Context())
	sem, _ := newStrategySemaphore(ctx, t, 3, StrictFIFO)

	now := time.Now()
	for _, key := range []string{"default/wf-01", "default/wf-02", "default/wf-03"} {
		require.NoError(t, sem.addToQueue(ctx, key, 0, now))
		now = now.Add(time.Second)
	}

	// Two slots are free, but only the head may proceed.
	acquired, already, _ := sem.checkAcquire(ctx, "default/wf-02", nil)
	assert.False(t, acquired, "wf-02 must be refused under StrictFIFO despite free slots")
	assert.False(t, already)

	acquired, _, _ = sem.checkAcquire(ctx, "default/wf-01", nil)
	assert.True(t, acquired, "the head must be allowed to proceed")
}

// TestBestEffortFIFOAdmitsNonHead is the same situation under BestEffortFIFO:
// the free slots are handed out rather than reserved for the head.
func TestBestEffortFIFOAdmitsNonHead(t *testing.T) {
	ctx := logging.TestContext(t.Context())
	sem, _ := newStrategySemaphore(ctx, t, 3, BestEffortFIFO)

	now := time.Now()
	for _, key := range []string{"default/wf-01", "default/wf-02", "default/wf-03"} {
		require.NoError(t, sem.addToQueue(ctx, key, 0, now))
		now = now.Add(time.Second)
	}

	acquired, _, _ := sem.checkAcquire(ctx, "default/wf-02", nil)
	assert.True(t, acquired, "wf-02 fits in a free slot, so it must not be blocked by the head")
}

// TestBestEffortFIFOStillRespectsLimit is the safety property: admitting
// out of order must never admit more than the limit.
func TestBestEffortFIFOStillRespectsLimit(t *testing.T) {
	ctx := logging.TestContext(t.Context())
	sem, _ := newStrategySemaphore(ctx, t, 2, BestEffortFIFO)

	now := time.Now()
	keys := []string{"default/wf-01", "default/wf-02", "default/wf-03", "default/wf-04"}
	for _, key := range keys {
		require.NoError(t, sem.addToQueue(ctx, key, 0, now))
		now = now.Add(time.Second)
	}

	acquiredCount := 0
	for _, key := range keys {
		acquired, _, err := sem.tryAcquire(ctx, key, nil)
		require.NoError(t, err)
		if acquired {
			acquiredCount++
		}
	}
	assert.Equal(t, 2, acquiredCount, "must not admit beyond the limit")
	assert.Len(t, sem.lockHolder, 2)
}

// TestBestEffortFIFOFillsAllFreeSlotsInOneRound is the throughput property that
// motivates the strategy: K free slots are filled in a single pass over the
// waiters, not one per pass.
func TestBestEffortFIFOFillsAllFreeSlotsInOneRound(t *testing.T) {
	ctx := logging.TestContext(t.Context())
	const limit = 5
	sem, _ := newStrategySemaphore(ctx, t, limit, BestEffortFIFO)

	now := time.Now()
	keys := make([]string, 0, 10)
	for i := range 10 {
		key := benchKey(i)
		keys = append(keys, key)
		require.NoError(t, sem.addToQueue(ctx, key, 0, now.Add(time.Duration(i)*time.Second)))
	}

	// Reconcile in reverse order, so the head is visited last. Under StrictFIFO
	// this admits at most one; under BestEffortFIFO every free slot is filled.
	for i := len(keys) - 1; i >= 0; i-- {
		_, _, err := sem.tryAcquire(ctx, keys[i], nil)
		require.NoError(t, err)
	}
	assert.Len(t, sem.lockHolder, limit, "one pass must fill every free slot")
}

// TestBestEffortFIFORemoveReclaimsGrant checks a workflow deleted while granted
// does not strand its grant, which would permanently shrink the grant set.
func TestBestEffortFIFORemoveReclaimsGrant(t *testing.T) {
	ctx := logging.TestContext(t.Context())
	sem, _ := newStrategySemaphore(ctx, t, 1, BestEffortFIFO)

	now := time.Now()
	require.NoError(t, sem.addToQueue(ctx, "default/wf-01", 0, now))
	require.NoError(t, sem.addToQueue(ctx, "default/wf-02", 0, now.Add(time.Second)))

	// Grant wf-01 without acquiring, then delete it.
	acquired, _, _ := sem.checkAcquire(ctx, "default/wf-01", nil)
	require.True(t, acquired)
	require.True(t, sem.granted["default/wf-01"])

	require.NoError(t, sem.removeFromQueue(ctx, "default/wf-01"))
	assert.NotContains(t, sem.granted, "default/wf-01", "a removed workflow must not keep its grant")

	// The slot must now be available to wf-02.
	acquired, _, err := sem.tryAcquire(ctx, "default/wf-02", nil)
	require.NoError(t, err)
	assert.True(t, acquired, "the reclaimed grant must let the next waiter through")
}

// TestBestEffortFIFOReacquireReclaimsGrant covers controller restart: a holder
// re-established by reacquire is no longer a waiter, so any grant it held must
// be dropped. Leaving one behind would count against the grant set forever.
func TestBestEffortFIFOReacquireReclaimsGrant(t *testing.T) {
	ctx := logging.TestContext(t.Context())
	sem, _ := newStrategySemaphore(ctx, t, 2, BestEffortFIFO)

	now := time.Now()
	require.NoError(t, sem.addToQueue(ctx, "default/wf-01", 0, now))

	acquired, _, _ := sem.checkAcquire(ctx, "default/wf-01", nil)
	require.True(t, acquired)
	require.True(t, sem.granted["default/wf-01"])

	require.NoError(t, sem.reacquire(ctx, "default/wf-01", nil))
	assert.NotContains(t, sem.granted, "default/wf-01", "a reestablished holder must not keep its grant")
	assert.Contains(t, sem.lockHolder, "default/wf-01")
}

// TestBestEffortFIFODoesNotRewakeGrantedHead checks that repeated release bursts
// do not re-enqueue an already-granted waiter. That repeated wake is the
// enqueue amplification the strategy exists to remove.
func TestBestEffortFIFODoesNotRewakeGrantedHead(t *testing.T) {
	ctx := logging.TestContext(t.Context())
	sem, notified := newStrategySemaphore(ctx, t, 2, BestEffortFIFO)

	now := time.Now()
	require.NoError(t, sem.addToQueue(ctx, "default/wf-01", 0, now))
	require.NoError(t, sem.addToQueue(ctx, "default/wf-02", 0, now.Add(time.Second)))

	sem.notifyWaiters(ctx)
	first := len(*notified)
	require.Positive(t, first, "waiters must be woken at least once")

	// Further notifications must not re-wake the same already-granted waiters.
	sem.notifyWaiters(ctx)
	sem.notifyWaiters(ctx)
	assert.Len(t, *notified, first, "already-granted waiters must not be re-enqueued")
}

// TestStrictFIFOWrongEntryNotRemovedOnAcquire covers the queue-bookkeeping bug
// reported upstream: tryAcquire used pending.pop(), which removes the queue
// FRONT rather than the acquirer. checkAcquire permits a non-front node of the
// SAME workflow to acquire (isSameWorkflowNodeKeys), so under StrictFIFO the
// acquirer is not necessarily the front and the wrong entry could be dropped.
//
// This test pins the fix: after node-bbb acquires, node-bbb's entry is gone and
// its sibling node-aaa is still queued.
func TestStrictFIFOWrongEntryNotRemovedOnAcquire(t *testing.T) {
	ctx := logging.TestContext(t.Context())
	sem, _ := newStrategySemaphore(ctx, t, 1, StrictFIFO)

	now := time.Now()
	// Two nodes of the same workflow. node-aaa is the queue front.
	require.NoError(t, sem.addToQueue(ctx, "default/wf-01/node-aaa", 0, now))
	require.NoError(t, sem.addToQueue(ctx, "default/wf-01/node-bbb", 0, now.Add(time.Second)))

	// node-bbb is not the front, but is the same workflow, so StrictFIFO admits it.
	acquired, _, err := sem.tryAcquire(ctx, "default/wf-01/node-bbb", nil)
	require.NoError(t, err)
	require.True(t, acquired, "a same-workflow sibling is admitted under StrictFIFO")

	pending, err := sem.getCurrentPending(ctx)
	require.NoError(t, err)
	assert.Contains(t, pending, "default/wf-01/node-aaa",
		"the front entry must survive: a sibling acquiring must not evict it")
	assert.NotContains(t, pending, "default/wf-01/node-bbb",
		"the acquirer's own entry must be the one removed")
}
