package sync

import (
	"context"
	"time"
)

type limitProvider interface {
	get(ctx context.Context, key string) (int, QueueingStrategy, bool, error)
}

var _ limitProvider = &cachedLimit{}

type cachedLimit struct {
	limit          int
	strategy       QueueingStrategy
	limitTimestamp time.Time
	TTL            time.Duration
	getter         GetSyncLimit
}

func newCachedLimit(getter GetSyncLimit, ttl time.Duration) *cachedLimit {
	return &cachedLimit{
		limit:          0,
		strategy:       StrictFIFO,
		limitTimestamp: time.Time{}, // very long ago, so first use will update
		TTL:            ttl,
		getter:         getter,
	}
}

// get returns the cached limit and queueing strategy, refreshing both once the
// TTL has elapsed. Only a change of limit is reported as changed: the limit
// drives a semaphore resize, whereas the strategy is read afresh on each use.
func (c *cachedLimit) get(ctx context.Context, key string) (int, QueueingStrategy, bool, error) {
	changed := false
	if nowFn().Sub(c.limitTimestamp) >= c.TTL {
		limit, strategy, err := c.getter(ctx, key)
		if err != nil {
			return c.limit, c.strategy, false, err
		}
		if limit != c.limit {
			c.limit = limit
			changed = true
		}
		c.strategy = strategy
		c.limitTimestamp = nowFn()
	}
	return c.limit, c.strategy, changed, nil
}
