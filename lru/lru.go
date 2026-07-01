// Package lru provides a thread-safe, fixed-size LRU cache with optional
// per-entry TTL expiration. It is a dependency-free, clean-room replacement
// for the third-party expirable LRU this library previously depended on.
//
// Semantics are kept compatible with that previous implementation:
//   - size <= 0 disables the size bound (unlimited entries).
//   - ttl  <= 0 disables expiration (entries live until evicted or removed).
//   - with ttl > 0, an entry expires ttl after its last write and is dropped
//     lazily on access and proactively by a background sweeper.
package lru

import (
	"container/list"
	"sync"
	"time"
)

// EvictCallback is invoked with the key and value of an entry when it is
// evicted by size pressure, removed, or reaped after expiration. It may be nil.
type EvictCallback[K comparable, V any] func(key K, value V)

type entry[K comparable, V any] struct {
	key       K
	value     V
	expiresAt time.Time // zero value => never expires
}

func (e *entry[K, V]) expired(now time.Time) bool {
	return !e.expiresAt.IsZero() && now.After(e.expiresAt)
}

// LRU is a thread-safe LRU cache with optional per-entry TTL.
type LRU[K comparable, V any] struct {
	mu      sync.Mutex
	size    int
	ttl     time.Duration
	ll      *list.List // front = most-recently-used, back = oldest
	items   map[K]*list.Element
	onEvict EvictCallback[K, V]
	done    chan struct{}
	closed  bool
}

// NewLRU builds a cache holding up to size entries (0 = unlimited) with the
// given per-entry ttl (0 = no expiration). onEvict may be nil.
func NewLRU[K comparable, V any](size int, onEvict EvictCallback[K, V], ttl time.Duration) *LRU[K, V] {
	if size < 0 {
		size = 0
	}
	c := &LRU[K, V]{
		size:    size,
		ttl:     ttl,
		ll:      list.New(),
		items:   make(map[K]*list.Element),
		onEvict: onEvict,
		done:    make(chan struct{}),
	}
	if ttl > 0 {
		go c.reap()
	}
	return c
}

// Add stores value under key, renewing its TTL, and reports whether the write
// evicted the oldest entry to stay within the size bound.
func (c *LRU[K, V]) Add(key K, value V) (evicted bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var expiresAt time.Time
	if c.ttl > 0 {
		expiresAt = time.Now().Add(c.ttl)
	}

	if el, ok := c.items[key]; ok {
		c.ll.MoveToFront(el)
		en := el.Value.(*entry[K, V])
		en.value = value
		en.expiresAt = expiresAt
		return false
	}

	el := c.ll.PushFront(&entry[K, V]{key: key, value: value, expiresAt: expiresAt})
	c.items[key] = el

	if c.size > 0 && c.ll.Len() > c.size {
		c.removeOldest()
		return true
	}
	return false
}

// Get returns the value for key. A missing or expired entry returns ok=false.
func (c *LRU[K, V]) Get(key K) (value V, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	el, ok := c.items[key]
	if !ok {
		return value, false
	}
	en := el.Value.(*entry[K, V])
	if en.expired(time.Now()) {
		return value, false
	}
	c.ll.MoveToFront(el)
	return en.value, true
}

// Contains reports whether key is present, without updating recency. As in the
// previous implementation it does not check expiration.
func (c *LRU[K, V]) Contains(key K) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.items[key]
	return ok
}

// Remove deletes key, returning whether it was present.
func (c *LRU[K, V]) Remove(key K) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		c.removeElement(el)
		return true
	}
	return false
}

// Keys returns all keys from oldest to newest.
func (c *LRU[K, V]) Keys() []K {
	c.mu.Lock()
	defer c.mu.Unlock()
	keys := make([]K, 0, len(c.items))
	for el := c.ll.Back(); el != nil; el = el.Prev() {
		keys = append(keys, el.Value.(*entry[K, V]).key)
	}
	return keys
}

// Len returns the number of entries currently held (including any expired but
// not yet reaped).
func (c *LRU[K, V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ll.Len()
}

// Close stops the background expiration sweeper. It is safe to call more than
// once. A cache with no TTL has no sweeper, so Close only flips the flag there.
func (c *LRU[K, V]) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		c.closed = true
		close(c.done)
	}
}

func (c *LRU[K, V]) removeOldest() {
	if el := c.ll.Back(); el != nil {
		c.removeElement(el)
	}
}

func (c *LRU[K, V]) removeElement(el *list.Element) {
	c.ll.Remove(el)
	en := el.Value.(*entry[K, V])
	delete(c.items, en.key)
	if c.onEvict != nil {
		c.onEvict(en.key, en.value)
	}
}

// reap runs until Close, periodically dropping expired entries so untouched
// keys don't linger in an unbounded (size 0) cache.
// ponytail: one coarse full-sweep per ttl instead of hashicorp's 100 time
// buckets. Expiry is still enforced lazily in Get, so the sweep only reclaims
// memory; tighten the interval if dead-entry memory ever becomes measurable.
func (c *LRU[K, V]) reap() {
	ticker := time.NewTicker(c.ttl)
	defer ticker.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			c.deleteExpired()
		}
	}
}

func (c *LRU[K, V]) deleteExpired() {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	// All entries share one ttl, so list order (back = oldest) is also expiry
	// order: stop at the first still-valid entry.
	for {
		el := c.ll.Back()
		if el == nil || !el.Value.(*entry[K, V]).expired(now) {
			return
		}
		c.removeElement(el)
	}
}
