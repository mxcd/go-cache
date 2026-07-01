package lru

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGetAddRemoveContains(t *testing.T) {
	c := NewLRU[string, int](0, nil, 0)

	_, ok := c.Get("missing")
	assert.False(t, ok)
	assert.False(t, c.Contains("missing"))

	c.Add("a", 1)
	v, ok := c.Get("a")
	assert.True(t, ok)
	assert.Equal(t, 1, v)
	assert.True(t, c.Contains("a"))

	// overwrite renews the value
	c.Add("a", 2)
	v, _ = c.Get("a")
	assert.Equal(t, 2, v)
	assert.Equal(t, 1, c.Len())

	assert.True(t, c.Remove("a"))
	assert.False(t, c.Remove("a"))
	_, ok = c.Get("a")
	assert.False(t, ok)
	assert.False(t, c.Contains("a"))
}

func TestSizeEviction(t *testing.T) {
	var evicted []string
	c := NewLRU[string, int](2, func(k string, _ int) {
		evicted = append(evicted, k)
	}, 0)

	assert.False(t, c.Add("a", 1))
	assert.False(t, c.Add("b", 2))
	// touch "a" so "b" becomes the least-recently-used
	_, _ = c.Get("a")
	assert.True(t, c.Add("c", 3)) // over size -> evicts oldest ("b")

	assert.Equal(t, []string{"b"}, evicted)
	assert.False(t, c.Contains("b"))
	assert.True(t, c.Contains("a"))
	assert.True(t, c.Contains("c"))
	assert.Equal(t, 2, c.Len())
}

func TestUnlimitedSize(t *testing.T) {
	c := NewLRU[int, int](0, nil, 0)
	for i := 0; i < 1000; i++ {
		c.Add(i, i)
	}
	assert.Equal(t, 1000, c.Len())
	v, ok := c.Get(0)
	assert.True(t, ok)
	assert.Equal(t, 0, v)
}

func TestKeysOrderOldestToNewest(t *testing.T) {
	c := NewLRU[string, int](0, nil, 0)
	c.Add("first", 1)
	c.Add("second", 2)
	c.Add("third", 3)
	assert.Equal(t, []string{"first", "second", "third"}, c.Keys())
}

func TestNoExpirationWhenTTLZero(t *testing.T) {
	c := NewLRU[string, int](0, nil, 0)
	c.Add("a", 1)
	time.Sleep(20 * time.Millisecond)
	v, ok := c.Get("a")
	assert.True(t, ok)
	assert.Equal(t, 1, v)
}

func TestLazyExpirationOnGet(t *testing.T) {
	c := NewLRU[string, int](0, nil, 10*time.Millisecond)
	defer c.Close()

	c.Add("a", 1)
	v, ok := c.Get("a")
	assert.True(t, ok)
	assert.Equal(t, 1, v)

	time.Sleep(25 * time.Millisecond)
	_, ok = c.Get("a")
	assert.False(t, ok)
}

func TestAddRenewsTTL(t *testing.T) {
	c := NewLRU[string, int](0, nil, 40*time.Millisecond)
	defer c.Close()

	c.Add("a", 1)
	time.Sleep(25 * time.Millisecond)
	c.Add("a", 2) // renew before expiry
	time.Sleep(25 * time.Millisecond)
	v, ok := c.Get("a") // 50ms since first write, but only 25ms since renewal
	assert.True(t, ok)
	assert.Equal(t, 2, v)
}

// deleteExpired is exercised deterministically (no reliance on ticker timing).
func TestDeleteExpiredDeterministic(t *testing.T) {
	c := NewLRU[string, int](0, nil, 10*time.Millisecond)
	defer c.Close()

	c.Add("a", 1)
	c.Add("b", 2)
	assert.Equal(t, 2, c.Len())

	time.Sleep(25 * time.Millisecond)
	c.deleteExpired()
	assert.Equal(t, 0, c.Len())
}

// The background sweeper reclaims untouched expired entries in an unbounded cache.
func TestBackgroundReaper(t *testing.T) {
	c := NewLRU[string, int](0, nil, 25*time.Millisecond)
	defer c.Close()

	for i := 0; i < 10; i++ {
		c.Add(fmt.Sprintf("k%d", i), i)
	}
	assert.Equal(t, 10, c.Len())

	assert.Eventually(t, func() bool {
		return c.Len() == 0
	}, 2*time.Second, 10*time.Millisecond)
}

func TestCloseIdempotentAndStopsReaper(t *testing.T) {
	c := NewLRU[string, int](0, nil, 10*time.Millisecond)
	c.Close()
	c.Close() // must not panic on double close

	// After Close the sweeper is gone; expired entries survive until accessed.
	c.Add("a", 1)
	time.Sleep(30 * time.Millisecond)
	assert.Equal(t, 1, c.Len()) // not reaped
	_, ok := c.Get("a")         // lazy expiry still applies
	assert.False(t, ok)
}

func TestNegativeSizeIsUnlimited(t *testing.T) {
	c := NewLRU[int, int](-1, nil, 0)
	for i := 0; i < 500; i++ {
		c.Add(i, i)
	}
	assert.Equal(t, 500, c.Len())
}

func TestAddReturnsEvicted(t *testing.T) {
	c := NewLRU[string, int](1, nil, 0)
	assert.False(t, c.Add("a", 1)) // fits
	assert.False(t, c.Add("a", 2)) // overwrite, no eviction
	assert.True(t, c.Add("b", 3))  // over size -> evicts "a"
}

// Contract: Contains deliberately does NOT check expiration (kept compatible
// with the previous implementation). An expired-but-unreaped key still reports
// present, even though Get misses.
func TestContainsIgnoresExpiration(t *testing.T) {
	c := NewLRU[string, int](0, nil, 10*time.Millisecond)
	c.Close() // stop the reaper so the expired entry lingers

	c.Add("a", 1)
	time.Sleep(25 * time.Millisecond)

	_, ok := c.Get("a")
	assert.False(t, ok) // Get enforces expiry
	assert.True(t, c.Contains("a"))
}

func TestOnEvictFiresOnRemoveAndReap(t *testing.T) {
	var mu sync.Mutex
	var evicted []string
	onEvict := func(k string, _ int) {
		mu.Lock()
		evicted = append(evicted, k)
		mu.Unlock()
	}

	c := NewLRU[string, int](0, onEvict, 20*time.Millisecond)
	defer c.Close()

	c.Add("removed", 1)
	c.Add("reaped", 2)

	c.Remove("removed")
	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(evicted) == 2
	}, 2*time.Second, 10*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	assert.Contains(t, evicted, "removed") // Remove path
	assert.Contains(t, evicted, "reaped")  // background reap path
}

func TestConcurrentAccess(t *testing.T) {
	c := NewLRU[int, int](128, nil, 50*time.Millisecond)
	defer c.Close()

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				k := (g*2000 + i) % 256
				c.Add(k, i)
				c.Get(k)
				if i%7 == 0 {
					c.Remove(k)
				}
				_ = c.Keys()
				_ = c.Contains(k)
			}
		}(g)
	}
	wg.Wait()
	assert.LessOrEqual(t, c.Len(), 128)
}
