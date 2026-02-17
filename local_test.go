package cache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLocalCacheString(t *testing.T) {
	cache := NewLocalCache[string, string](&LocalCacheOptions[string]{
		TTL:      0,
		Size:     0,
		CacheKey: &StringCacheKey{},
	})

	assert.NotNil(t, cache)
	cache.Set("foo", "bar")
	value, ok := cache.Get("foo")
	assert.True(t, ok)
	assert.Equal(t, "bar", *value)
	ok = cache.Remove("foo")
	assert.True(t, ok)
	value, ok = cache.Get("foo")
	assert.False(t, ok)
	assert.Nil(t, value)
}

func TestLocalCacheInt(t *testing.T) {
	cache := NewLocalCache[int, int](&LocalCacheOptions[int]{
		TTL:      0,
		Size:     0,
		CacheKey: &IntCacheKey{},
	})

	assert.NotNil(t, cache)
	cache.Set(1, 2)
	value, ok := cache.Get(1)
	assert.True(t, ok)
	assert.Equal(t, 2, *value)
	ok = cache.Remove(1)
	assert.True(t, ok)
	value, ok = cache.Get(1)
	assert.False(t, ok)
	assert.Nil(t, value)
}

func TestLocalCacheStruct(t *testing.T) {
	type TestStruct struct {
		Foo string
	}

	cache := NewLocalCache[int, TestStruct](&LocalCacheOptions[int]{
		TTL:      0,
		Size:     0,
		CacheKey: &IntCacheKey{},
	})

	assert.NotNil(t, cache)
	cache.Set(1, TestStruct{Foo: "bar"})
	value, ok := cache.Get(1)
	assert.True(t, ok)
	assert.Equal(t, "bar", value.Foo)
	ok = cache.Remove(1)
	assert.True(t, ok)
	value, ok = cache.Get(1)
	assert.False(t, ok)
	assert.Nil(t, value)
}

func TestLocalCacheLoad(t *testing.T) {
	cache := NewLocalCache[string, string](&LocalCacheOptions[string]{
		TTL:      0,
		Size:     0,
		CacheKey: &StringCacheKey{},
	})

	assert.NotNil(t, cache)
	value, ok := cache.Get("foo")
	assert.False(t, ok)
	assert.Nil(t, value)
	value, ok = cache.Get("fizz")
	assert.False(t, ok)
	assert.Nil(t, value)

	cache.Set("foo", "bar")
	cache.Set("fizz", "buzz")

	values, err := cache.Load()
	assert.Nil(t, err)
	assert.Len(t, values, 2)

	assert.Equal(t, "foo", values[0].Key)
	assert.Equal(t, "bar", *values[0].Value)

	assert.Equal(t, "fizz", values[1].Key)
	assert.Equal(t, "buzz", *values[1].Value)
}

func TestLocalCacheRemovePrefix(t *testing.T) {
	cache := NewLocalCache[string, string](&LocalCacheOptions[string]{
		TTL:      0,
		Size:     0,
		CacheKey: &StringCacheKey{},
	})
	assert.NotNil(t, cache)

	cache.Set("foo:bar", "baz")
	cache.Set("foo:baz", "qux")
	cache.Set("fizz:buzz", "fizzbuzz")
	cache.Set("fizz:qux", "fizzqux")
	cache.Set("qux:foo", "quxbar")
	cache.Set("qux:baz", "quxbaz")
	assert.Len(t, cache.Cache.Keys(), 6)
	cache.RemovePrefix("foo")

	assert.Len(t, cache.Cache.Keys(), 4)
	assert.False(t, cache.Contains("foo:bar"))
	assert.False(t, cache.Contains("foo:baz"))

	cache.RemovePrefix("fizz")
	assert.Len(t, cache.Cache.Keys(), 2)
	assert.False(t, cache.Contains("fizz:buzz"))
	assert.False(t, cache.Contains("fizz:qux"))

	cache.RemovePrefix("qux")
	assert.Len(t, cache.Cache.Keys(), 0)
	assert.False(t, cache.Contains("qux:foo"))
	assert.False(t, cache.Contains("qux:baz"))
}

func TestLocalCacheOptionsAccessors(t *testing.T) {
	ck := &StringCacheKey{}
	opts := &LocalCacheOptions[string]{
		TTL:      5 * time.Second,
		Size:     100,
		CacheKey: ck,
	}

	assert.Equal(t, 5*time.Second, opts.GetTtl())
	assert.Equal(t, 100, opts.GetSize())
	assert.Equal(t, ck, opts.GetCacheKey())
}

func TestLocalCacheContains(t *testing.T) {
	cache := NewLocalCache[string, string](&LocalCacheOptions[string]{
		TTL:      0,
		Size:     0,
		CacheKey: &StringCacheKey{},
	})

	assert.False(t, cache.Contains("missing"))

	cache.Set("present", "value")
	assert.True(t, cache.Contains("present"))

	cache.Remove("present")
	assert.False(t, cache.Contains("present"))
}
