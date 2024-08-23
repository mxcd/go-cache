package cache

import (
	"testing"

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
