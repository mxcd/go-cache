package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCacheKey(t *testing.T) {
	cacheKey := "foo"
	assert.Equal(t, "foo", cacheKey)

	stringCacheKey := &StringCacheKey{}
	assert.Equal(t, "foo", stringCacheKey.Marshal("foo"))
	key, err := stringCacheKey.Unmarshal("foo")
	assert.Nil(t, err)
	assert.Equal(t, "foo", key)

	intCacheKey := &IntCacheKey{}
	assert.Equal(t, "1", intCacheKey.Marshal(1))
	keyInt, err := intCacheKey.Unmarshal("1")
	assert.Nil(t, err)
	assert.Equal(t, 1, keyInt)
}
