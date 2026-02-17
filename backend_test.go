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

func TestStringCacheKeyMarshallAll(t *testing.T) {
	k := &StringCacheKey{}

	input := []string{"a", "b", "c"}
	marshalled := k.MarshallAll(input)
	assert.Equal(t, input, marshalled)

	unmarshalled, err := k.UnmarshalAll(marshalled)
	assert.Nil(t, err)
	assert.Equal(t, input, unmarshalled)
}

func TestIntCacheKeyMarshallAll(t *testing.T) {
	k := &IntCacheKey{}

	input := []int{1, 2, 3}
	marshalled := k.MarshallAll(input)
	assert.Equal(t, []string{"1", "2", "3"}, marshalled)

	unmarshalled, err := k.UnmarshalAll(marshalled)
	assert.Nil(t, err)
	assert.Equal(t, input, unmarshalled)
}

func TestIntCacheKeyUnmarshalError(t *testing.T) {
	k := &IntCacheKey{}

	_, err := k.Unmarshal("not_a_number")
	assert.NotNil(t, err)

	_, err = k.UnmarshalAll([]string{"1", "bad", "3"})
	assert.NotNil(t, err)
}
