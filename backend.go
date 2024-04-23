package cache

import (
	"context"
	"fmt"
	"strconv"
)

type CacheKey[K comparable] interface {
	Marshal(K) string
	Unmarshal(string) (K, error)
}

type StringCacheKey struct {
}

func (k *StringCacheKey) Marshal(key string) string {
	return key
}

func (k *StringCacheKey) Unmarshal(data string) (string, error) {
	return data, nil
}

type IntCacheKey struct {
}

func (k *IntCacheKey) Marshal(key int) string {
	return fmt.Sprintf("%d", key)
}

func (k *IntCacheKey) Unmarshal(data string) (int, error) {
	return strconv.Atoi(data)
}

type StorageBackend[K comparable, V any] interface {
	Get(context.Context, K) (*V, error)
	Set(context.Context, K, V) error
	Remove(context.Context, K) error
	Contains(context.Context, K) (bool, error)
	Load(context.Context) ([]CacheEntry[K, V], error)
	AddCallback(func(CacheEvent[K, V]))
}
