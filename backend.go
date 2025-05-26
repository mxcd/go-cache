package cache

import (
	"context"
	"fmt"
	"strconv"
	"time"
)

type CacheKey[K comparable] interface {
	Marshal(K) string
	Unmarshal(string) (K, error)
	MarshallAll([]K) []string
	UnmarshalAll([]string) ([]K, error)
}

type StringCacheKey struct {
}

func (k *StringCacheKey) Marshal(key string) string {
	return key
}

func (k *StringCacheKey) MarshallAll(keys []string) []string {
	result := make([]string, len(keys))
	for i, key := range keys {
		result[i] = k.Marshal(key)
	}
	return result
}

func (k *StringCacheKey) Unmarshal(data string) (string, error) {
	return data, nil
}

func (k *StringCacheKey) UnmarshalAll(data []string) ([]string, error) {
	result := make([]string, len(data))
	for i, d := range data {
		result[i] = d
	}
	return result, nil
}

type IntCacheKey struct {
}

func (k *IntCacheKey) Marshal(key int) string {
	return fmt.Sprintf("%d", key)
}

func (k *IntCacheKey) MarshallAll(keys []int) []string {
	result := make([]string, len(keys))
	for i, key := range keys {
		result[i] = k.Marshal(key)
	}
	return result
}

func (k *IntCacheKey) Unmarshal(data string) (int, error) {
	return strconv.Atoi(data)
}

func (k *IntCacheKey) UnmarshalAll(data []string) ([]int, error) {
	result := make([]int, len(data))
	for i, d := range data {
		value, err := k.Unmarshal(d)
		if err != nil {
			return nil, err
		}
		result[i] = value
	}
	return result, nil
}

type StorageBackend[K comparable, V any] interface {
	Get(context.Context, K) (*V, error)
	Ttl(context.Context, K) (time.Duration, error)
	Set(context.Context, K, V) error
	Remove(context.Context, K) error
	RemovePrefix(context.Context, string) error
	Contains(context.Context, K) (bool, error)
	Load(context.Context) ([]CacheEntry[K, V], error)
	AddCallback(func(CacheEvent[K, V]))
}
