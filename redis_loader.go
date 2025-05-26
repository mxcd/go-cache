package cache

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/vmihailenco/msgpack/v5"
)

func (b *RedisStorageBackend[K, V]) fetchValues(ctx context.Context, keys []string, resultsChan chan<- map[string]V, wg *sync.WaitGroup) {
	defer wg.Done()
	keyValues := make(map[string]V)
	for _, key := range keys {
		value, err := b.Client.Get(ctx, key).Bytes()

		if err != nil {
			fmt.Printf("Error fetching value for key %s: %v\n", key, err)
			continue
		}

		var unmarshalledValue V
		err = msgpack.Unmarshal(value, &unmarshalledValue)
		if err == nil {
			keyValues[key] = unmarshalledValue
		} else {
			fmt.Printf("Error unmarshalling value for key %s: %v\n", key, err)
		}
	}
	resultsChan <- keyValues
}

func (b *RedisStorageBackend[K, V]) fetchEntriesWithPrefix(ctx context.Context, prefix string, batchSize int) (map[K]V, error) {
	var cursor uint64
	var err error
	resultsChan := make(chan map[string]V)
	var wg sync.WaitGroup

	keyPattern := fmt.Sprintf("%s:%s*", b.Options.KeyPrefix, prefix)

	for {
		var scanKeys []string
		scanKeys, cursor, err = b.Client.Scan(ctx, cursor, keyPattern, 0).Result()
		if err != nil {
			close(resultsChan)
			return nil, err
		}

		// Process the keys in batches.
		for i := 0; i < len(scanKeys); i += batchSize {
			end := i + batchSize
			if end > len(scanKeys) {
				end = len(scanKeys)
			}
			wg.Add(1)
			go b.fetchValues(ctx, scanKeys[i:end], resultsChan, &wg)
		}

		if cursor == 0 {
			break
		}
	}

	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	keys := make(map[K]V)
	for keyMap := range resultsChan {
		for key, value := range keyMap {
			key = strings.TrimPrefix(key, b.Options.KeyPrefix+":")
			unmarshalledKey, err := b.Options.CacheKey.Unmarshal(key)
			if err != nil {
				fmt.Printf("Error marshalling key %v: %v\n", key, err)
				continue
			}

			keys[unmarshalledKey] = value
		}
	}

	return keys, nil
}

func (b *RedisStorageBackend[K, V]) fetchKeysWithPrefix(ctx context.Context, prefix string) ([]K, error) {
	var cursor uint64
	var err error

	keyPattern := fmt.Sprintf("%s:%s*", b.Options.KeyPrefix, prefix)

	var stringKeys []string

	for {
		var scanKeys []string
		scanKeys, cursor, err = b.Client.Scan(ctx, cursor, keyPattern, 0).Result()
		if err != nil {
			return nil, err
		}

		stringKeys = append(stringKeys, scanKeys...)

		if cursor == 0 {
			break
		}
	}

	keys := []K{}

	for _, key := range stringKeys {
		key = strings.TrimPrefix(key, b.Options.KeyPrefix+":")
		unmarshalledKey, err := b.Options.CacheKey.Unmarshal(key)
		if err != nil {
			fmt.Printf("Error marshalling key %v: %v\n", key, err)
			continue
		}

		keys = append(keys, unmarshalledKey)
	}

	return keys, nil
}
