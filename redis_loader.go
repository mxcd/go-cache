package cache

import (
	"context"
	"fmt"
	"sync"

	"github.com/vmihailenco/msgpack/v5"
)

func (b *RedisStorageBackend[K, V]) fetchValues(ctx context.Context, keys []string, resultsChan chan<- map[K]V, wg *sync.WaitGroup) {
	defer wg.Done()
	keyValues := make(map[K]V)
	for _, key := range keys {
		value, err := b.Client.Get(ctx, key).Bytes()

		if err != nil {
			fmt.Printf("Error fetching value for key %s: %v\n", key, err)
			continue
		}

		var unmarshalledValue V
		err = msgpack.Unmarshal(value, &unmarshalledValue)
		if err == nil {
			unmarshalledKey, err := b.Options.CacheKey.Unmarshal(key)
			if err != nil {
				fmt.Printf("Error marshalling key %v: %v\n", key, err)
				continue
			}
			keyValues[unmarshalledKey] = unmarshalledValue
		} else {
			fmt.Printf("Error unmarshalling value for key %s: %v\n", key, err)
		}
	}
	resultsChan <- keyValues
}

func (b *RedisStorageBackend[K, V]) fetchKeysWithPrefix(ctx context.Context, prefix string, batchSize int) (map[K]V, error) {
	var cursor uint64
	var err error
	keys := make(map[K]V)
	resultsChan := make(chan map[K]V)
	var wg sync.WaitGroup

	for {
		var scanKeys []string
		scanKeys, cursor, err = b.Client.Scan(ctx, cursor, fmt.Sprintf("%s:*", prefix), 0).Result()
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

	for keyMap := range resultsChan {
		for k, v := range keyMap {
			keys[k] = v
		}
	}

	return keys, nil
}
