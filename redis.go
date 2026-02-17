package cache

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"github.com/vmihailenco/msgpack/v5"
)

type RedisStorageBackend[K comparable, V any] struct {
	Options      *RedisStorageBackendOptions[K]
	Client       *redis.Client
	callbacks    []func(CacheEvent[K, V])
	callbacksMu  sync.RWMutex
	cancelPubSub context.CancelFunc
	pubSubWg     sync.WaitGroup
}

func (b *RedisStorageBackend[K, V]) GetStringKey(key K) string {
	if b.Options.KeyPrefix == "" {
		return b.Options.CacheKey.Marshal(key)
	} else {
		return b.Options.KeyPrefix + ":" + b.Options.CacheKey.Marshal(key)
	}
}

func (b *RedisStorageBackend[K, V]) Get(ctx context.Context, key K) (*V, error) {
	data, err := b.Client.Get(ctx, b.GetStringKey(key)).Bytes()
	if err != nil {
		return nil, err
	}

	var value V
	err = msgpack.Unmarshal(data, &value)
	if err != nil {
		return nil, err
	}

	return &value, nil
}

func (b *RedisStorageBackend[K, V]) Ttl(ctx context.Context, key K) (time.Duration, error) {
	return b.Client.TTL(ctx, b.GetStringKey(key)).Result()
}

func (b *RedisStorageBackend[K, V]) Set(ctx context.Context, key K, value V) error {
	data, err := msgpack.Marshal(value)
	if err != nil {
		return err
	}

	err = b.Client.Set(ctx, b.GetStringKey(key), data, b.Options.TTL).Err()
	if err != nil {
		return err
	}

	if b.Options.PubSub {
		err = b.PublishEvent(ctx, &CacheEvent[K, V]{
			Entry: &CacheEntry[K, V]{
				Key:   key,
				Value: &value,
			},
			Type: CacheEventSet,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (b *RedisStorageBackend[K, V]) Remove(ctx context.Context, key K) error {
	err := b.Client.Del(ctx, b.GetStringKey(key)).Err()
	if err != nil {
		return err
	}

	if b.Options.PubSub {
		err = b.PublishEvent(ctx, &CacheEvent[K, V]{
			Entry: &CacheEntry[K, V]{
				Key: key,
			},
			Type: CacheEventRemove,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (b *RedisStorageBackend[K, V]) RemovePrefix(ctx context.Context, keyPrefix string) error {
	keys, err := b.fetchKeysWithPrefix(ctx, keyPrefix)
	if err != nil {
		return err
	}

	var errs []error
	for i := 0; i < len(keys); i += 1000 {
		end := i + 1000
		if end > len(keys) {
			end = len(keys)
		}

		batchKeys := make([]string, end-i)
		for j, key := range keys[i:end] {
			batchKeys[j] = b.GetStringKey(key)
		}

		if err := b.Client.Del(ctx, batchKeys...).Err(); err != nil {
			errs = append(errs, err)
		}
	}

	if err := errors.Join(errs...); err != nil {
		return err
	}

	if b.Options.PubSub {
		err = b.PublishEvent(ctx, &CacheEvent[K, V]{
			Type:      CacheEventRemovePrefix,
			KeyPrefix: keyPrefix,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (b *RedisStorageBackend[K, V]) Contains(ctx context.Context, key K) (bool, error) {
	return b.Client.Exists(ctx, b.GetStringKey(key)).Val() == 1, nil
}

func (b *RedisStorageBackend[K, V]) Load(ctx context.Context) ([]CacheEntry[K, V], error) {
	data, err := b.fetchEntriesWithPrefix(ctx, "", 100)
	if err != nil {
		return nil, err
	}

	var entries []CacheEntry[K, V]
	for key, value := range data {
		entries = append(entries, CacheEntry[K, V]{
			Key:   key,
			Value: &value,
		})
	}

	return entries, nil
}

func (b *RedisStorageBackend[K, V]) AddCallback(callback func(CacheEvent[K, V])) {
	b.callbacksMu.Lock()
	defer b.callbacksMu.Unlock()
	b.callbacks = append(b.callbacks, callback)
}

func (b *RedisStorageBackend[K, V]) PublishEvent(ctx context.Context, event *CacheEvent[K, V]) error {
	data, err := msgpack.Marshal(event)
	if err != nil {
		return err
	}

	return b.Client.Publish(ctx, b.Options.PubSubChannelName, data).Err()
}

type RedisStorageBackendOptions[K comparable] struct {
	RedisOptions      *redis.Options
	TTL               time.Duration
	CacheKey          CacheKey[K]
	KeyPrefix         string
	PubSub            bool
	PubSubChannelName string
	ScanCount         int64
}

func (o *RedisStorageBackendOptions[K]) GetScanCount() int64 {
	if o.ScanCount <= 0 {
		return 0
	}
	return o.ScanCount
}

func NewRedisStorageBackend[K comparable, V any](options *RedisStorageBackendOptions[K]) (*RedisStorageBackend[K, V], error) {
	client := redis.NewClient(options.RedisOptions)

	if err := redisotel.InstrumentTracing(client); err != nil {
		client.Close()
		return nil, err
	}

	if err := redisotel.InstrumentMetrics(client); err != nil {
		client.Close()
		return nil, err
	}

	if options.PubSub && options.PubSubChannelName == "" {
		client.Close()
		return nil, errors.New("PubSubChannelName is required when PubSub is enabled")
	}

	b := &RedisStorageBackend[K, V]{
		Options: options,
		Client:  client,
	}

	if b.Options.PubSub {
		ctx, cancel := context.WithCancel(context.Background())
		b.cancelPubSub = cancel

		b.pubSubWg.Add(1)
		go func() {
			defer b.pubSubWg.Done()
			backoff := 100 * time.Millisecond
			maxBackoff := 10 * time.Second

			for {
				if ctx.Err() != nil {
					return
				}

				pubsub := b.Client.Subscribe(ctx, b.Options.PubSubChannelName)
				for {
					msg, err := pubsub.ReceiveMessage(ctx)
					if err != nil {
						if ctx.Err() != nil {
							pubsub.Close()
							return
						}
						log.Printf("go-cache: pubsub error, reconnecting: %s", err)
						break
					}

					backoff = 100 * time.Millisecond

					var entry CacheEvent[K, V]
					err = msgpack.Unmarshal([]byte(msg.Payload), &entry)
					if err != nil {
						log.Printf("go-cache: error unmarshalling cache event message: %s", err)
						continue
					}

					b.callbacksMu.RLock()
					for _, callback := range b.callbacks {
						callback(entry)
					}
					b.callbacksMu.RUnlock()
				}
				pubsub.Close()

				select {
				case <-time.After(backoff):
					if backoff < maxBackoff {
						backoff *= 2
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	return b, nil
}

func (b *RedisStorageBackend[K, V]) Close() error {
	if b.cancelPubSub != nil {
		b.cancelPubSub()
	}
	// Close client to unblock any TCP reads in the PubSub goroutine,
	// then wait for the goroutine to finish.
	err := b.Client.Close()
	b.pubSubWg.Wait()
	return err
}
