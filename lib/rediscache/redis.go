package rediscache

import (
	"context"
	"errors"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/fastcache"
	"github.com/redis/go-redis/v9"
)

type RedisRollupResultCacheClient struct {
	c   redis.UniversalClient
	ttl time.Duration

	// stats
	calls  uint64
	misses uint64
}

func NewRedisClient(redisAddr string, ttl time.Duration) *RedisRollupResultCacheClient {
	return &RedisRollupResultCacheClient{
		c: redis.NewUniversalClient(&redis.UniversalOptions{
			Addrs: []string{redisAddr},
		}),
		ttl:    ttl,
		calls:  0,
		misses: 0,
	}
}

func (rc *RedisRollupResultCacheClient) Get(dst, key []byte) []byte {
	rc.calls++

	var err error
	dst, err = rc.c.GetEx(context.TODO(), string(key), rc.ttl).Bytes()
	if errors.Is(err, redis.Nil) {
		rc.misses++
	} else if err != nil {
		logger.Errorf("get rollup result cache from redis failed: %v", err)
	}

	return dst
}

func (rc *RedisRollupResultCacheClient) Set(key, value []byte) {
	if err := rc.c.Set(context.TODO(), string(key), value, rc.ttl).Err(); err != nil {
		logger.Errorf("set rollup result cache to redis failed: %v", err)
	}
	return
}

func (rc *RedisRollupResultCacheClient) GetBig(dst, key []byte) []byte {
	return rc.Get(dst, key)
}

func (rc *RedisRollupResultCacheClient) SetBig(key, value []byte) {
	rc.Set(key, value)
}

func (rc *RedisRollupResultCacheClient) Save(filePath string) error { return nil }

func (rc *RedisRollupResultCacheClient) Stop() {}

func (rc *RedisRollupResultCacheClient) UpdateStats(fcs *fastcache.Stats) {
	return
}

func (rc *RedisRollupResultCacheClient) GetCalls() uint64  { return rc.calls }
func (rc *RedisRollupResultCacheClient) GetMisses() uint64 { return rc.misses }
