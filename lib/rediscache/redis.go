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
	c redis.UniversalClient
}

func NewRedisClient() *RedisRollupResultCacheClient {
	return &RedisRollupResultCacheClient{
		c: redis.NewUniversalClient(&redis.UniversalOptions{
			Addrs: []string{"127.0.0.1:6379"},
		}),
	}
}

func (rc *RedisRollupResultCacheClient) Get(dst, key []byte) []byte {
	var err error
	dst, err = rc.c.GetEx(context.TODO(), string(key), time.Minute).Bytes()
	if err != nil && !errors.Is(err, redis.Nil) {
		logger.Errorf("get rollup result cache from redis failed: %v", err)
	}
	return dst
}

func (rc *RedisRollupResultCacheClient) Set(key, value []byte) {
	if err := rc.c.Set(context.TODO(), string(key), value, time.Minute).Err(); err != nil {
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
