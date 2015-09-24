package main

import (
	"errors"
	"hash/crc32"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"gopkg.in/redis.v3"
)

type KeyValueStore interface {
	Get(string) (string, error)
	Set(string, string) error
	Flush() error
}

type ShardedMemcached struct {
	servers []*memcache.Client
}

func NewShardedMemcachedKVS(shards []string) KeyValueStore {
	mc := &ShardedMemcached{}
	for _, each := range shards {
		newClient := memcache.New(each)
		newClient.Timeout = time.Second * 5
		mc.Add(newClient)
	}
	return mc
}

func (sm *ShardedMemcached) Add(c *memcache.Client) {
	sm.servers = append(sm.servers, c)
}

func (sm *ShardedMemcached) getShard(key string) *memcache.Client {
	return sm.servers[crc32.ChecksumIEEE([]byte(key))%uint32(len(sm.servers))]
}

func (sm *ShardedMemcached) Set(key, value string) error {
	i := NewMemcachedItem(key, value)
	return sm.getShard(i.mcitem.Key).Set(i.mcitem)
}

func (sm *ShardedMemcached) Get(key string) (string, error) {
	var stringResult string
	var err error
	defer func() {
		if r := recover(); r != nil {
			stringResult = ""
			err = errors.New("Connection dereference")
		}
	}()
	result, err := sm.getShard(key).Get(key)
	stringResult = string(result.Value)
	return stringResult, err
}

func (sm *ShardedMemcached) Flush() error {
	for _, each := range sm.servers {
		err := each.FlushAll()
		if err != nil {
			return err
		}
	}
	return nil
}

type RedisKVS struct {
	cl *redis.Client
}

func (r *RedisKVS) Set(key, value string) error {
	return r.cl.Set(key, value, 0).Err()
}

func (r *RedisKVS) Get(key string) (string, error) {
	return r.cl.Get(key).Result()
}

func (r *RedisKVS) Flush() error {
	return r.cl.FlushAll().Err()
}

type ShardedRedisKVS struct {
	servers []*RedisKVS
}

func NewShardedRedisKVS(shards []string) KeyValueStore {
	rc := &ShardedRedisKVS{}
	for _, each := range shards {
		newClient := redis.NewClient(&redis.Options{
			Addr:     each,
			Password: "", // no password set
			DB:       0,  // use default DB
		})
		rc.Add(newClient)
	}
	return rc
}

func (sr *ShardedRedisKVS) Add(c *redis.Client) {
	rc := &RedisKVS{cl: c}
	sr.servers = append(sr.servers, rc)
}

func (sr *ShardedRedisKVS) getShard(key string) *RedisKVS {
	return sr.servers[crc32.ChecksumIEEE([]byte(key))%uint32(len(sr.servers))]
}

func (sr *ShardedRedisKVS) Set(key, value string) error {
	return sr.getShard(key).Set(key, value)
}

func (sr *ShardedRedisKVS) Get(key string) (string, error) {
	return sr.getShard(key).Get(key)
}

func (sr *ShardedRedisKVS) Flush() error {
	for _, each := range sr.servers {
		err := each.Flush()
		if err != nil {
			return err
		}
	}
	return nil
}

type KVItem interface {
	SetKey(string) error
	SetValue(string) error
	GetKey() string
	GetValue() string
}

type MemcachedItem struct {
	mcitem *memcache.Item
}

func NewMemcachedItem(key, value string) *MemcachedItem {
	return &MemcachedItem{mcitem: &memcache.Item{Key: key, Value: []byte(value)}}
}

func (mi *MemcachedItem) SetKey(key string) error {
	mi.mcitem.Key = key
	return nil
}

func (mi *MemcachedItem) SetValue(value string) error {
	mi.mcitem.Value = []byte(value)
	return nil
}

func (mi *MemcachedItem) GetKey() string {
	return mi.mcitem.Key
}

func (mi *MemcachedItem) GetValue() string {
	return string(mi.mcitem.Value)
}
