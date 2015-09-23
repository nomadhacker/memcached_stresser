package main

import (
	"hash/crc32"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
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
	return string(sm.getShard(key).Get(key).Value)
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
	return &memcache.Item{mcitem: &memcache.Item{Key: key, Value: []byte(value)}}
}

func (mi *MemcachedItem) SetKey(key string) error {
	mi.mcitem.Key = key
	return nil
}

func (mi *MemcachedItem) SetValue(value string) error {
	mi.mcitem.Value = []byte(value)
	return nil
}

func (mi *MemcachedItem) GetKey(key string) error {
	return mi.mcitem.Key
}

func (mi *MemcachedItem) GetValue(value string) error {
	return string(mi.mcitem.Value)
}

type RedisItem struct {
	Key   string
	Value string
}

func NewRedisItem(key, value string) *RedisItem {
	return &RedisItem{Key: key, Value: value}
}

func (ri *RedisItem) SetKey(key string) error {
	ri.Key = key
	return nil
}

func (ri *RedisItem) SetValue(value string) error {
	ri.Value = value
	return nil
}

func (ri *RedisItem) GetKey(key string) error {
	return ri.Key
}

func (ri *RedisItem) GetValue(value string) error {
	return ri.Value
}
