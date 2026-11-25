package engine

import (
	"container/list"
	"sync"
)

type CacheStats struct {
	Capacity  int   `json:"capacity"`
	Size      int   `json:"size"`
	Hits      int64 `json:"hits"`
	Misses    int64 `json:"misses"`
	Evictions int64 `json:"evictions"`
}

type cacheItem struct {
	key     string
	payload []byte
}

type queryCache struct {
	mu        sync.Mutex
	capacity  int
	items     map[string]*list.Element
	order     *list.List
	hits      int64
	misses    int64
	evictions int64
}

func newQueryCache(capacity int) *queryCache {
	return &queryCache{capacity: capacity, items: make(map[string]*list.Element), order: list.New()}
}

func (c *queryCache) get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.capacity == 0 {
		c.misses++
		return nil, false
	}
	element, ok := c.items[key]
	if !ok {
		c.misses++
		return nil, false
	}
	c.order.MoveToFront(element)
	c.hits++
	payload := element.Value.(*cacheItem).payload
	return append([]byte(nil), payload...), true
}

func (c *queryCache) put(key string, payload []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.capacity == 0 {
		return
	}
	if existing, ok := c.items[key]; ok {
		existing.Value.(*cacheItem).payload = append([]byte(nil), payload...)
		c.order.MoveToFront(existing)
		return
	}
	item := &cacheItem{key: key, payload: append([]byte(nil), payload...)}
	element := c.order.PushFront(item)
	c.items[key] = element
	for c.order.Len() > c.capacity {
		last := c.order.Back()
		if last == nil {
			break
		}
		c.order.Remove(last)
		delete(c.items, last.Value.(*cacheItem).key)
		c.evictions++
	}
}

func (c *queryCache) stats() CacheStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return CacheStats{
		Capacity:  c.capacity,
		Size:      c.order.Len(),
		Hits:      c.hits,
		Misses:    c.misses,
		Evictions: c.evictions,
	}
}
