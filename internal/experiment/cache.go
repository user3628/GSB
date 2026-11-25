package experiment

import (
	"sync"

	"regworkbench/internal/canonical"
	"regworkbench/internal/cv"
)

type ResponseCache struct {
	mu sync.RWMutex
	m  map[string][]byte
}

func NewResponseCache() *ResponseCache {
	return &ResponseCache{m: map[string][]byte{}}
}

func (c *ResponseCache) Get(key cv.CacheKey) ([]byte, bool) {
	id := key.String()
	c.mu.RLock()
	b, ok := c.m[id]
	c.mu.RUnlock()
	if !ok {
		return nil, false
	}
	return append([]byte(nil), b...), true
}

func (c *ResponseCache) Put(key cv.CacheKey, v any) ([]byte, error) {
	b, err := canonical.JSON(v)
	if err != nil {
		return nil, err
	}
	id := key.String()
	c.mu.Lock()
	c.m[id] = append([]byte(nil), b...)
	c.mu.Unlock()
	return b, nil
}

func (c *ResponseCache) GetOrMarshal(key cv.CacheKey, v any) ([]byte, bool, error) {
	if b, ok := c.Get(key); ok {
		return b, true, nil
	}
	b, err := c.Put(key, v)
	if err != nil {
		return nil, false, err
	}
	return b, false, nil
}

func (c *ResponseCache) DeleteID(id string) {
	c.mu.Lock()
	delete(c.m, id)
	c.mu.Unlock()
}
