package stats

import (
	"sync"
	"time"
)

// CacheItem представляет элемент кэша с временем истечения
type CacheItem struct {
	value      interface{}
	expiration time.Time
}

// Cache представляет простой кэш с истечением срока действия
type Cache struct {
	items      map[string]CacheItem
	mu         sync.RWMutex
	defaultTTL time.Duration
}

// NewCache создает новый экземпляр кэша
func NewCache(defaultTTL time.Duration) *Cache {
	cache := &Cache{
		items:      make(map[string]CacheItem),
		defaultTTL: defaultTTL,
	}

	// Запускаем фоновую очистку устаревших данных
	go cache.startCleaner()

	return cache
}

// Set добавляет значение в кэш
func (c *Cache) Set(key string, value interface{}) {
	c.SetWithTTL(key, value, c.defaultTTL)
}

// SetWithTTL добавляет значение в кэш с указанным TTL
func (c *Cache) SetWithTTL(key string, value interface{}, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items[key] = CacheItem{
		value:      value,
		expiration: time.Now().Add(ttl),
	}
}

// Get возвращает значение из кэша, если оно существует и не устарело
func (c *Cache) Get(key string) (interface{}, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	item, ok := c.items[key]
	if !ok {
		return nil, false
	}

	// Проверяем, не устарело ли значение
	if time.Now().After(item.expiration) {
		return nil, false
	}

	return item.value, true
}

// Delete удаляет значение из кэша
func (c *Cache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.items, key)
}

// Clear очищает весь кэш
func (c *Cache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[string]CacheItem)
}

// startCleaner запускает процесс очистки устаревших элементов кэша
func (c *Cache) startCleaner() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		c.cleanExpired()
	}
}

// cleanExpired удаляет устаревшие элементы из кэша
func (c *Cache) cleanExpired() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()

	for key, item := range c.items {
		if now.After(item.expiration) {
			delete(c.items, key)
		}
	}
}
