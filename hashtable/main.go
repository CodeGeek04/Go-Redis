// File: hashtable.go

package hashtable

import (
	"fmt"
	"sync"
	"time"
)

const defaultSize = 16

type node struct {
	key   string
	value interface{}
	next  *node
}

type HashTable struct {
	buckets []*node
	size    int
	mu      sync.RWMutex
}

func New() *HashTable {
	return &HashTable{
		buckets: make([]*node, defaultSize),
		size:    0,
	}
}

func (ht *HashTable) hash(key string) int {
	hash := 0
	for i := 0; i < len(key); i++ {
		hash = 31*hash + int(key[i])
	}
	return hash % len(ht.buckets)
}

// Set adds a key-value pair to the hash table
func (ht *HashTable) Set(key string, value interface{}) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	ht.set(key, value)
}

// SetWithTime adds a key-value pair and returns the time taken
func (ht *HashTable) SetWithTime(key string, value interface{}) time.Duration {
	startTime := time.Now()
	ht.Set(key, value)
	return time.Since(startTime)
}

func (ht *HashTable) set(key string, value interface{}) {
	index := ht.hash(key)
	newNode := &node{key: key, value: value}

	if ht.buckets[index] == nil {
		ht.buckets[index] = newNode
	} else {
		current := ht.buckets[index]
		for current.next != nil {
			if current.key == key {
				current.value = value
				return
			}
			current = current.next
		}
		if current.key == key {
			current.value = value
		} else {
			current.next = newNode
		}
	}
	ht.size++
}

// Get retrieves a value from the hash table
func (ht *HashTable) Get(key string) (interface{}, bool) {
	ht.mu.RLock()
	defer ht.mu.RUnlock()

	return ht.get(key)
}

// GetWithTime retrieves a value and returns the time taken
func (ht *HashTable) GetWithTime(key string) (interface{}, bool, time.Duration) {
	startTime := time.Now()
	value, found := ht.Get(key)
	return value, found, time.Since(startTime)
}

func (ht *HashTable) get(key string) (interface{}, bool) {
	index := ht.hash(key)
	current := ht.buckets[index]

	for current != nil {
		if current.key == key {
			return current.value, true
		}
		current = current.next
	}
	return nil, false
}

// Del removes a key-value pair from the hash table
func (ht *HashTable) Del(key string) bool {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	return ht.del(key)
}

// DelWithTime removes a key-value pair and returns the time taken
func (ht *HashTable) DelWithTime(key string) (bool, time.Duration) {
	startTime := time.Now()
	deleted := ht.Del(key)
	return deleted, time.Since(startTime)
}

func (ht *HashTable) del(key string) bool {
	index := ht.hash(key)
	current := ht.buckets[index]
	var prev *node

	for current != nil {
		if current.key == key {
			if prev == nil {
				ht.buckets[index] = current.next
			} else {
				prev.next = current.next
			}
			ht.size--
			return true
		}
		prev = current
		current = current.next
	}
	return false
}

func (ht *HashTable) ListAll() []string {
	ht.mu.RLock()
	defer ht.mu.RUnlock()

	var result []string
	for _, bucket := range ht.buckets {
		current := bucket
		for current != nil {
			result = append(result, fmt.Sprintf("%s: %v", current.key, current.value))
			current = current.next
		}
	}
	return result
}