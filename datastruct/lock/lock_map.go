package lock

import (
	"sort"
	"sync"
)

const (
	prime32 = uint32(16777619)
)

/*
部分命令对于单个key的锁定是不行的，必须将一系列命令封装为一个原子操作
例如Incr和MSETNX
因此需要实现一个DB.locker用于锁定一个或者一组key，并且在需要的时候释放
*/

// Locks 为db含有，并且table内部的锁不会释放，为每一个槽设置一个锁
type Locks struct {
	table []*sync.RWMutex
}

func Make(tableSize int) *Locks {
	// tableSize 默认为1024，默认同一时间需要加锁的槽上限不超过1024
	table := make([]*sync.RWMutex, tableSize)
	// 初始化锁的数量
	for i := 0; i < tableSize; i++ {
		table[i] = &sync.RWMutex{}
	}
	return &Locks{
		table: table,
	}
}

/*---以下代码和ConcurrentDict中代码相同-----*/
func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

func (locks *Locks) spread(hashCode uint32) uint32 {
	if locks == nil {
		panic("dict is nil")
	}
	tableSize := uint32(len(locks.table))
	return (tableSize - 1) & uint32(hashCode)
}

// Lock obtains exclusive lock for writing
func (locks *Locks) Lock(key string) {
	// 获得槽的坐标，为槽加锁
	index := locks.spread(fnv32(key))
	mu := locks.table[index]
	mu.Lock()
}

// RLock obtains shared lock for reading
func (locks *Locks) RLock(key string) {
	index := locks.spread(fnv32(key))
	mu := locks.table[index]
	mu.RLock()
}

// UnLock release exclusive lock
func (locks *Locks) UnLock(key string) {
	index := locks.spread(fnv32(key))
	mu := locks.table[index]
	mu.Unlock()
}

// RUnLock release shared lock
func (locks *Locks) RUnLock(key string) {
	index := locks.spread(fnv32(key))
	mu := locks.table[index]
	mu.RUnlock()
}

// toLockIndices 同时为多个键值加锁，为了防止形成死锁，提供了加锁的顺序
func (locks *Locks) toLockIndices(key []string, reverse bool) []uint32 {
	// 散列之后的index仍然有可能重复，使用indexMap而不是indexSlice来保存index值
	indexMap := make(map[uint32]struct{})
	for _, k := range key {
		index := locks.spread(fnv32(k))
		indexMap[index] = struct{}{}
	}

	indices := make([]uint32, 0, len(indexMap))
	for index := range indexMap {
		indices = append(indices, index)
	}
	sort.Slice(indices, func(i, j int) bool {
		if !reverse {
			return indices[i] < indices[j]
		}
		return indices[i] > indices[j]
	})
	return indices
}

func (locks *Locks) Locks(keys ...string) {
	indices := locks.toLockIndices(keys, false)
	for _, index := range indices {
		mu := locks.table[index]
		mu.Lock()
	}
}

func (locks *Locks) RLocks(keys ...string) {
	indices := locks.toLockIndices(keys, false)
	for _, index := range indices {
		mu := locks.table[index]
		mu.RLock()
	}
}

func (locks *Locks) UnLocks(keys ...string) {
	indices := locks.toLockIndices(keys, true)
	for _, index := range indices {
		mu := locks.table[index]
		mu.Unlock()
	}
}

func (locks *Locks) RUnLocks(keys ...string) {
	indices := locks.toLockIndices(keys, true)
	for _, index := range indices {
		mu := locks.table[index]
		mu.RUnlock()
	}
}

/*---锁的着重使用函数---*/
func (locks *Locks) RWLocks(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	indices := locks.toLockIndices(keys, false)
	writeIndexSet := make(map[uint32]struct{})
	for _, k := range writeKeys {
		index := locks.spread(fnv32(k))
		writeIndexSet[index] = struct{}{}
	}
	for _, index := range indices {
		// w is true 代表可以取出来
		_, e := writeIndexSet[index]
		mu := locks.table[index]
		if e {
			mu.Lock()
		} else {
			mu.RLock()
		}
	}
}

func (locks *Locks) RWUnLocks(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	indices := locks.toLockIndices(keys, true)
	writeIndexSet := make(map[uint32]struct{})
	for _, k := range writeKeys {
		index := locks.spread(fnv32(k))
		writeIndexSet[index] = struct{}{}
	}
	for _, index := range indices {
		_, w := writeIndexSet[index]
		mu := locks.table[index]
		if w {
			mu.Unlock()
		} else {
			mu.RUnlock()
		}
	}
}
