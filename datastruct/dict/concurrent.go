package dict

import (
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
)

// ConcurrentDict 是一个线程安全map，使用了卡槽作为基本项，减少rehash
type ConcurrentDict struct {
	// 一个dict所占有的卡槽
	table      []*shard
	count      int32
	shardCount int
}

type shard struct {
	m map[string]interface{}
	// 一个卡槽共享一个锁
	mutex sync.RWMutex
}

func computeCapacity(param int) (size int) {
	if param <= 16 {
		return 16
	}
	n := param - 1
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	if n < 0 {
		return math.MaxInt32
	}
	return n + 1
}

func MakeConcurrent(shardCount int) *ConcurrentDict {
	shardCount = computeCapacity(shardCount)
	table := make([]*shard, shardCount)
	for i := 0; i < shardCount; i++ {
		table[i] = &shard{
			m: make(map[string]interface{}),
		}
	}
	d := &ConcurrentDict{
		count:      0,
		table:      table,
		shardCount: shardCount,
	}
	return d
}

// hash算法以及相关加锁操作

const prime32 = uint32(16777619)

// fnv32 可以很好的将一些相差不大的例如时间等数据均匀的散列到一个范围内，关于prime素数的值可以查看相应位数下的取值
func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		// 进行乘运算
		hash *= prime32
		// 进行异或运算
		hash ^= uint32(key[i])
	}
	return hash
}

func (dict *ConcurrentDict) spread(hashCode uint32) uint32 {
	dict.isNil()
	tableSize := uint32(len(dict.table))
	// 也可以使用除法求余数，个人认为
	return (tableSize - 1) & uint32(hashCode)
}

func (dict *ConcurrentDict) getShard(index uint32) *shard {
	dict.isNil()
	return dict.table[index]
}

func (dict *ConcurrentDict) Len() int {
	dict.isNil()
	return int(atomic.LoadInt32(&dict.count))
}

func (dict *ConcurrentDict) Get(key string) (val interface{}, exists bool) {
	dict.isNil()
	// 这里的hash都是为了将key均匀的放入到shard中去
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	sh := dict.getShard(index)
	sh.mutex.RLock()
	defer sh.mutex.RUnlock()
	// 返回值的赋值可以直接用来使用
	val, exists = sh.m[key]
	return
}

func (dict *ConcurrentDict) Put(key string, val interface{}) (result int) {
	dict.isNil()
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	sh := dict.getShard(index)
	// 加锁
	sh.mutex.Lock()
	defer sh.mutex.Unlock()
	if _, ok := sh.m[key]; ok {
		// 意味着重新更替值，同时dict的count不需要给出了
		sh.m[key] = val
		return 0
	}
	sh.m[key] = val
	dict.addCount()
	return 1

}

// PutIfAbsent 当key不存在的时候更新
func (dict *ConcurrentDict) PutIfAbsent(key string, val interface{}) (result int) {
	dict.isNil()
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	shard := dict.getShard(index)
	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	if _, ok := shard.m[key]; ok {
		return 0
	}
	shard.m[key] = val
	dict.addCount()
	return 1
}

// PutIfExists 当key的时候更新
func (dict *ConcurrentDict) PutIfExists(key string, val interface{}) (result int) {
	dict.isNil()
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	shard := dict.getShard(index)
	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	if _, ok := shard.m[key]; ok {
		shard.m[key] = val
		return 1
	}
	return 0
}

// Remove remove负责删除一个键，当键存在返回1，不存在返回0
func (dict *ConcurrentDict) Remove(key string) (result int) {
	dict.isNil()
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	shard := dict.getShard(index)
	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	if _, ok := shard.m[key]; ok {
		delete(shard.m, key)
		dict.decreaseCount()
		return 1
	}
	return 0
}

// ForEach 遍历整个安全dict
func (dict *ConcurrentDict) ForEach(consumer Consumer) {
	dict.isNil()

	// for第一次遍历
	for _, shard := range dict.table {
		shard.mutex.RLock()
		func() {
			defer shard.mutex.RUnlock()
			for key, value := range shard.m {
				continues := consumer(key, value)
				if !continues {
					return
				}
			}
		}()
	}
}

// Keys returns all keys in dict
func (dict *ConcurrentDict) Keys() []string {
	keys := make([]string, dict.Len())
	i := 0
	dict.ForEach(func(key string, val interface{}) bool {
		// 防止某些原因数组越界
		if i < len(keys) {
			keys[i] = key
			i++
		} else {
			keys = append(keys, key)
		}
		return true
	})
	return keys
}

// RandomKey returns a key randomly
func (shard *shard) RandomKey() string {
	if shard == nil {
		panic("shard is nil")
	}
	shard.mutex.RLock()
	defer shard.mutex.RUnlock()

	for key := range shard.m {
		return key
	}
	return ""
}

// RandomKeys randomly returns keys of the given number, may contain duplicated key
func (dict *ConcurrentDict) RandomKeys(limit int) []string {
	size := dict.Len()
	if limit >= size {
		return dict.Keys()
	}
	shardCount := len(dict.table)

	result := make([]string, limit)
	for i := 0; i < limit; {
		sh := dict.getShard(uint32(rand.Intn(shardCount)))

		key := sh.RandomKey()
		// RandomKey返回空，当有些卡槽内部没有keys的时候
		if key != "" {
			result[i] = key
			i++
		}
	}
	return result
}

// RandomDistinctKeys randomly returns keys of the given number, won't contain duplicated key
func (dict *ConcurrentDict) RandomDistinctKeys(limit int) []string {
	size := dict.Len()
	if limit >= size {
		return dict.Keys()
	}

	shardCount := len(dict.table)
	result := make(map[string]bool)
	for len(result) < limit {
		shardIndex := uint32(rand.Intn(shardCount))
		shard := dict.getShard(shardIndex)
		if shard == nil {
			continue
		}
		key := shard.RandomKey()
		if key != "" {
			result[key] = true
		}
	}
	arr := make([]string, limit)
	i := 0
	for k := range result {
		arr[i] = k
		i++
	}
	return arr
}

// Clear 清空dict，重新赋值，不需要考虑gc
func (dict *ConcurrentDict) Clear() {
	*dict = *MakeConcurrent(dict.shardCount)
}

/*-----dict的工具函数------*/

func (dict *ConcurrentDict) isNil() {
	if dict == nil {
		panic("dict is nil")
	}
}

func (dict *ConcurrentDict) addCount() int32 {
	return atomic.AddInt32(&dict.count, 1)
}

func (dict *ConcurrentDict) decreaseCount() int32 {
	return atomic.AddInt32(&dict.count, -1)
}
