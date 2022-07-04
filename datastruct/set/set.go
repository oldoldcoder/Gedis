package set

import "gedis/datastruct/dict"

// 写完几种最基本的结构之后进行填充操作处理

// Set 哈希表的变种，内置结构为dict
type Set struct {
	dict dict.Dict
}

func Make(members ...string) *Set {
	set := &Set{
		// 上层有concurrent保证线程安全，使用simple获取简单dict
		dict: dict.MakeSimple(),
	}
	for _, member := range members {
		set.Add(member)
	}
	return set
}

func (set *Set) Add(val string) int {
	return set.dict.Put(val, nil)
}

func (set *Set) Remove(val string) int {
	return set.dict.Remove(val)
}

func (set *Set) Has(val string) bool {
	_, exists := set.dict.Get(val)
	return exists
}

func (set *Set) Len() int {
	return set.dict.Len()
}

func (set *Set) isNil() {
	if set == nil {
		panic("set is nil")
	}
}

// ToSlice convert set to []string
func (set *Set) ToSlice() []string {
	slice := make([]string, set.Len())
	i := 0
	set.dict.ForEach(func(key string, val interface{}) bool {
		if i < len(slice) {
			slice[i] = key
		} else {
			// set extended during traversal
			slice = append(slice, key)
		}
		i++
		return true
	})

	return slice
}

func (set *Set) ForEach(consumer func(member string) bool) {
	set.isNil()
	set.dict.ForEach(func(key string, val interface{}) bool {
		return consumer(key)
	})
}

// Intersect 返回两个集合的交际
func (set *Set) Intersect(another *Set) *Set {
	set.isNil()
	result := Make()
	another.ForEach(func(member string) bool {
		if set.Has(member) {
			result.Add(member)
		}
		return true
	})
	return result
}

func (set *Set) Union(another *Set) *Set {
	set.isNil()
	result := Make()
	another.ForEach(func(member string) bool {
		result.Add(member)
		return true
	})
	set.ForEach(func(member string) bool {
		result.Add(member)
		return true
	})

	return result
}

// Diff 返回与一个集合的差集
func (set *Set) Diff(another *Set) *Set {
	if set == nil {
		panic("set is nil")
	}

	result := Make()
	set.ForEach(func(member string) bool {
		if !another.Has(member) {
			result.Add(member)
		}
		return true
	})
	return result
}

// RandomMembers 返回随机的键值，会有可能复
func (set *Set) RandomMembers(limit int) []string {
	return set.dict.RandomKeys(limit)
}

// RandomDistinctMembers 返回随机的键值，不会重复
func (set *Set) RandomDistinctMembers(limit int) []string {
	return set.dict.RandomDistinctKeys(limit)
}
