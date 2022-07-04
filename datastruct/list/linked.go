package list

import (
	"gedis/lib/utils"
)

/*
list在redis中的方法：
1.获取长度
2.头插入||未插入
3.某元素前插入||某元素后插入
4.删除某一元素
5.删除区间元素||删除头元素||删除尾元素
6.加入值
7.获取值
8.根据rank获取范围值
info:这里List是不需要加锁的，因为所有的list作为dataEntity被存储在dict中，当需要修改的时候dict会自动加锁
*/

// LinkedList 双端链表
type LinkedList struct {
	first *node
	last  *node
	size  int
}

// 双端链表的一个节点
type node struct {
	val  interface{}
	prev *node
	next *node
}

func (l *LinkedList) isNil() {
	if l == nil {
		panic("LinkedList is nil")
	}
}

func (l *LinkedList) Add(val interface{}) {
	l.isNil()
	// 其他量自动赋值nil
	n := &node{
		val: val,
	}
	if l.last == nil {
		l.first = n
		l.last = n
	} else {
		n.prev = l.last
		l.last.next = n
		l.last = n
	}
	l.size++
}

// 通过下标查找
func (l *LinkedList) find(index int) (n *node) {
	// 中间差分加快查找，list不能randomAccess
	if index < l.size/2 {
		n = l.first
		for i := 0; i < index; i++ {
			n = n.next
		}
	} else {
		n = l.last
		for i := l.size - 1; i > index; i-- {
			n = n.prev
		}
	}
	return n
}

func (l *LinkedList) Get(index int) (val interface{}) {
	l.isNil()
	if index < 0 || index >= l.size {
		panic("index out of bound")
	}
	return l.find(index)
}

func (l *LinkedList) Set(index int, val interface{}) {
	l.isNil()
	if index < 0 || index >= l.size {
		panic("index out of bound")
	}
	n := l.find(index)
	n.val = val
}

// Insert 插入到index结点之前
func (l *LinkedList) Insert(index int, val interface{}) {
	l.isNil()
	if index < 0 || index > l.size {
		panic("index out of bound")
	}

	// 给末尾添加元素
	if index == l.size {
		l.Add(val)
		return
	}

	pivot := l.find(index)
	n := &node{
		val:  val,
		prev: pivot.prev,
		next: pivot,
	}
	if pivot.prev == nil {
		l.first = n

	} else {
		pivot.prev.next = n
	}
	pivot.prev = n
	l.size++
}

func (l *LinkedList) removeNode(n *node) {
	// n是链表首部
	if n.prev == nil {
		l.first = n.next
	} else {
		n.prev.next = n.next
	}
	// n是末尾
	if n.next == nil {
		l.last = n.prev
	} else {
		n.next.prev = n.prev
	}

	// 与外界联系切除才能进行gc
	n.prev = nil
	n.next = nil

	l.size--
}

func (l *LinkedList) Remove(index int) (val interface{}) {
	l.isNil()
	if index < 0 || index > l.size {
		panic("index out of bound")
	}
	n := l.find(index)

	l.removeNode(n)
	return n.val
}

// RemoveLast 删除最后一个节点
func (l *LinkedList) RemoveLast() (val interface{}) {
	if l == nil {
		panic("list is nil")
	}
	if l.last == nil {
		// empty list
		return nil
	}
	n := l.last
	l.removeNode(n)
	return n.val
}

// RemoveAllByVal 删除一个值的所有元素
func (l *LinkedList) RemoveAllByVal(val interface{}) int {
	l.isNil()
	n := l.first
	removed := 0
	var nextNode *node
	for n != nil {
		nextNode = n.next
		if utils.Equals(n.val, val) {
			l.removeNode(n)
			removed++
		}
		n = nextNode
	}
	return removed
}

func (l *LinkedList) RemoveByVal(val interface{}, count int) int {
	l.isNil()
	n := l.first
	removed := 0
	// 不能使用删除指针，防止野指针的使用
	var nextNode *node
	for n != nil {
		nextNode = n.next
		if utils.Equals(n.val, val) {
			l.removeNode(n)
			removed++
		}
		if removed == count {
			break
		}
		n = nextNode
	}
	// removed可能会删除不够
	return removed
}

func (list *LinkedList) ReverseRemoveByVal(val interface{}, count int) int {
	if list == nil {
		panic("list is nil")
	}
	n := list.last
	removed := 0
	var prevNode *node
	for n != nil {
		prevNode = n.prev
		if utils.Equals(n.val, val) {
			list.removeNode(n)
			removed++
		}
		if removed == count {
			break
		}
		n = prevNode
	}
	return removed
}

func (l *LinkedList) Len() int {
	l.isNil()
	return l.size
}

func (l *LinkedList) ForEach(consumer func(int, interface{}) bool) {
	l.isNil()
	n := l.first
	for i := 0; n != nil; i++ {
		ok := consumer(i, n)
		if !ok {
			break
		}
		n = n.next
	}
}

func (l *LinkedList) Contains(val interface{}) bool {
	contains := false
	consumer := func(i int, val2 interface{}) bool {
		contains = utils.Equals(val, val2)
		return !contains
	}
	l.ForEach(consumer)
	return contains
}

// Range 返回start和stop直接的元素
func (l *LinkedList) Range(start int, stop int) []interface{} {
	l.isNil()
	if start < 0 || start >= l.size {
		panic("`start` out of range")
	} else if stop < start || stop >= l.size {
		panic("`stop` out of range")
	}
	sliceResult := make([]interface{}, stop-start)

	n := l.first
	sliceIndex := 0
	for i := 0; n != nil; i++ {
		if i >= start && i < stop {
			sliceResult[sliceIndex] = n.val
			sliceIndex++
		} else if i >= stop {
			break
		}
		n = n.next
	}
	return sliceResult
}

// Make 返回一个链表带有传递进来的元素
func Make(vals ...interface{}) *LinkedList {
	list := LinkedList{}
	for _, v := range vals {
		list.Add(v)
	}
	return &list
}
