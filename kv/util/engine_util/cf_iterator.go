package engine_util

import (
	"github.com/Connor1996/badger"
)

type CFItem struct {
	item      *badger.Item
	prefixLen int
}

// String 返回 Item 的字符串表示
func (i *CFItem) String() string {
	return i.item.String()
}

func (i *CFItem) Key() []byte {
	return i.item.Key()[i.prefixLen:]
}

func (i *CFItem) KeyCopy(dst []byte) []byte {
	return i.item.KeyCopy(dst)[i.prefixLen:]
}

func (i *CFItem) Version() uint64 {
	return i.item.Version()
}

func (i *CFItem) IsEmpty() bool {
	return i.item.IsEmpty()
}

func (i *CFItem) Value() ([]byte, error) {
	return i.item.Value()
}

func (i *CFItem) ValueSize() int {
	return i.item.ValueSize()
}

func (i *CFItem) ValueCopy(dst []byte) ([]byte, error) {
	return i.item.ValueCopy(dst)
}

func (i *CFItem) IsDeleted() bool {
	return i.item.IsDeleted()
}

func (i *CFItem) EstimatedSize() int64 {
	return i.item.EstimatedSize()
}

func (i *CFItem) UserMeta() []byte {
	return i.item.UserMeta()
}

type BadgerIterator struct {
	iter   *badger.Iterator
	prefix string
}

func NewCFIterator(cf string, txn *badger.Txn) *BadgerIterator {
	return &BadgerIterator{
		iter:   txn.NewIterator(badger.DefaultIteratorOptions),
		prefix: cf + "_",
	}
}

func (it *BadgerIterator) Item() DBItem {
	return &CFItem{
		item:      it.iter.Item(),
		prefixLen: len(it.prefix),
	}
}

func (it *BadgerIterator) Valid() bool { return it.iter.ValidForPrefix([]byte(it.prefix)) }

func (it *BadgerIterator) ValidForPrefix(prefix []byte) bool {
	return it.iter.ValidForPrefix(append([]byte(it.prefix), prefix...))
}

func (it *BadgerIterator) Close() {
	it.iter.Close()
}

func (it *BadgerIterator) Next() {
	it.iter.Next()
}

func (it *BadgerIterator) Seek(key []byte) {
	it.iter.Seek(append([]byte(it.prefix), key...))
}

func (it *BadgerIterator) Rewind() {
	it.iter.Rewind()
}

type DBIterator interface {
	// Item 返回指向当前键值对的指针。
	Item() DBItem
	// Valid 在迭代结束时返回 false。
	Valid() bool
	// Next 将迭代器向前推进一步。每次调用 Next() 后务必检查 it.Valid()，
	// 以确保可以访问有效的 it.Item()。
	Next()
	// Seek 定位到指定的 key。如果该 key 不存在，则定位到大于该 key 的最小 key。
	Seek([]byte)

	// Close 关闭迭代器。
	Close()
}

type DBItem interface {
	// Key 返回键。
	Key() []byte
	// KeyCopy 返回 item 的键的副本，写入到 dst 切片中。
	// 如果传入 nil，或 dst 容量不足，则会分配并返回一个新的切片。
	KeyCopy(dst []byte) []byte
	// Value 获取 item 的值。
	Value() ([]byte, error)
	// ValueSize 返回值的大小。
	ValueSize() int
	// ValueCopy 从 value log 中返回 item 值的副本，写入到 dst 切片中。
	// 如果传入 nil，或 dst 容量不足，则会分配并返回一个新的切片。
	ValueCopy(dst []byte) ([]byte, error)
}
