package mvcc

import (
	"encoding/binary"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
)

// KeyError 是一个包装类型，以便我们可以实现 `error` 接口。
type KeyError struct {
	kvrpcpb.KeyError
}

func (ke *KeyError) Error() string {
	return ke.String()
}

// MvccTxn 将写操作组合在一起作为单个事务的一部分。它还提供了对底层存储的抽象，
// 将时间戳、写操作和锁的概念降低为普通的键和值。
type MvccTxn struct {
	StartTS uint64
	Reader  storage.StorageReader
	writes  []storage.Modify
}

func NewMvccTxn(reader storage.StorageReader, startTs uint64) *MvccTxn {
	return &MvccTxn{
		Reader:  reader,
		StartTS: startTs,
	}
}

// Writes 返回添加到此事务的所有更改。
func (txn *MvccTxn) Writes() []storage.Modify {
	return txn.writes
}

// PutWrite 在 key 和 ts 处记录一个写操作。
func (txn *MvccTxn) PutWrite(key []byte, ts uint64, write *Write) {
	// 你的代码在这里 (4A)。
}

// GetLock 如果 key 被锁定则返回一个锁。如果 key 上没有锁则返回 (nil, nil)，
// 如果查找过程中发生错误则返回 (nil, err)。
func (txn *MvccTxn) GetLock(key []byte) (*Lock, error) {
	// 你的代码在这里 (4A)。
	return nil, nil
}

// PutLock 向此事务添加一个 key/lock 对。
func (txn *MvccTxn) PutLock(key []byte, lock *Lock) {
	// 你的代码在这里 (4A)。
}

// DeleteLock 向此事务添加一个删除锁操作。
func (txn *MvccTxn) DeleteLock(key []byte) {
	// 你的代码在这里 (4A)。
}

// GetValue 查找 key 的值，该值在此事务的开始时间戳处有效。
// 即在此事务开始之前提交的最新值。
func (txn *MvccTxn) GetValue(key []byte) ([]byte, error) {
	// 你的代码在这里 (4A)。
	return nil, nil
}

// PutValue 向此事务添加一个 key/value 写操作。
func (txn *MvccTxn) PutValue(key []byte, value []byte) {
	// 你的代码在这里 (4A)。
}

// DeleteValue 在此事务中删除一个 key/value 对。
func (txn *MvccTxn) DeleteValue(key []byte) {
	// 你的代码在这里 (4A)。
}

// CurrentWrite 搜索具有此事务开始时间戳的写操作。它返回数据库中的 Write 和该
// 写操作的提交时间戳，或者返回错误。
func (txn *MvccTxn) CurrentWrite(key []byte) (*Write, uint64, error) {
	// 你的代码在这里 (4A)。
	return nil, 0, nil
}

// MostRecentWrite 查找给定 key 的最新写操作。它返回数据库中的 Write 和该
// 写操作的提交时间戳，或者返回错误。
func (txn *MvccTxn) MostRecentWrite(key []byte) (*Write, uint64, error) {
	// 你的代码在这里 (4A)。
	return nil, 0, nil
}

// EncodeKey 编码用户 key 并将编码后的时间戳追加到 key。key 和时间戳的编码方式使得
// 带时间戳的 key 首先按 key 排序（升序），然后按时间戳排序（降序）。编码基于
// https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format。
func EncodeKey(key []byte, ts uint64) []byte {
	encodedKey := codec.EncodeBytes(key)
	newKey := append(encodedKey, make([]byte, 8)...)
	binary.BigEndian.PutUint64(newKey[len(encodedKey):], ^ts)
	return newKey
}

// DecodeUserKey 接受一个 key + 时间戳并返回 key 部分。
func DecodeUserKey(key []byte) []byte {
	_, userKey, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return userKey
}

// decodeTimestamp 接受一个 key + 时间戳并返回时间戳部分。
func decodeTimestamp(key []byte) uint64 {
	left, _, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return ^binary.BigEndian.Uint64(left)
}

// PhysicalTime 返回时间戳的物理时间部分。
func PhysicalTime(ts uint64) uint64 {
	return ts >> tsoutil.PhysicalShiftBits
}
