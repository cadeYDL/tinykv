package storage

import (
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// Storage 表示 TinyKV 面向内部的服务器部分，它处理与其他 TinyKV 节点的发送和接收。
// 作为该职责的一部分，它还负责将数据读写到磁盘（或半永久内存）。
type Storage interface {
	Start() error
	Stop() error
	Write(ctx *kvrpcpb.Context, batch []Modify) error
	Reader(ctx *kvrpcpb.Context) (StorageReader, error)
}

type StorageReader interface {
	// 当键不存在时，返回 nil 作为值
	GetCF(cf string, key []byte) ([]byte, error)
	IterCF(cf string) engine_util.DBIterator
	Close()
}
