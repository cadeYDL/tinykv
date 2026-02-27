package standalone_storage

import (
	"log"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap/errors"
)

// StandAloneStorage 是单节点 TinyKV 实例的 `Storage` 接口实现。
// 它不与其他节点通信，所有数据都存储在本地。
type StandAloneStorage struct {
	// 你的数据在这里 (1)。
	storage *badger.DB
	config  *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	aloneStorage := &StandAloneStorage{
		config: conf,
	}
	option := conf.ToBadgerOptions()
	db, err := badger.Open(option)
	if err != nil {
		log.Fatalf("初始化失败:err:%v", err)
		return nil
	}
	aloneStorage.storage = db
	return aloneStorage
}

func (s *StandAloneStorage) Start() error {
	if s == nil || s.storage == nil {
		return errors.New("StandAloneStorage is nil")
	}
	return s.Start()
}

func (s *StandAloneStorage) Stop() error {
	if s == nil || s.storage == nil {
		return errors.New("StandAloneStorage is nil")
	}
	return s.Stop()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// 你的代码在这里 (1)。
	return nil, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// 你的代码在这里 (1)。
	return nil
}
