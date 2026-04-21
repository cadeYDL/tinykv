package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap/errors"
)

// 以下函数是 Server 的 Raw API（实现 TinyKvServer 接口）。
// 一些辅助方法可以在当前目录的 server.go 中找到

// RawGet 根据 RawGetRequest 的 CF 和 Key 字段返回相应的 Get 响应
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// 你的代码在这里 (1)。
	resp := new(kvrpcpb.RawGetResponse)
	if req == nil {
		resp.Error = "入参为空"
		return resp, errors.BadRequestf("入参为空")
	}
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	if reader == nil {
		resp.Error = "系统错误"
		return resp, errors.New("reader is nil")
	}
	data, err := reader.GetCF(req.GetCf(), req.GetKey())
	if err != nil {
		resp.Error = err.Error()
	}
	if len(data) == 0 {
		resp.NotFound = true
	}
	resp.Value = data
	return resp, err
}

// RawPut 将目标数据写入存储并返回相应的响应
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// 你的代码在这里 (1)。
	// 提示：考虑使用 Storage.Modify 来存储要修改的数据
	resp := new(kvrpcpb.RawPutResponse)
	if req == nil {
		resp.Error = "入参为空"
		return resp, errors.BadRequestf("入参为空")
	}
	data := storage.Modify{
		Data: storage.Put{
			Key:   req.GetKey(),
			Value: req.GetValue(),
			Cf:    req.GetCf(),
		},
	}
	err := server.storage.Write(req.GetContext(), []storage.Modify{data})
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, err
}

// RawDelete 从存储中删除目标数据并返回相应的响应
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// 你的代码在这里 (1)。
	// 提示：考虑使用 Storage.Modify 来存储要删除的数据
	resp := new(kvrpcpb.RawDeleteResponse)
	if req == nil {
		resp.Error = "入参为空"
		return resp, errors.BadRequestf("入参为空")
	}
	data := storage.Modify{
		Data: storage.Delete{
			Key: req.GetKey(),
			Cf:  req.GetCf(),
		},
	}
	err := server.storage.Write(req.GetContext(), []storage.Modify{data})
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, err
}

// RawScan 从起始键开始扫描数据直到达到限制数量，并返回相应的结果
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// 你的代码在这里 (1)。
	// 提示：考虑使用 reader.IterCF
	resp := new(kvrpcpb.RawScanResponse)
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	if reader == nil {
		resp.Error = "系统错误"
		return resp, errors.New("reader is nil")
	}
	iter := reader.IterCF(req.GetCf())
	if len(req.GetStartKey()) > 0 {
		iter.Seek(req.GetStartKey())
	}
	resp.Kvs = make([]*kvrpcpb.KvPair, 0, req.GetLimit())
	for item := iter.Item(); item != nil && iter.Valid(); iter.Next() {
		if len(resp.GetKvs()) == int(req.GetLimit()) {
			break
		}
		keyCopy := make([]byte, 0, len(item.Key()))
		item.KeyCopy(keyCopy)
		valCopy, valErr := item.ValueCopy(nil)
		if valErr != nil {
			resp.Error = valErr.Error()
		}
		resp.Kvs = append(resp.GetKvs(), &kvrpcpb.KvPair{
			Error: nil,
			Key:   item.Key(),
			Value: valCopy,
		})
		if !iter.Valid() {
			break
		}
	}
	return resp, nil
}
