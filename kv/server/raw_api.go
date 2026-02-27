package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// 以下函数是 Server 的 Raw API（实现 TinyKvServer 接口）。
// 一些辅助方法可以在当前目录的 server.go 中找到

// RawGet 根据 RawGetRequest 的 CF 和 Key 字段返回相应的 Get 响应
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// 你的代码在这里 (1)。
	return nil, nil
}

// RawPut 将目标数据写入存储并返回相应的响应
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// 你的代码在这里 (1)。
	// 提示：考虑使用 Storage.Modify 来存储要修改的数据
	return nil, nil
}

// RawDelete 从存储中删除目标数据并返回相应的响应
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// 你的代码在这里 (1)。
	// 提示：考虑使用 Storage.Modify 来存储要删除的数据
	return nil, nil
}

// RawScan 从起始键开始扫描数据直到达到限制数量，并返回相应的结果
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// 你的代码在这里 (1)。
	// 提示：考虑使用 reader.IterCF
	return nil, nil
}
