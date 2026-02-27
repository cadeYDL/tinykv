// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

// RaftLog 管理日志条目，其结构如下：
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            日志条目
//
// 为简化起见，RaftLog 实现应该管理所有未截断的日志条目
type RaftLog struct {
	// storage 包含自上次快照以来的所有稳定条目。
	storage Storage

	// committed 是已知在法定人数节点的稳定存储中的最高日志位置。
	committed uint64

	// applied 是应用程序已被指示应用到其状态机的最高日志位置。
	// 不变量：applied <= committed
	applied uint64

	// 索引 <= stabled 的日志条目已持久化到存储。
	// 它用于记录尚未被 storage 持久化的日志。
	// 每次处理 `Ready` 时，都会包含未稳定的日志。
	stabled uint64

	// 所有尚未压缩的条目。
	entries []pb.Entry

	// 传入的不稳定快照（如果有）。
	// （用于 2C）
	pendingSnapshot *pb.Snapshot

	// 你的数据在这里 (2A)。
}

// newLog 使用给定的存储返回日志。它将日志恢复到
// 刚刚提交并应用最新快照的状态。
func newLog(storage Storage) *RaftLog {
	// 你的代码在这里 (2A)。
	return nil
}

// 我们需要在某个时间点压缩日志条目，
// 例如 storage 压缩已稳定的日志条目，
// 以防止日志条目在内存中无限增长
func (l *RaftLog) maybeCompact() {
	// 你的代码在这里 (2C)。
}

// allEntries 返回所有未压缩的条目。
// 注意，从返回值中排除任何虚拟条目。
// 注意，这是你需要实现的测试桩函数之一。
func (l *RaftLog) allEntries() []pb.Entry {
	// 你的代码在这里 (2A)。
	return nil
}

// unstableEntries 返回所有不稳定的条目
func (l *RaftLog) unstableEntries() []pb.Entry {
	// 你的代码在这里 (2A)。
	return nil
}

// nextEnts 返回所有已提交但未应用的条目
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// 你的代码在这里 (2A)。
	return nil
}

// LastIndex 返回日志条目的最后索引
func (l *RaftLog) LastIndex() uint64 {
	// 你的代码在这里 (2A)。
	return 0
}

// Term 返回给定索引处条目的任期
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// 你的代码在这里 (2A)。
	return 0, nil
}
