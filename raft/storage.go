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

import (
	"errors"
	"sync"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// ErrCompacted 当请求的索引由于在最后一个快照之前而不可用时，
// 由 Storage.Entries/Compact 返回。
var ErrCompacted = errors.New("requested index is unavailable due to compaction")

// ErrSnapOutOfDate 当请求的索引比现有快照更旧时，
// 由 Storage.CreateSnapshot 返回。
var ErrSnapOutOfDate = errors.New("requested index is older than the existing snapshot")

// ErrUnavailable 当请求的日志条目不可用时，由 Storage 接口返回。
var ErrUnavailable = errors.New("requested entry at index is unavailable")

// ErrSnapshotTemporarilyUnavailable 当所需的快照暂时不可用时，
// 由 Storage 接口返回。
var ErrSnapshotTemporarilyUnavailable = errors.New("snapshot is temporarily unavailable")

// Storage 是一个接口，可以由应用程序实现以从存储中检索日志条目。
//
// 如果任何 Storage 方法返回错误，raft 实例将变得不可操作
// 并拒绝参与选举；在这种情况下，应用程序负责清理和恢复。
type Storage interface {
	// InitialState 返回保存的 HardState 和 ConfState 信息。
	InitialState() (pb.HardState, pb.ConfState, error)
	// Entries 返回 [lo,hi) 范围内的日志条目切片。
	// MaxSize 限制返回的日志条目的总大小，但如果有条目的话，
	// Entries 至少返回一个条目。
	Entries(lo, hi uint64) ([]pb.Entry, error)
	// Term 返回条目 i 的任期，它必须在 [FirstIndex()-1, LastIndex()] 范围内。
	// FirstIndex 之前条目的任期被保留用于匹配目的，
	// 即使该条目的其余部分可能不可用。
	Term(i uint64) (uint64, error)
	// LastIndex 返回日志中最后一个条目的索引。
	LastIndex() (uint64, error)
	// FirstIndex 返回可能通过 Entries 获取的第一个日志条目的索引
	// （较旧的条目已被合并到最新的 Snapshot 中；
	// 如果 storage 只包含虚拟条目，则第一个日志条目不可用）。
	FirstIndex() (uint64, error)
	// Snapshot 返回最近的快照。
	// 如果快照暂时不可用，它应该返回 ErrSnapshotTemporarilyUnavailable，
	// 这样 raft 状态机可以知道 Storage 需要一些时间来准备快照，
	// 稍后再调用 Snapshot。
	Snapshot() (pb.Snapshot, error)
}

// MemoryStorage 实现了由内存数组支持的 Storage 接口。
type MemoryStorage struct {
	// 保护对所有字段的访问。MemoryStorage 的大多数方法
	// 在 raft goroutine 上运行，但 Append() 在应用程序 goroutine 上运行。
	sync.Mutex

	hardState pb.HardState
	snapshot  pb.Snapshot
	// ents[i] 的 raft 日志位置是 i+snapshot.Metadata.Index
	ents []pb.Entry
}

// NewMemoryStorage 创建一个空的 MemoryStorage。
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		// 从头开始时，用任期为零的虚拟条目填充列表。
		ents:     make([]pb.Entry, 1),
		snapshot: pb.Snapshot{Metadata: &pb.SnapshotMetadata{ConfState: &pb.ConfState{}}},
	}
}

// InitialState 实现 Storage 接口。
func (ms *MemoryStorage) InitialState() (pb.HardState, pb.ConfState, error) {
	return ms.hardState, *ms.snapshot.Metadata.ConfState, nil
}

// SetHardState 保存当前的 HardState。
func (ms *MemoryStorage) SetHardState(st pb.HardState) error {
	ms.Lock()
	defer ms.Unlock()
	ms.hardState = st
	return nil
}

// Entries 实现 Storage 接口。
func (ms *MemoryStorage) Entries(lo, hi uint64) ([]pb.Entry, error) {
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].Index
	if lo <= offset {
		return nil, ErrCompacted
	}
	if hi > ms.lastIndex()+1 {
		log.Panicf("entries' hi(%d) is out of bound lastindex(%d)", hi, ms.lastIndex())
	}

	ents := ms.ents[lo-offset : hi-offset]
	if len(ms.ents) == 1 && len(ents) != 0 {
		// 只包含虚拟条目。
		return nil, ErrUnavailable
	}
	return ents, nil
}

// Term 实现 Storage 接口。
func (ms *MemoryStorage) Term(i uint64) (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].Index
	if i < offset {
		return 0, ErrCompacted
	}
	if int(i-offset) >= len(ms.ents) {
		return 0, ErrUnavailable
	}
	return ms.ents[i-offset].Term, nil
}

// LastIndex 实现 Storage 接口。
func (ms *MemoryStorage) LastIndex() (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.lastIndex(), nil
}

func (ms *MemoryStorage) lastIndex() uint64 {
	return ms.ents[0].Index + uint64(len(ms.ents)) - 1
}

// FirstIndex 实现 Storage 接口。
func (ms *MemoryStorage) FirstIndex() (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.firstIndex(), nil
}

func (ms *MemoryStorage) firstIndex() uint64 {
	return ms.ents[0].Index + 1
}

// Snapshot 实现 Storage 接口。
func (ms *MemoryStorage) Snapshot() (pb.Snapshot, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.snapshot, nil
}

// ApplySnapshot 用给定快照的内容覆盖此 Storage 对象的内容。
func (ms *MemoryStorage) ApplySnapshot(snap pb.Snapshot) error {
	ms.Lock()
	defer ms.Unlock()

	// 处理旧快照被应用的检查
	msIndex := ms.snapshot.Metadata.Index
	snapIndex := snap.Metadata.Index
	if msIndex >= snapIndex {
		return ErrSnapOutOfDate
	}

	ms.snapshot = snap
	ms.ents = []pb.Entry{{Term: snap.Metadata.Term, Index: snap.Metadata.Index}}
	return nil
}

// CreateSnapshot 创建一个快照，可以用 Snapshot() 检索，
// 并可用于重建该时间点的状态。
// 如果自上次压缩以来进行了任何配置更改，
// 必须传入最后一次 ApplyConfChange 的结果。
func (ms *MemoryStorage) CreateSnapshot(i uint64, cs *pb.ConfState, data []byte) (pb.Snapshot, error) {
	ms.Lock()
	defer ms.Unlock()
	if i <= ms.snapshot.Metadata.Index {
		return pb.Snapshot{}, ErrSnapOutOfDate
	}

	offset := ms.ents[0].Index
	if i > ms.lastIndex() {
		log.Panicf("snapshot %d is out of bound lastindex(%d)", i, ms.lastIndex())
	}

	ms.snapshot.Metadata.Index = i
	ms.snapshot.Metadata.Term = ms.ents[i-offset].Term
	if cs != nil {
		ms.snapshot.Metadata.ConfState = cs
	}
	ms.snapshot.Data = data
	return ms.snapshot, nil
}

// Compact 丢弃 compactIndex 之前的所有日志条目。
// 应用程序有责任不尝试压缩大于 raftLog.applied 的索引。
func (ms *MemoryStorage) Compact(compactIndex uint64) error {
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].Index
	if compactIndex <= offset {
		return ErrCompacted
	}
	if compactIndex > ms.lastIndex() {
		log.Panicf("compact %d is out of bound lastindex(%d)", compactIndex, ms.lastIndex())
	}

	i := compactIndex - offset
	ents := make([]pb.Entry, 1, 1+uint64(len(ms.ents))-i)
	ents[0].Index = ms.ents[i].Index
	ents[0].Term = ms.ents[i].Term
	ents = append(ents, ms.ents[i+1:]...)
	ms.ents = ents
	return nil
}

// Append 将新条目追加到存储。
// TODO (xiangli): 确保条目是连续的，并且 entries[0].Index > ms.entries[0].Index
func (ms *MemoryStorage) Append(entries []pb.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	ms.Lock()
	defer ms.Unlock()

	first := ms.firstIndex()
	last := entries[0].Index + uint64(len(entries)) - 1

	// 如果没有新条目则快捷返回。
	if last < first {
		return nil
	}
	// 截断已压缩的条目
	if first > entries[0].Index {
		entries = entries[first-entries[0].Index:]
	}

	offset := entries[0].Index - ms.ents[0].Index
	switch {
	case uint64(len(ms.ents)) > offset:
		ms.ents = append([]pb.Entry{}, ms.ents[:offset]...)
		ms.ents = append(ms.ents, entries...)
	case uint64(len(ms.ents)) == offset:
		ms.ents = append(ms.ents, entries...)
	default:
		log.Panicf("missing log entry [last: %d, append at: %d]",
			ms.lastIndex(), entries[0].Index)
	}
	return nil
}
