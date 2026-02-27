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

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// ErrStepLocalMsg 在尝试步进本地 raft 消息时返回
var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

// ErrStepPeerNotFound 在尝试步进响应消息但在 raft.Prs 中找不到该节点的对等节点时返回
var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

// SoftState 提供易失性状态，不需要持久化到 WAL。
type SoftState struct {
	Lead      uint64
	RaftState StateType
}

// Ready 封装了准备好读取、保存到稳定存储、提交或发送给其他对等节点的条目和消息。
// Ready 中的所有字段都是只读的。
type Ready struct {
	// 节点的当前易失状态。
	// 如果没有更新，SoftState 将为 nil。
	// 不需要消费或存储 SoftState。
	*SoftState

	// 在发送 Messages 之前要保存到稳定存储的节点当前状态。
	// 如果没有更新，HardState 将等于空状态。
	pb.HardState

	// Entries 指定在发送 Messages 之前要保存到稳定存储的条目。
	Entries []pb.Entry

	// Snapshot 指定要保存到稳定存储的快照。
	Snapshot pb.Snapshot

	// CommittedEntries 指定要提交到存储/状态机的条目。
	// 这些条目之前已经提交到稳定存储。
	CommittedEntries []pb.Entry

	// Messages 指定在 Entries 提交到稳定存储后要发送的出站消息。
	// 如果它包含 MessageType_MsgSnapshot 消息，应用程序必须在
	// 收到快照或快照失败时通过调用 ReportSnapshot 向 raft 报告。
	Messages []pb.Message
}

// RawNode 是 Raft 的包装器。
type RawNode struct {
	Raft *Raft
	// 你的数据在这里 (2A)。
}

// NewRawNode 给定配置和 raft 对等节点列表返回一个新的 RawNode。
func NewRawNode(config *Config) (*RawNode, error) {
	// 你的代码在这里 (2A)。
	return nil, nil
}

// Tick 将内部逻辑时钟推进一个 tick。
func (rn *RawNode) Tick() {
	rn.Raft.tick()
}

// Campaign 使此 RawNode 转换到 candidate 状态。
func (rn *RawNode) Campaign() error {
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgHup,
	})
}

// Propose 提议将数据追加到 raft 日志。
func (rn *RawNode) Propose(data []byte) error {
	ent := pb.Entry{Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    rn.Raft.id,
		Entries: []*pb.Entry{&ent}})
}

// ProposeConfChange 提议一个配置变更。
func (rn *RawNode) ProposeConfChange(cc pb.ConfChange) error {
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	ent := pb.Entry{EntryType: pb.EntryType_EntryConfChange, Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{&ent},
	})
}

// ApplyConfChange 将配置变更应用到本地节点。
func (rn *RawNode) ApplyConfChange(cc pb.ConfChange) *pb.ConfState {
	if cc.NodeId == None {
		return &pb.ConfState{Nodes: nodes(rn.Raft)}
	}
	switch cc.ChangeType {
	case pb.ConfChangeType_AddNode:
		rn.Raft.addNode(cc.NodeId)
	case pb.ConfChangeType_RemoveNode:
		rn.Raft.removeNode(cc.NodeId)
	default:
		panic("unexpected conf type")
	}
	return &pb.ConfState{Nodes: nodes(rn.Raft)}
}

// Step 使用给定的消息推进状态机。
func (rn *RawNode) Step(m pb.Message) error {
	// 忽略通过网络接收的意外本地消息
	if IsLocalMsg(m.MsgType) {
		return ErrStepLocalMsg
	}
	if pr := rn.Raft.Prs[m.From]; pr != nil || !IsResponseMsg(m.MsgType) {
		return rn.Raft.Step(m)
	}
	return ErrStepPeerNotFound
}

// Ready 返回此 RawNode 的当前时间点状态。
func (rn *RawNode) Ready() Ready {
	// 你的代码在这里 (2A)。
	return Ready{}
}

// HasReady 在 RawNode 用户需要检查是否有待处理的 Ready 时调用。
func (rn *RawNode) HasReady() bool {
	// 你的代码在这里 (2A)。
	return false
}

// Advance 通知 RawNode 应用程序已在上次 Ready 结果中应用并保存了进度。
func (rn *RawNode) Advance(rd Ready) {
	// 你的代码在这里 (2A)。
}

// GetProgress 返回此节点及其对等节点的 Progress，如果此节点是 leader。
func (rn *RawNode) GetProgress() map[uint64]Progress {
	prs := make(map[uint64]Progress)
	if rn.Raft.State == StateLeader {
		for id, p := range rn.Raft.Prs {
			prs[id] = *p
		}
	}
	return prs
}

// TransferLeader 尝试将领导权转移给给定的 transferee。
func (rn *RawNode) TransferLeader(transferee uint64) {
	_ = rn.Raft.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: transferee})
}
