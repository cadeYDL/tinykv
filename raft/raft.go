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

// None 是一个占位符节点 ID，用于表示没有领导者。
const None uint64 = 0

// StateType 表示节点在集群中的角色。
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped 在提议被某些情况忽略时返回，
// 以便提议者可以被通知并快速失败。
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config 包含启动 raft 的参数。
type Config struct {
	// ID 是本地 raft 的标识。ID 不能为 0。
	ID uint64

	// peers 包含 raft 集群中所有节点（包括自己）的 ID。
	// 它应该仅在启动新的 raft 集群时设置。
	// 如果设置了 peers，从之前的配置重启 raft 将会 panic。
	// peer 是私有的，目前仅用于测试。
	peers []uint64

	// ElectionTick 是在选举之间必须经过的 Node.Tick 调用次数。
	// 也就是说，如果一个 follower 在 ElectionTick 过去之前没有收到
	// 当前任期领导者的任何消息，它将成为 candidate 并开始选举。
	// ElectionTick 必须大于 HeartbeatTick。
	// 我们建议 ElectionTick = 10 * HeartbeatTick 以避免不必要的领导者切换。
	ElectionTick int
	// HeartbeatTick 是在心跳之间必须经过的 Node.Tick 调用次数。
	// 也就是说，领导者每 HeartbeatTick 个 tick 发送心跳消息以维持其领导地位。
	HeartbeatTick int

	// Storage 是 raft 的存储。raft 生成要存储在 storage 中的条目和状态。
	// 当需要时，raft 从 Storage 读取持久化的条目和状态。
	// 重启时，raft 从 storage 中读取之前的状态和配置。
	Storage Storage
	// Applied 是最后应用的索引。它应该仅在重启 raft 时设置。
	// raft 不会返回小于或等于 Applied 的条目给应用程序。
	// 如果重启时未设置 Applied，raft 可能会返回之前已应用的条目。
	// 这是一个非常依赖应用程序的配置。
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress 表示领导者视角下 follower 的进度。
// 领导者维护所有 follower 的进度，并根据其进度向 follower 发送条目。
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// 日志
	RaftLog *RaftLog

	// 每个对等节点的日志复制进度
	Prs map[uint64]*Progress

	// 此对等节点的角色
	State StateType

	// 投票记录
	votes map[uint64]bool

	// 需要发送的消息
	msgs []pb.Message

	// 领导者 id
	Lead uint64

	// 心跳间隔，应该发送
	heartbeatTimeout int
	// 选举间隔的基准
	electionTimeout int
	// 自上次到达 heartbeatTimeout 以来的 tick 数。
	// 只有 leader 保持 heartbeatElapsed。
	heartbeatElapsed int
	// 当它是 leader 或 candidate 时，自上次到达 electionTimeout 以来的 tick 数。
	// 当它是 follower 时，自上次到达 electionTimeout 或收到
	// 来自当前领导者的有效消息以来的 tick 数。
	electionElapsed int

	// leadTransferee 是领导权转移目标的 id，当其值不为零时。
	// 遵循 Raft 博士论文第 3.10 节中定义的过程。
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// （用于 3A 领导权转移）
	leadTransferee uint64

	// 同一时间只能有一个配置变更处于待定状态（在日志中，但尚未应用）。
	// 这通过 PendingConfIndex 强制执行，它被设置为一个
	// >= 最新待定配置变更（如果有）的日志索引的值。
	// 只有当领导者的已应用索引大于此值时，才允许提议配置变更。
	// （用于 3A 配置变更）
	PendingConfIndex uint64
}

// newRaft 返回一个具有给定配置的 raft 对等节点
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// 你的代码在这里 (2A)。
	return nil
}

// sendAppend 向给定的对等节点发送一个包含新条目（如果有）
// 和当前提交索引的 append RPC。如果消息被发送则返回 true。
func (r *Raft) sendAppend(to uint64) bool {
	// 你的代码在这里 (2A)。
	return false
}

// sendHeartbeat 向给定的对等节点发送心跳 RPC。
func (r *Raft) sendHeartbeat(to uint64) {
	// 你的代码在这里 (2A)。
}

// tick 将内部逻辑时钟推进一个 tick。
func (r *Raft) tick() {
	// 你的代码在这里 (2A)。
}

// becomeFollower 将此对等节点的状态转换为 Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// 你的代码在这里 (2A)。
}

// becomeCandidate 将此对等节点的状态转换为 candidate
func (r *Raft) becomeCandidate() {
	// 你的代码在这里 (2A)。
}

// becomeLeader 将此对等节点的状态转换为 leader
func (r *Raft) becomeLeader() {
	// 你的代码在这里 (2A)。
	// 注意：Leader 应该在其任期内提议一个空操作条目
}

// Step 是处理消息的入口，参见 `eraftpb.proto` 中的 `MessageType`
// 了解应该处理哪些消息
func (r *Raft) Step(m pb.Message) error {
	// 你的代码在这里 (2A)。
	switch r.State {
	case StateFollower:
	case StateCandidate:
	case StateLeader:
	}
	return nil
}

// handleAppendEntries 处理 AppendEntries RPC 请求
func (r *Raft) handleAppendEntries(m pb.Message) {
	// 你的代码在这里 (2A)。
}

// handleHeartbeat 处理 Heartbeat RPC 请求
func (r *Raft) handleHeartbeat(m pb.Message) {
	// 你的代码在这里 (2A)。
}

// handleSnapshot 处理 Snapshot RPC 请求
func (r *Raft) handleSnapshot(m pb.Message) {
	// 你的代码在这里 (2C)。
}

// addNode 向 raft 组添加一个新节点
func (r *Raft) addNode(id uint64) {
	// 你的代码在这里 (3A)。
}

// removeNode 从 raft 组移除一个节点
func (r *Raft) removeNode(id uint64) {
	// 你的代码在这里 (3A)。
}
