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

/*
raft 包使用 eraftpb 包中定义的 Protocol Buffer 格式发送和接收消息。

Raft 是一种协议，集群中的节点可以通过它维护一个复制状态机。
状态机通过使用复制日志保持同步。
有关 Raft 的更多详细信息，请参阅 Diego Ongaro 和 John Ousterhout 的
"In Search of an Understandable Consensus Algorithm"
(https://ramcloud.stanford.edu/raft.pdf)。

用法

raft 中的主要对象是 Node。你可以使用 raft.StartNode 从头开始启动一个 Node，
或者使用 raft.RestartNode 从某个初始状态启动一个 Node。

从头开始启动一个节点：

  storage := raft.NewMemoryStorage()
  c := &Config{
    ID:              0x01,
    ElectionTick:    10,
    HeartbeatTick:   1,
    Storage:         storage,
  }
  n := raft.StartNode(c, []raft.Peer{{ID: 0x02}, {ID: 0x03}})

从之前的状态重启一个节点：

  storage := raft.NewMemoryStorage()

  // 从持久化的快照、状态和日志条目恢复内存存储。
  storage.ApplySnapshot(snapshot)
  storage.SetHardState(state)
  storage.Append(entries)

  c := &Config{
    ID:              0x01,
    ElectionTick:    10,
    HeartbeatTick:   1,
    Storage:         storage,
    MaxInflightMsgs: 256,
  }

  // 重启 raft，不需要对等节点信息。
  // 对等节点信息已经包含在存储中。
  n := raft.RestartNode(c)

现在你持有了一个 Node，你有几个职责：

首先，你必须从 Node.Ready() 通道读取并处理它包含的更新。
这些步骤可以并行执行，除了步骤 2 中注明的情况。

1. 如果 HardState、Entries 和 Snapshot 不为空，则将它们写入持久化存储。
注意，当写入索引为 i 的 Entry 时，任何之前持久化的索引 >= i 的条目都必须被丢弃。

2. 将所有 Messages 发送到 To 字段中指定的节点。重要的是，
在最新的 HardState 被持久化到磁盘之前，以及任何之前 Ready 批次写入的
所有 Entries 之前，不能发送任何消息（当同一批次的条目正在持久化时，可以发送消息）。

注意：消息的序列化不是线程安全的；重要的是确保在序列化时
没有新条目被持久化。实现这一点最简单的方法是直接在
你的主 raft 循环中序列化消息。

3. 将 Snapshot（如果有）和 CommittedEntries 应用到状态机。
如果任何已提交的 Entry 的 Type 是 EntryType_EntryConfChange，
则调用 Node.ApplyConfChange() 将其应用到节点。
在调用 ApplyConfChange 之前，可以通过将 NodeId 字段设置为零来取消配置更改
（但必须以某种方式调用 ApplyConfChange，并且取消的决定
必须仅基于状态机，而不是外部信息，如观察到的节点健康状况）。

4. 调用 Node.Advance() 表示准备好接收下一批更新。
这可以在步骤 1 之后的任何时间完成，但所有更新必须按照
Ready 返回的顺序处理。

其次，所有持久化的日志条目必须通过 Storage 接口的实现提供。
如果你在重启时重新填充其状态，可以使用提供的 MemoryStorage 类型，
或者你可以提供自己的磁盘支持的实现。

第三，当你从另一个节点收到消息时，将其传递给 Node.Step：

	func recvRaftRPC(ctx context.Context, m eraftpb.Message) {
		n.Step(ctx, m)
	}

最后，你需要定期调用 Node.Tick()（可能通过 time.Ticker）。
Raft 有两个重要的超时：心跳和选举超时。然而，在 raft 包内部，
时间由抽象的"tick"表示。

完整的状态机处理循环看起来像这样：

  for {
    select {
    case <-s.Ticker:
      n.Tick()
    case rd := <-s.Node.Ready():
      saveToStorage(rd.State, rd.Entries, rd.Snapshot)
      send(rd.Messages)
      if !raft.IsEmptySnap(rd.Snapshot) {
        processSnapshot(rd.Snapshot)
      }
      for _, entry := range rd.CommittedEntries {
        process(entry)
        if entry.Type == eraftpb.EntryType_EntryConfChange {
          var cc eraftpb.ConfChange
          cc.Unmarshal(entry.Data)
          s.Node.ApplyConfChange(cc)
        }
      }
      s.Node.Advance()
    case <-s.done:
      return
    }
  }

要从你的节点提议对状态机的更改，获取你的应用程序数据，
将其序列化为字节切片，然后调用：

	n.Propose(data)

如果提议被提交，数据将出现在类型为 eraftpb.EntryType_EntryNormal 的已提交条目中。
不能保证提议的命令会被提交；你可能需要在超时后重新提议。

要在集群中添加或删除节点，构建 ConfChange 结构体 'cc' 并调用：

	n.ProposeConfChange(cc)

配置更改提交后，将返回一些类型为 eraftpb.EntryType_EntryConfChange 的已提交条目。
你必须通过以下方式将其应用到节点：

	var cc eraftpb.ConfChange
	cc.Unmarshal(data)
	n.ApplyConfChange(cc)

注意：ID 代表集群中一个节点的永久唯一标识。
给定的 ID 必须只使用一次，即使旧节点已被删除。
这意味着，例如，IP 地址作为节点 ID 是不好的，因为它们可能被重用。
节点 ID 必须是非零的。

实现说明

这个实现与最终的 Raft 论文保持同步
(https://ramcloud.stanford.edu/~ongaro/thesis.pdf)，
尽管我们对成员变更协议的实现与第 4 章中描述的有所不同。
成员变更一次只发生一个节点的关键不变量被保留，
但在我们的实现中，成员变更在其条目被应用时生效，
而不是在它被添加到日志时生效（因此条目是在旧成员配置下提交的，
而不是新配置）。这在安全性方面是等价的，因为旧配置和新配置保证重叠。

为了确保我们不会通过匹配日志位置尝试同时提交两个成员变更
（这将是不安全的，因为它们应该有不同的法定人数要求），
我们简单地禁止在领导者日志中出现任何未提交的变更时
提议任何成员变更。

当你尝试从两成员集群中删除一个成员时，这种方法会引入一个问题：
如果其中一个成员在另一个成员收到 confchange 条目的提交之前死亡，
那么该成员就不能再被删除，因为集群无法取得进展。
因此，强烈建议每个集群使用三个或更多节点。

MessageType

raft 包以 Protocol Buffer 格式（在 eraftpb 包中定义）发送和接收消息。
每个状态（follower、candidate、leader）在处理给定的 eraftpb.Message 时
实现自己的 'step' 方法（'stepFollower'、'stepCandidate'、'stepLeader'）。
每个步骤由其 eraftpb.MessageType 决定。注意，每个步骤都由一个通用方法 'Step' 检查，
该方法对节点和传入消息的任期进行安全检查，以防止过时的日志条目：

	'MessageType_MsgHup' 用于选举。如果一个节点是 follower 或 candidate，
	'raft' 结构体中的 'tick' 函数设置为 'tickElection'。
	如果 follower 或 candidate 在选举超时之前没有收到来自当前任期领导者的任何心跳，
	它会将 'MessageType_MsgHup' 传递给它的 Step 方法，
	并成为（或保持）candidate 以开始新的选举。

	'MessageType_MsgBeat' 是一个内部类型，用于通知领导者发送
	'MessageType_MsgHeartbeat' 类型的心跳。如果一个节点是 leader，
	'raft' 结构体中的 'tick' 函数设置为 'tickHeartbeat'，
	并触发领导者定期向其 follower 发送 'MessageType_MsgHeartbeat' 消息。

	'MessageType_MsgPropose' 提议将数据追加到其日志条目中。
	这是一种特殊类型，用于将提议重定向到领导者。因此，send 方法
	用其 HardState 的任期覆盖 eraftpb.Message 的任期，
	以避免将其本地任期附加到 'MessageType_MsgPropose'。
	当 'MessageType_MsgPropose' 传递给领导者的 'Step' 方法时，
	领导者首先调用 'appendEntry' 方法将条目追加到其日志，
	然后调用 'bcastAppend' 方法将这些条目发送给其对等节点。
	当传递给 candidate 时，'MessageType_MsgPropose' 被丢弃。
	当传递给 follower 时，'MessageType_MsgPropose' 通过 send 方法
	存储在 follower 的邮箱（msgs）中。它与发送者的 ID 一起存储，
	稍后由 rafthttp 包转发给领导者。

	'MessageType_MsgAppend' 包含要复制的日志条目。领导者调用 bcastAppend，
	它调用 sendAppend，发送即将复制的日志，类型为 'MessageType_MsgAppend'。
	当 'MessageType_MsgAppend' 传递给 candidate 的 Step 方法时，
	candidate 恢复为 follower，因为这表明有一个有效的领导者
	正在发送 'MessageType_MsgAppend' 消息。Candidate 和 follower
	以 'MessageType_MsgAppendResponse' 类型响应此消息。

	'MessageType_MsgAppendResponse' 是对日志复制请求（'MessageType_MsgAppend'）的响应。
	当 'MessageType_MsgAppend' 传递给 candidate 或 follower 的 Step 方法时，
	它通过调用 'handleAppendEntries' 方法响应，
	该方法将 'MessageType_MsgAppendResponse' 发送到 raft 邮箱。

	'MessageType_MsgRequestVote' 请求选举投票。当一个节点是 follower 或 candidate
	并且 'MessageType_MsgHup' 传递给它的 Step 方法时，
	该节点调用 'campaign' 方法来竞选成为领导者。
	一旦调用 'campaign' 方法，该节点成为 candidate
	并向集群中的对等节点发送 'MessageType_MsgRequestVote' 以请求投票。
	当传递给领导者或 candidate 的 Step 方法并且消息的 Term
	低于领导者或 candidate 的时，'MessageType_MsgRequestVote' 将被拒绝
	（返回 Reject 为 true 的 'MessageType_MsgRequestVoteResponse'）。
	如果领导者或 candidate 收到具有更高任期的 'MessageType_MsgRequestVote'，
	它将恢复为 follower。当 'MessageType_MsgRequestVote' 传递给 follower 时，
	只有当发送者的最后任期大于 MessageType_MsgRequestVote 的任期，
	或发送者的最后任期等于 MessageType_MsgRequestVote 的任期
	但发送者的最后已提交索引大于或等于 follower 的时，它才投票给发送者。

	'MessageType_MsgRequestVoteResponse' 包含投票请求的响应。
	当 'MessageType_MsgRequestVoteResponse' 传递给 candidate 时，
	candidate 计算它赢得了多少票。如果超过多数（法定人数），
	它成为领导者并调用 'bcastAppend'。
	如果 candidate 收到多数拒绝票，它恢复为 follower。

	'MessageType_MsgSnapshot' 请求安装快照消息。当一个节点刚刚成为领导者
	或领导者收到 'MessageType_MsgPropose' 消息时，它调用 'bcastAppend' 方法，
	然后调用 'sendAppend' 方法给每个 follower。在 'sendAppend' 中，
	如果领导者无法获取任期或条目，
	领导者通过发送 'MessageType_MsgSnapshot' 类型的消息请求快照。

	'MessageType_MsgHeartbeat' 从领导者发送心跳。
	当 'MessageType_MsgHeartbeat' 传递给 candidate
	并且消息的任期高于 candidate 的时，candidate 恢复为 follower
	并从此心跳中更新其已提交索引。它将消息发送到其邮箱。
	当 'MessageType_MsgHeartbeat' 传递给 follower 的 Step 方法
	并且消息的任期高于 follower 的时，follower 用消息中的 ID 更新其 leaderID。

	'MessageType_MsgHeartbeatResponse' 是对 'MessageType_MsgHeartbeat' 的响应。
	当 'MessageType_MsgHeartbeatResponse' 传递给领导者的 Step 方法时，
	领导者知道哪个 follower 响应了。

*/
package raft
