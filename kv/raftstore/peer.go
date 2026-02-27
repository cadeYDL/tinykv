package raftstore

import (
	"fmt"
	"time"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/util/worker"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/raft"
	"github.com/pingcap/errors"
)

func NotifyStaleReq(term uint64, cb *message.Callback) {
	cb.Done(ErrRespStaleCommand(term))
}

func NotifyReqRegionRemoved(regionId uint64, cb *message.Callback) {
	regionNotFound := &util.ErrRegionNotFound{RegionId: regionId}
	resp := ErrResp(regionNotFound)
	cb.Done(resp)
}

// 如果我们主动创建 peer，比如 bootstrap/split/merge region，我们应该
// 使用这个函数来创建 peer。region 必须包含此 store 的 peer 信息。
func createPeer(storeID uint64, cfg *config.Config, sched chan<- worker.Task,
	engines *engine_util.Engines, region *metapb.Region) (*peer, error) {
	metaPeer := util.FindPeer(region, storeID)
	if metaPeer == nil {
		return nil, errors.Errorf("find no peer for store %d in region %v", storeID, region)
	}
	log.Infof("region %v create peer with ID %d", region, metaPeer.Id)
	return NewPeer(storeID, cfg, engines, region, sched, metaPeer)
}

// peer 可以通过 raft 成员变更从另一个节点创建，创建这个复制的 peer 时
// 我们只知道 region_id 和 peer_id，region 信息将在应用快照后获取。
func replicatePeer(storeID uint64, cfg *config.Config, sched chan<- worker.Task,
	engines *engine_util.Engines, regionID uint64, metaPeer *metapb.Peer) (*peer, error) {
	// 我们将在应用快照时删除 tombstone 键
	log.Infof("[region %v] replicates peer with ID %d", regionID, metaPeer.GetId())
	region := &metapb.Region{
		Id:          regionID,
		RegionEpoch: &metapb.RegionEpoch{},
	}
	return NewPeer(storeID, cfg, engines, region, sched, metaPeer)
}

type proposal struct {
	// index + term 用于唯一标识
	index uint64
	term  uint64
	cb    *message.Callback
}

type peer struct {
	// peer 的定时器，用于触发
	// * raft tick
	// * raft 日志垃圾回收
	// * region 心跳
	// * 分裂检查
	ticker *ticker
	// Raft 模块的实例
	RaftGroup *raft.RawNode
	// Raft 模块的 peer 存储
	peerStorage *PeerStorage

	// 记录 peer 的元信息
	Meta     *metapb.Peer
	regionId uint64
	// 用于打印日志的标签
	Tag string

	// 记录提议的回调
	// （在 2B 中使用）
	proposals []*proposal

	// 上次调度的压缩 raft 日志的索引
	// （在 2C 中使用）
	LastCompactedIdx uint64

	// 缓存来自其他 store 的 peer 信息
	// 向其他 peer 发送 raft 消息时，用于获取目标 peer 的 store id
	// （在 3B 配置变更中使用）
	peerCache map[uint64]*metapb.Peer
	// 记录 peer 被添加到配置中的时刻
	// 当它们不再处于 pending 状态后移除
	// （在 3B 配置变更中使用）
	PeersStartPendingTime map[uint64]time.Time
	// 标记 peer 已停止，当 peer 被销毁时设置
	// （在 3B 配置变更中使用）
	stopped bool

	// 自上次重置以来 region 大小的不精确差异
	// 当它超过阈值时触发分裂检查器，这使得分裂检查器不会频繁扫描数据
	// （在 3B 分裂中使用）
	SizeDiffHint uint64
	// region 的近似大小
	// 每次分裂检查器扫描数据时更新
	// （在 3B 分裂中使用）
	ApproximateSize *uint64
}

func NewPeer(storeId uint64, cfg *config.Config, engines *engine_util.Engines, region *metapb.Region, regionSched chan<- worker.Task,
	meta *metapb.Peer) (*peer, error) {
	if meta.GetId() == util.InvalidID {
		return nil, fmt.Errorf("invalid peer id")
	}
	tag := fmt.Sprintf("[region %v] %v", region.GetId(), meta.GetId())

	ps, err := NewPeerStorage(engines, region, regionSched, tag)
	if err != nil {
		return nil, err
	}

	appliedIndex := ps.AppliedIndex()

	raftCfg := &raft.Config{
		ID:            meta.GetId(),
		ElectionTick:  cfg.RaftElectionTimeoutTicks,
		HeartbeatTick: cfg.RaftHeartbeatTicks,
		Applied:       appliedIndex,
		Storage:       ps,
	}

	raftGroup, err := raft.NewRawNode(raftCfg)
	if err != nil {
		return nil, err
	}
	p := &peer{
		Meta:                  meta,
		regionId:              region.GetId(),
		RaftGroup:             raftGroup,
		peerStorage:           ps,
		peerCache:             make(map[uint64]*metapb.Peer),
		PeersStartPendingTime: make(map[uint64]time.Time),
		Tag:                   tag,
		ticker:                newTicker(region.GetId(), cfg),
	}

	// 如果这个 region 只有一个 peer 且我就是那个 peer，直接开始竞选。
	if len(region.GetPeers()) == 1 && region.GetPeers()[0].GetStoreId() == storeId {
		err = p.RaftGroup.Campaign()
		if err != nil {
			return nil, err
		}
	}

	return p, nil
}

func (p *peer) insertPeerCache(peer *metapb.Peer) {
	p.peerCache[peer.GetId()] = peer
}

func (p *peer) removePeerCache(peerID uint64) {
	delete(p.peerCache, peerID)
}

func (p *peer) getPeerFromCache(peerID uint64) *metapb.Peer {
	if peer, ok := p.peerCache[peerID]; ok {
		return peer
	}
	for _, peer := range p.peerStorage.Region().GetPeers() {
		if peer.GetId() == peerID {
			p.insertPeerCache(peer)
			return peer
		}
	}
	return nil
}

func (p *peer) nextProposalIndex() uint64 {
	return p.RaftGroup.Raft.RaftLog.LastIndex() + 1
}

/// 尝试销毁自己。如果需要，返回一个任务来执行更多清理工作。
func (p *peer) MaybeDestroy() bool {
	if p.stopped {
		log.Infof("%v is being destroyed, skip", p.Tag)
		return false
	}
	return true
}

/// 执行实际的销毁 worker.Task，包括：
/// 1. 将 region 设置为 tombstone；
/// 2. 清除数据；
/// 3. 通知所有待处理的请求。
func (p *peer) Destroy(engine *engine_util.Engines, keepData bool) error {
	start := time.Now()
	region := p.Region()
	log.Infof("%v begin to destroy", p.Tag)

	// 显式设置 Tombstone 状态
	kvWB := new(engine_util.WriteBatch)
	raftWB := new(engine_util.WriteBatch)
	if err := p.peerStorage.clearMeta(kvWB, raftWB); err != nil {
		return err
	}
	meta.WriteRegionState(kvWB, region, rspb.PeerState_Tombstone)
	// 首先写入 kv badgerDB，以防两次写入之间发生重启
	if err := kvWB.WriteToDB(engine.Kv); err != nil {
		return err
	}
	if err := raftWB.WriteToDB(engine.Raft); err != nil {
		return err
	}

	if p.peerStorage.isInitialized() && !keepData {
		// 如果我们在删除数据和 raft 日志时遇到 panic，脏数据
		// 将通过更新的快照应用或重启来清除。
		p.peerStorage.ClearData()
	}

	for _, proposal := range p.proposals {
		NotifyReqRegionRemoved(region.Id, proposal.cb)
	}
	p.proposals = nil

	log.Infof("%v destroy itself, takes %v", p.Tag, time.Now().Sub(start))
	return nil
}

func (p *peer) isInitialized() bool {
	return p.peerStorage.isInitialized()
}

func (p *peer) storeID() uint64 {
	return p.Meta.StoreId
}

func (p *peer) Region() *metapb.Region {
	return p.peerStorage.Region()
}

/// 设置 peer 的 region。
///
/// 这将更新 peer 的 region，调用者必须确保 region
/// 已被保存在持久化设备中。
func (p *peer) SetRegion(region *metapb.Region) {
	p.peerStorage.SetRegion(region)
}

func (p *peer) PeerId() uint64 {
	return p.Meta.GetId()
}

func (p *peer) LeaderId() uint64 {
	return p.RaftGroup.Raft.Lead
}

func (p *peer) IsLeader() bool {
	return p.RaftGroup.Raft.State == raft.StateLeader
}

func (p *peer) Send(trans Transport, msgs []eraftpb.Message) {
	for _, msg := range msgs {
		err := p.sendRaftMessage(msg, trans)
		if err != nil {
			log.Debugf("%v send message err: %v", p.Tag, err)
		}
	}
}

/// 收集所有待处理的 peer 并更新 `peers_start_pending_time`。
func (p *peer) CollectPendingPeers() []*metapb.Peer {
	pendingPeers := make([]*metapb.Peer, 0, len(p.Region().GetPeers()))
	truncatedIdx := p.peerStorage.truncatedIndex()
	for id, progress := range p.RaftGroup.GetProgress() {
		if id == p.Meta.GetId() {
			continue
		}
		if progress.Match < truncatedIdx {
			if peer := p.getPeerFromCache(id); peer != nil {
				pendingPeers = append(pendingPeers, peer)
				if _, ok := p.PeersStartPendingTime[id]; !ok {
					now := time.Now()
					p.PeersStartPendingTime[id] = now
					log.Debugf("%v peer %v start pending at %v", p.Tag, id, now)
				}
			}
		}
	}
	return pendingPeers
}

func (p *peer) clearPeersStartPendingTime() {
	for id := range p.PeersStartPendingTime {
		delete(p.PeersStartPendingTime, id)
	}
}

/// 如果有任何新 peer 在复制日志方面追上了 leader，则返回 `true`。
/// 如果需要，还会更新 `PeersStartPendingTime`。
func (p *peer) AnyNewPeerCatchUp(peerId uint64) bool {
	if len(p.PeersStartPendingTime) == 0 {
		return false
	}
	if !p.IsLeader() {
		p.clearPeersStartPendingTime()
		return false
	}
	if startPendingTime, ok := p.PeersStartPendingTime[peerId]; ok {
		truncatedIdx := p.peerStorage.truncatedIndex()
		progress, ok := p.RaftGroup.Raft.Prs[peerId]
		if ok {
			if progress.Match >= truncatedIdx {
				delete(p.PeersStartPendingTime, peerId)
				elapsed := time.Since(startPendingTime)
				log.Debugf("%v peer %v has caught up logs, elapsed: %v", p.Tag, peerId, elapsed)
				return true
			}
		}
	}
	return false
}

func (p *peer) MaybeCampaign(parentIsLeader bool) bool {
	// peer 在创建时已经竞选过了，不需要再做。
	if len(p.Region().GetPeers()) <= 1 || !parentIsLeader {
		return false
	}

	// 如果上一个 peer 在分裂前是 region 的 leader，
	// 让它成为新分裂 region 的 leader 是很自然的。
	p.RaftGroup.Campaign()
	return true
}

func (p *peer) Term() uint64 {
	return p.RaftGroup.Raft.Term
}

func (p *peer) HeartbeatScheduler(ch chan<- worker.Task) {
	clonedRegion := new(metapb.Region)
	err := util.CloneMsg(p.Region(), clonedRegion)
	if err != nil {
		return
	}
	ch <- &runner.SchedulerRegionHeartbeatTask{
		Region:          clonedRegion,
		Peer:            p.Meta,
		PendingPeers:    p.CollectPendingPeers(),
		ApproximateSize: p.ApproximateSize,
	}
}

func (p *peer) sendRaftMessage(msg eraftpb.Message, trans Transport) error {
	sendMsg := new(rspb.RaftMessage)
	sendMsg.RegionId = p.regionId
	// 设置当前 epoch
	sendMsg.RegionEpoch = &metapb.RegionEpoch{
		ConfVer: p.Region().RegionEpoch.ConfVer,
		Version: p.Region().RegionEpoch.Version,
	}

	fromPeer := *p.Meta
	toPeer := p.getPeerFromCache(msg.To)
	if toPeer == nil {
		return fmt.Errorf("failed to lookup recipient peer %v in region %v", msg.To, p.regionId)
	}
	log.Debugf("%v, send raft msg %v from %v to %v", p.Tag, msg.MsgType, fromPeer, toPeer)

	sendMsg.FromPeer = &fromPeer
	sendMsg.ToPeer = toPeer

	// 可能有两种情况：
	// 1. 目标 peer 已存在但尚未与 leader 建立通信
	// 2. 目标 peer 因成员变更或 region 分裂而新添加，但尚未创建
	// 对于这两种情况，region 的 start key 和 end key 会附加在 RequestVote 和
	// Heartbeat 消息中，以便该 peer 的 store 在收到这些消息时检查是否需要
	// 创建新的 peer，或者等待待处理的 region 分裂稍后执行。
	if p.peerStorage.isInitialized() && util.IsInitialMsg(&msg) {
		sendMsg.StartKey = append([]byte{}, p.Region().StartKey...)
		sendMsg.EndKey = append([]byte{}, p.Region().EndKey...)
	}
	sendMsg.Message = &msg
	return trans.Send(sendMsg)
}
