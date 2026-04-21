package util

import (
	"bytes"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	"github.com/pingcap/errors"
)

const RaftInvalidIndex uint64 = 0
const InvalidID uint64 = 0

// IsInitialMsg 检查 `msg` 是否可以用于初始化一个新的 peer。
// 有两种情况：
// 1. 目标 peer 已经存在，但尚未与 leader 建立通信
// 2. 目标 peer 是由于成员变更或 region 分裂而新增的，但尚未被创建
// 对于这两种情况，RequestVote 和 Heartbeat 消息中都会附带 region 的 start key 和 end key，
// 以便该 peer 所在的 store 在收到这些消息时判断是创建新的 peer，还是等待挂起的 region 分裂稍后执行。
func IsInitialMsg(msg *eraftpb.Message) bool {
	return msg.MsgType == eraftpb.MessageType_MsgRequestVote ||
		// 该 peer 对 leader 来说是未知的，可能存在也可能不存在。
		(msg.MsgType == eraftpb.MessageType_MsgHeartbeat && msg.Commit == RaftInvalidIndex)
}

// / 检查 key 是否在 region 范围 [`start_key`, `end_key`) 内。
func CheckKeyInRegion(key []byte, region *metapb.Region) error {
	if bytes.Compare(key, region.StartKey) >= 0 && (len(region.EndKey) == 0 || bytes.Compare(key, region.EndKey) < 0) {
		return nil
	} else {
		return &ErrKeyNotInRegion{Key: key, Region: region}
	}
}

// / 检查 key 是否在 region 范围 (`start_key`, `end_key`) 内（不含两端）。
func CheckKeyInRegionExclusive(key []byte, region *metapb.Region) error {
	if bytes.Compare(region.StartKey, key) < 0 && (len(region.EndKey) == 0 || bytes.Compare(key, region.EndKey) < 0) {
		return nil
	} else {
		return &ErrKeyNotInRegion{Key: key, Region: region}
	}
}

// / 检查 key 是否在 region 范围 [`start_key`, `end_key`] 内（含两端）。
func CheckKeyInRegionInclusive(key []byte, region *metapb.Region) error {
	if bytes.Compare(key, region.StartKey) >= 0 && (len(region.EndKey) == 0 || bytes.Compare(key, region.EndKey) <= 0) {
		return nil
	} else {
		return &ErrKeyNotInRegion{Key: key, Region: region}
	}
}

// / 检查 epoch 是否比 checkEpoch 更旧。
func IsEpochStale(epoch *metapb.RegionEpoch, checkEpoch *metapb.RegionEpoch) bool {
	return epoch.Version < checkEpoch.Version || epoch.ConfVer < checkEpoch.ConfVer
}

func IsVoteMessage(msg *eraftpb.Message) bool {
	tp := msg.GetMsgType()
	return tp == eraftpb.MessageType_MsgRequestVote
}

// IsFirstVoteMessage 检查 `msg` 是否是第一条投票消息。
// 当收到消息但 `Store::region_peers` 中不存在对应的 region，且该 region 与其他 region 有重叠时使用。
// 在这种情况下，应该将 `msg` 放入 `pending_votes` 中，而不是创建新的 peer。
func IsFirstVoteMessage(msg *eraftpb.Message) bool {
	return IsVoteMessage(msg) && msg.Term == meta.RaftInitLogTerm+1
}

func CheckRegionEpoch(req *raft_cmdpb.RaftCmdRequest, region *metapb.Region, includeRegion bool) error {
	checkVer, checkConfVer := false, false
	if req.AdminRequest == nil {
		checkVer = true
	} else {
		switch req.AdminRequest.CmdType {
		case raft_cmdpb.AdminCmdType_CompactLog, raft_cmdpb.AdminCmdType_InvalidAdmin:
		case raft_cmdpb.AdminCmdType_ChangePeer:
			checkConfVer = true
		case raft_cmdpb.AdminCmdType_Split, raft_cmdpb.AdminCmdType_TransferLeader:
			checkVer = true
			checkConfVer = true
		}
	}

	if !checkVer && !checkConfVer {
		return nil
	}

	if req.Header == nil {
		return fmt.Errorf("missing header!")
	}

	if req.Header.RegionEpoch == nil {
		return fmt.Errorf("missing epoch!")
	}

	fromEpoch := req.Header.RegionEpoch
	currentEpoch := region.RegionEpoch

	// 我们必须严格检查 epoch 以避免 key not in region 错误。
	//
	// 一个启用了 merge 的 3 节点 TiKV 集群，在提交 merge 后，TiKV A
	// 向 TiDB 返回一个 epoch not match 错误，其中包含最新的目标 Region 信息，
	// TiDB 更新其 region 缓存并向 TiKV B 发送请求，
	// 而 TiKV B 尚未应用 commit merge。由于请求中的 region epoch
	// 高于 TiKV B 的 epoch，该请求必须因 epoch 不匹配而被拒绝，
	// 这样就不会在过期的快照上读取数据，从而避免 KeyNotInRegion 错误。
	if (checkConfVer && fromEpoch.ConfVer != currentEpoch.ConfVer) ||
		(checkVer && fromEpoch.Version != currentEpoch.Version) {
		log.Debugf("epoch not match, region id %v, from epoch %v, current epoch %v",
			region.Id, fromEpoch, currentEpoch)

		regions := []*metapb.Region{}
		if includeRegion {
			regions = []*metapb.Region{region}
		}
		return &ErrEpochNotMatch{Message: fmt.Sprintf("current epoch of region %v is %v, but you sent %v",
			region.Id, currentEpoch, fromEpoch), Regions: regions}
	}

	return nil
}

func FindPeer(region *metapb.Region, storeID uint64) *metapb.Peer {
	for _, peer := range region.Peers {
		if peer.StoreId == storeID {
			return peer
		}
	}
	return nil
}

func RemovePeer(region *metapb.Region, storeID uint64) *metapb.Peer {
	for i, peer := range region.Peers {
		if peer.StoreId == storeID {
			region.Peers = append(region.Peers[:i], region.Peers[i+1:]...)
			return peer
		}
	}
	return nil
}

func ConfStateFromRegion(region *metapb.Region) (confState eraftpb.ConfState) {
	for _, p := range region.Peers {
		confState.Nodes = append(confState.Nodes, p.GetId())
	}
	return
}

func CheckStoreID(req *raft_cmdpb.RaftCmdRequest, storeID uint64) error {
	peer := req.Header.Peer
	if peer.StoreId == storeID {
		return nil
	}
	return errors.Errorf("store not match %d %d", peer.StoreId, storeID)
}

func CheckTerm(req *raft_cmdpb.RaftCmdRequest, term uint64) error {
	header := req.Header
	if header.Term == 0 || term <= header.Term+1 {
		return nil
	}
	// 如果 header 的 term 比当前 term 落后 2 个版本，
	// 说明 leadership 可能已经发生了变更。
	return &ErrStaleCommand{}
}

func CheckPeerID(req *raft_cmdpb.RaftCmdRequest, peerID uint64) error {
	peer := req.Header.Peer
	if peer.Id == peerID {
		return nil
	}
	return errors.Errorf("mismatch peer id %d != %d", peer.Id, peerID)
}

func CloneMsg(origin, cloned proto.Message) error {
	data, err := proto.Marshal(origin)
	if err != nil {
		return err
	}
	return proto.Unmarshal(data, cloned)
}

func SafeCopy(b []byte) []byte {
	return append([]byte{}, b...)
}

func PeerEqual(l, r *metapb.Peer) bool {
	return l.Id == r.Id && l.StoreId == r.StoreId
}

func RegionEqual(l, r *metapb.Region) bool {
	if l == nil || r == nil {
		return false
	}
	return l.Id == r.Id && l.RegionEpoch.Version == r.RegionEpoch.Version && l.RegionEpoch.ConfVer == r.RegionEpoch.ConfVer
}
