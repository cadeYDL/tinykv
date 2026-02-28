// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/schedulerpb"
)

// RegionInfo 记录 region 的详细信息。
// 创建后只读。
type RegionInfo struct {
	meta            *metapb.Region
	learners        []*metapb.Peer
	voters          []*metapb.Peer
	leader          *metapb.Peer
	pendingPeers    []*metapb.Peer
	approximateSize int64
}

// NewRegionInfo 使用 region 的元数据和 leader peer 创建 RegionInfo。
func NewRegionInfo(region *metapb.Region, leader *metapb.Peer, opts ...RegionCreateOption) *RegionInfo {
	regionInfo := &RegionInfo{
		meta:   region,
		leader: leader,
	}

	for _, opt := range opts {
		opt(regionInfo)
	}
	classifyVoterAndLearner(regionInfo)
	return regionInfo
}

// classifyVoterAndLearner 将 peer 中的 voter 和 learner 分类到不同的切片中。
func classifyVoterAndLearner(region *RegionInfo) {
	voters := make([]*metapb.Peer, 0, len(region.meta.Peers))
	for _, p := range region.meta.Peers {
		voters = append(voters, p)
	}
	region.voters = voters
}

// EmptyRegionApproximateSize 是空 region 的近似大小
// （心跳大小 <= 1MB）。
const EmptyRegionApproximateSize = 1

// RegionFromHeartbeat 从 region 心跳构造一个 Region。
func RegionFromHeartbeat(heartbeat *schedulerpb.RegionHeartbeatRequest) *RegionInfo {
	// 将单位转换为 MB。
	// 如果 region 为空或小于 1MB，则使用 1MB。
	regionSize := heartbeat.GetApproximateSize() / (1 << 20)
	if regionSize < EmptyRegionApproximateSize {
		regionSize = EmptyRegionApproximateSize
	}

	region := &RegionInfo{
		meta:            heartbeat.GetRegion(),
		leader:          heartbeat.GetLeader(),
		pendingPeers:    heartbeat.GetPendingPeers(),
		approximateSize: int64(regionSize),
	}

	classifyVoterAndLearner(region)
	return region
}

// Clone 返回当前 regionInfo 的副本。
func (r *RegionInfo) Clone(opts ...RegionCreateOption) *RegionInfo {
	pendingPeers := make([]*metapb.Peer, 0, len(r.pendingPeers))
	for _, peer := range r.pendingPeers {
		pendingPeers = append(pendingPeers, proto.Clone(peer).(*metapb.Peer))
	}

	region := &RegionInfo{
		meta:            proto.Clone(r.meta).(*metapb.Region),
		leader:          proto.Clone(r.leader).(*metapb.Peer),
		pendingPeers:    pendingPeers,
		approximateSize: r.approximateSize,
	}

	for _, opt := range opts {
		opt(region)
	}
	classifyVoterAndLearner(region)
	return region
}

// GetLearners 返回 learner 列表。
func (r *RegionInfo) GetLearners() []*metapb.Peer {
	return r.learners
}

// GetVoters 返回 voter 列表。
func (r *RegionInfo) GetVoters() []*metapb.Peer {
	return r.voters
}

// GetPeer 返回具有指定 peer id 的 peer。
func (r *RegionInfo) GetPeer(peerID uint64) *metapb.Peer {
	for _, peer := range r.meta.GetPeers() {
		if peer.GetId() == peerID {
			return peer
		}
	}
	return nil
}

// GetDownLearner 返回具有指定 peer id 的宕机 learner。
func (r *RegionInfo) GetDownLearner(peerID uint64) *metapb.Peer {
	return nil
}

// GetPendingPeer 返回具有指定 peer id 的 pending peer。
func (r *RegionInfo) GetPendingPeer(peerID uint64) *metapb.Peer {
	for _, peer := range r.pendingPeers {
		if peer.GetId() == peerID {
			return peer
		}
	}
	return nil
}

// GetPendingVoter 返回具有指定 peer id 的 pending voter。
func (r *RegionInfo) GetPendingVoter(peerID uint64) *metapb.Peer {
	for _, peer := range r.pendingPeers {
		if peer.GetId() == peerID {
			return peer
		}
	}
	return nil
}

// GetPendingLearner 返回具有指定 peer id 的 pending learner peer。
func (r *RegionInfo) GetPendingLearner(peerID uint64) *metapb.Peer {
	return nil
}

// GetStorePeer 返回指定 store 中的 peer。
func (r *RegionInfo) GetStorePeer(storeID uint64) *metapb.Peer {
	for _, peer := range r.meta.GetPeers() {
		if peer.GetStoreId() == storeID {
			return peer
		}
	}
	return nil
}

// GetStoreVoter 返回指定 store 中的 voter。
func (r *RegionInfo) GetStoreVoter(storeID uint64) *metapb.Peer {
	for _, peer := range r.voters {
		if peer.GetStoreId() == storeID {
			return peer
		}
	}
	return nil
}

// GetStoreLearner 返回指定 store 中的 learner peer。
func (r *RegionInfo) GetStoreLearner(storeID uint64) *metapb.Peer {
	for _, peer := range r.learners {
		if peer.GetStoreId() == storeID {
			return peer
		}
	}
	return nil
}

// GetStoreIds 返回一个 map 表示 region 的分布情况。
func (r *RegionInfo) GetStoreIds() map[uint64]struct{} {
	peers := r.meta.GetPeers()
	stores := make(map[uint64]struct{}, len(peers))
	for _, peer := range peers {
		stores[peer.GetStoreId()] = struct{}{}
	}
	return stores
}

// GetFollowers 返回一个 map 表示 follower peer 的分布情况。
func (r *RegionInfo) GetFollowers() map[uint64]*metapb.Peer {
	peers := r.GetVoters()
	followers := make(map[uint64]*metapb.Peer, len(peers))
	for _, peer := range peers {
		if r.leader == nil || r.leader.GetId() != peer.GetId() {
			followers[peer.GetStoreId()] = peer
		}
	}
	return followers
}

// GetFollower 随机返回一个 follower peer。
func (r *RegionInfo) GetFollower() *metapb.Peer {
	for _, peer := range r.GetVoters() {
		if r.leader == nil || r.leader.GetId() != peer.GetId() {
			return peer
		}
	}
	return nil
}

// GetDiffFollowers 返回不与另一个指定 region 的任何 follower 位于同一 store 的 follower。
func (r *RegionInfo) GetDiffFollowers(other *RegionInfo) []*metapb.Peer {
	res := make([]*metapb.Peer, 0, len(r.meta.Peers))
	for _, p := range r.GetFollowers() {
		diff := true
		for _, o := range other.GetFollowers() {
			if p.GetStoreId() == o.GetStoreId() {
				diff = false
				break
			}
		}
		if diff {
			res = append(res, p)
		}
	}
	return res
}

// GetID 返回 region 的 ID。
func (r *RegionInfo) GetID() uint64 {
	return r.meta.GetId()
}

// GetMeta 返回 region 的元信息。
func (r *RegionInfo) GetMeta() *metapb.Region {
	return r.meta
}

// GetApproximateSize 返回 region 的近似大小。
func (r *RegionInfo) GetApproximateSize() int64 {
	return r.approximateSize
}

// GetPendingPeers 返回 region 的 pending peer 列表。
func (r *RegionInfo) GetPendingPeers() []*metapb.Peer {
	return r.pendingPeers
}

// GetLeader 返回 region 的 leader。
func (r *RegionInfo) GetLeader() *metapb.Peer {
	return r.leader
}

// GetStartKey 返回 region 的起始 key。
func (r *RegionInfo) GetStartKey() []byte {
	return r.meta.StartKey
}

// GetEndKey 返回 region 的结束 key。
func (r *RegionInfo) GetEndKey() []byte {
	return r.meta.EndKey
}

// GetPeers 返回 region 的 peer 列表。
func (r *RegionInfo) GetPeers() []*metapb.Peer {
	return r.meta.GetPeers()
}

// GetRegionEpoch 返回 region 的 epoch。
func (r *RegionInfo) GetRegionEpoch() *metapb.RegionEpoch {
	return r.meta.RegionEpoch
}

// regionMap 封装 map[uint64]*core.RegionInfo 并支持随机选择一个 region。
type regionMap struct {
	m         map[uint64]*RegionInfo
	totalSize int64
	totalKeys int64
}

func newRegionMap() *regionMap {
	return &regionMap{
		m: make(map[uint64]*RegionInfo),
	}
}

func (rm *regionMap) Len() int {
	if rm == nil {
		return 0
	}
	return len(rm.m)
}

func (rm *regionMap) Get(id uint64) *RegionInfo {
	if rm == nil {
		return nil
	}
	if r, ok := rm.m[id]; ok {
		return r
	}
	return nil
}

func (rm *regionMap) Put(region *RegionInfo) {
	if old, ok := rm.m[region.GetID()]; ok {
		rm.totalSize -= old.approximateSize
	}
	rm.m[region.GetID()] = region
	rm.totalSize += region.approximateSize
}

func (rm *regionMap) Delete(id uint64) {
	if rm == nil {
		return
	}
	if old, ok := rm.m[id]; ok {
		delete(rm.m, id)
		rm.totalSize -= old.approximateSize
	}
}

func (rm *regionMap) TotalSize() int64 {
	if rm.Len() == 0 {
		return 0
	}
	return rm.totalSize
}

// regionSubTree 用于管理不同类型的 region。
type regionSubTree struct {
	*regionTree
	totalSize int64
}

func newRegionSubTree() *regionSubTree {
	return &regionSubTree{
		regionTree: newRegionTree(),
		totalSize:  0,
	}
}

func (rst *regionSubTree) TotalSize() int64 {
	if rst.length() == 0 {
		return 0
	}
	return rst.totalSize
}

func (rst *regionSubTree) scanRanges() []*RegionInfo {
	if rst.length() == 0 {
		return nil
	}
	var res []*RegionInfo
	rst.scanRange([]byte(""), func(region *RegionInfo) bool {
		res = append(res, region)
		return true
	})
	return res
}

func (rst *regionSubTree) update(region *RegionInfo) {
	if r := rst.find(region); r != nil {
		rst.totalSize += region.approximateSize - r.region.approximateSize
		r.region = region
		return
	}
	rst.totalSize += region.approximateSize
	rst.regionTree.update(region)
}

func (rst *regionSubTree) remove(region *RegionInfo) {
	if rst.length() == 0 {
		return
	}
	rst.regionTree.remove(region)
}

func (rst *regionSubTree) length() int {
	if rst == nil {
		return 0
	}
	return rst.regionTree.length()
}

func (rst *regionSubTree) RandomRegion(startKey, endKey []byte) *RegionInfo {
	if rst.length() == 0 {
		return nil
	}
	return rst.regionTree.RandomRegion(startKey, endKey)
}

// RegionsInfo 用于导出
type RegionsInfo struct {
	tree         *regionTree
	regions      *regionMap                // regionID -> regionInfo
	leaders      map[uint64]*regionSubTree // storeID -> regionSubTree
	followers    map[uint64]*regionSubTree // storeID -> regionSubTree
	learners     map[uint64]*regionSubTree // storeID -> regionSubTree
	pendingPeers map[uint64]*regionSubTree // storeID -> regionSubTree
}

// NewRegionsInfo 创建包含 tree、regions、leaders 和 followers 的 RegionsInfo
func NewRegionsInfo() *RegionsInfo {
	return &RegionsInfo{
		tree:         newRegionTree(),
		regions:      newRegionMap(),
		leaders:      make(map[uint64]*regionSubTree),
		followers:    make(map[uint64]*regionSubTree),
		learners:     make(map[uint64]*regionSubTree),
		pendingPeers: make(map[uint64]*regionSubTree),
	}
}

// GetRegion 通过 regionID 返回 RegionInfo
func (r *RegionsInfo) GetRegion(regionID uint64) *RegionInfo {
	region := r.regions.Get(regionID)
	if region == nil {
		return nil
	}
	return region
}

// SetRegion 通过 regionID 设置 RegionInfo
func (r *RegionsInfo) SetRegion(region *RegionInfo) []*RegionInfo {
	if origin := r.regions.Get(region.GetID()); origin != nil {
		r.RemoveRegion(origin)
	}
	return r.AddRegion(region)
}

// Length 返回 RegionsInfo 的长度
func (r *RegionsInfo) Length() int {
	return r.regions.Len()
}

// TreeLength 返回 RegionsInfo tree 的长度（目前仅用于测试）
func (r *RegionsInfo) TreeLength() int {
	return r.tree.length()
}

// GetOverlaps 返回与指定 region 范围重叠的 region。
func (r *RegionsInfo) GetOverlaps(region *RegionInfo) []*RegionInfo {
	return r.tree.getOverlaps(region)
}

// AddRegion 将 RegionInfo 添加到 regionTree 和 regionMap，同时根据 region peer 更新 leaders 和 followers
func (r *RegionsInfo) AddRegion(region *RegionInfo) []*RegionInfo {
	// 添加到 tree 和 regions。
	overlaps := r.tree.update(region)
	for _, item := range overlaps {
		r.RemoveRegion(r.GetRegion(item.GetID()))
	}

	r.regions.Put(region)

	// 添加到 leaders 和 followers。
	for _, peer := range region.GetVoters() {
		storeID := peer.GetStoreId()
		if peer.GetId() == region.leader.GetId() {
			// 将 leader peer 添加到 leaders。
			store, ok := r.leaders[storeID]
			if !ok {
				store = newRegionSubTree()
				r.leaders[storeID] = store
			}
			store.update(region)
		} else {
			// 将 follower peer 添加到 followers。
			store, ok := r.followers[storeID]
			if !ok {
				store = newRegionSubTree()
				r.followers[storeID] = store
			}
			store.update(region)
		}
	}

	// 添加到 learners。
	for _, peer := range region.GetLearners() {
		storeID := peer.GetStoreId()
		store, ok := r.learners[storeID]
		if !ok {
			store = newRegionSubTree()
			r.learners[storeID] = store
		}
		store.update(region)
	}

	for _, peer := range region.pendingPeers {
		storeID := peer.GetStoreId()
		store, ok := r.pendingPeers[storeID]
		if !ok {
			store = newRegionSubTree()
			r.pendingPeers[storeID] = store
		}
		store.update(region)
	}

	return overlaps
}

// RemoveRegion 从 regionTree 和 regionMap 中移除 RegionInfo
func (r *RegionsInfo) RemoveRegion(region *RegionInfo) {
	// 从 tree 和 regions 中移除。
	r.tree.remove(region)
	r.regions.Delete(region.GetID())
	// 从 leaders 和 followers 中移除。
	for _, peer := range region.meta.GetPeers() {
		storeID := peer.GetStoreId()
		r.leaders[storeID].remove(region)
		r.followers[storeID].remove(region)
		r.learners[storeID].remove(region)
		r.pendingPeers[storeID].remove(region)
	}
}

// SearchRegion 从 regionTree 中搜索 RegionInfo
func (r *RegionsInfo) SearchRegion(regionKey []byte) *RegionInfo {
	region := r.tree.search(regionKey)
	if region == nil {
		return nil
	}
	return r.GetRegion(region.GetID())
}

// SearchPrevRegion 从 regionTree 中搜索前一个 RegionInfo
func (r *RegionsInfo) SearchPrevRegion(regionKey []byte) *RegionInfo {
	region := r.tree.searchPrev(regionKey)
	if region == nil {
		return nil
	}
	return r.GetRegion(region.GetID())
}

// GetRegions 从 regionMap 获取所有 RegionInfo
func (r *RegionsInfo) GetRegions() []*RegionInfo {
	regions := make([]*RegionInfo, 0, r.regions.Len())
	for _, region := range r.regions.m {
		regions = append(regions, region)
	}
	return regions
}

// GetStoreRegions 获取给定 storeID 的所有 RegionInfo
func (r *RegionsInfo) GetStoreRegions(storeID uint64) []*RegionInfo {
	regions := make([]*RegionInfo, 0, r.GetStoreLeaderCount(storeID)+r.GetStoreFollowerCount(storeID))
	if leaders, ok := r.leaders[storeID]; ok {
		for _, region := range leaders.scanRanges() {
			regions = append(regions, region)
		}
	}
	if followers, ok := r.followers[storeID]; ok {
		for _, region := range followers.scanRanges() {
			regions = append(regions, region)
		}
	}
	return regions
}

// GetStoreLeaderRegionSize 获取 store 的 leader region 总大小
func (r *RegionsInfo) GetStoreLeaderRegionSize(storeID uint64) int64 {
	return r.leaders[storeID].TotalSize()
}

// GetStoreFollowerRegionSize 获取 store 的 follower region 总大小
func (r *RegionsInfo) GetStoreFollowerRegionSize(storeID uint64) int64 {
	return r.followers[storeID].TotalSize()
}

// GetStoreLearnerRegionSize 获取 store 的 learner region 总大小
func (r *RegionsInfo) GetStoreLearnerRegionSize(storeID uint64) int64 {
	return r.learners[storeID].TotalSize()
}

// GetStoreRegionSize 获取 store 的 region 总大小
func (r *RegionsInfo) GetStoreRegionSize(storeID uint64) int64 {
	return r.GetStoreLeaderRegionSize(storeID) + r.GetStoreFollowerRegionSize(storeID) + r.GetStoreLearnerRegionSize(storeID)
}

// GetMetaRegions 从 regionMap 获取 metapb.Region 集合
func (r *RegionsInfo) GetMetaRegions() []*metapb.Region {
	regions := make([]*metapb.Region, 0, r.regions.Len())
	for _, region := range r.regions.m {
		regions = append(regions, proto.Clone(region.meta).(*metapb.Region))
	}
	return regions
}

// GetRegionCount 获取 regionMap 中 RegionInfo 的总数
func (r *RegionsInfo) GetRegionCount() int {
	return r.regions.Len()
}

// GetStoreRegionCount 通过 storeID 获取 store 的 leader 和 follower RegionInfo 总数
func (r *RegionsInfo) GetStoreRegionCount(storeID uint64) int {
	return r.GetStoreLeaderCount(storeID) + r.GetStoreFollowerCount(storeID) + r.GetStoreLearnerCount(storeID)
}

// GetStorePendingPeerCount 获取包含 pending peer 的 store region 总数
func (r *RegionsInfo) GetStorePendingPeerCount(storeID uint64) int {
	return r.pendingPeers[storeID].length()
}

// GetStoreLeaderCount 获取 store 的 leader RegionInfo 总数
func (r *RegionsInfo) GetStoreLeaderCount(storeID uint64) int {
	return r.leaders[storeID].length()
}

// GetStoreFollowerCount 获取 store 的 follower RegionInfo 总数
func (r *RegionsInfo) GetStoreFollowerCount(storeID uint64) int {
	return r.followers[storeID].length()
}

// GetStoreLearnerCount 获取 store 的 learner RegionInfo 总数
func (r *RegionsInfo) GetStoreLearnerCount(storeID uint64) int {
	return r.learners[storeID].length()
}

// RandRegion 随机获取一个 region
func (r *RegionsInfo) RandRegion(opts ...RegionOption) *RegionInfo {
	return randRegion(r.tree, opts...)
}

// RandPendingRegion 随机获取 store 中包含 pending peer 的 region。
func (r *RegionsInfo) RandPendingRegion(storeID uint64, opts ...RegionOption) *RegionInfo {
	return randRegion(r.pendingPeers[storeID], opts...)
}

// RandLeaderRegion 随机获取 store 的 leader region。
func (r *RegionsInfo) RandLeaderRegion(storeID uint64, opts ...RegionOption) *RegionInfo {
	return randRegion(r.leaders[storeID], opts...)
}

// RandFollowerRegion 随机获取 store 的 follower region。
func (r *RegionsInfo) RandFollowerRegion(storeID uint64, opts ...RegionOption) *RegionInfo {
	return randRegion(r.followers[storeID], opts...)
}

// GetPendingRegionsWithLock 通过 storeID 返回 pending region 子树
func (r *RegionsInfo) GetPendingRegionsWithLock(storeID uint64, callback func(RegionsContainer)) {
	callback(r.pendingPeers[storeID])
}

// GetLeadersWithLock 通过 storeID 返回 leader 子树
func (r *RegionsInfo) GetLeadersWithLock(storeID uint64, callback func(RegionsContainer)) {
	callback(r.leaders[storeID])
}

// GetFollowersWithLock 通过 storeID 返回 follower 子树
func (r *RegionsInfo) GetFollowersWithLock(storeID uint64, callback func(RegionsContainer)) {
	callback(r.followers[storeID])
}

// GetLeader 通过 storeID 和 regionID 返回 leader RegionInfo（目前仅用于测试）
func (r *RegionsInfo) GetLeader(storeID uint64, region *RegionInfo) *RegionInfo {
	return r.leaders[storeID].find(region).region
}

// GetFollower 通过 storeID 和 regionID 返回 follower RegionInfo（目前仅用于测试）
func (r *RegionsInfo) GetFollower(storeID uint64, region *RegionInfo) *RegionInfo {
	return r.followers[storeID].find(region).region
}

// ScanRange 扫描与 [start key, end key) 相交的 region，最多返回 `limit` 个 region。
// limit <= 0 表示没有限制。
func (r *RegionsInfo) ScanRange(startKey, endKey []byte, limit int) []*RegionInfo {
	var res []*RegionInfo
	r.tree.scanRange(startKey, func(region *RegionInfo) bool {
		if len(endKey) > 0 && bytes.Compare(region.GetStartKey(), endKey) >= 0 {
			return false
		}
		if limit > 0 && len(res) >= limit {
			return false
		}
		res = append(res, r.GetRegion(region.GetID()))
		return true
	})
	return res
}

// GetAverageRegionSize 返回 region 的平均近似大小。
func (r *RegionsInfo) GetAverageRegionSize() int64 {
	if r.regions.Len() == 0 {
		return 0
	}
	return r.regions.TotalSize() / int64(r.regions.Len())
}

const randomRegionMaxRetry = 10

// RegionsContainer 是存储 region 的容器。
type RegionsContainer interface {
	RandomRegion(startKey, endKey []byte) *RegionInfo
}

func randRegion(regions RegionsContainer, opts ...RegionOption) *RegionInfo {
	for i := 0; i < randomRegionMaxRetry; i++ {
		region := regions.RandomRegion(nil, nil)
		if region == nil {
			return nil
		}
		isSelect := true
		for _, opt := range opts {
			if !opt(region) {
				isSelect = false
				break
			}
		}
		if isSelect {
			return region
		}
	}
	return nil
}

// DiffRegionPeersInfo 返回两个 RegionInfo 之间 peer 信息的差异
func DiffRegionPeersInfo(origin *RegionInfo, other *RegionInfo) string {
	var ret []string
	for _, a := range origin.meta.Peers {
		both := false
		for _, b := range other.meta.Peers {
			if reflect.DeepEqual(a, b) {
				both = true
				break
			}
		}
		if !both {
			ret = append(ret, fmt.Sprintf("Remove peer:{%v}", a))
		}
	}
	for _, b := range other.meta.Peers {
		both := false
		for _, a := range origin.meta.Peers {
			if reflect.DeepEqual(a, b) {
				both = true
				break
			}
		}
		if !both {
			ret = append(ret, fmt.Sprintf("Add peer:{%v}", b))
		}
	}
	return strings.Join(ret, ",")
}

// DiffRegionKeyInfo 返回两个 RegionInfo 之间 key 信息的差异
func DiffRegionKeyInfo(origin *RegionInfo, other *RegionInfo) string {
	var ret []string
	if !bytes.Equal(origin.meta.StartKey, other.meta.StartKey) {
		ret = append(ret, fmt.Sprintf("StartKey Changed:{%s} -> {%s}", HexRegionKey(origin.meta.StartKey), HexRegionKey(other.meta.StartKey)))
	} else {
		ret = append(ret, fmt.Sprintf("StartKey:{%s}", HexRegionKey(origin.meta.StartKey)))
	}
	if !bytes.Equal(origin.meta.EndKey, other.meta.EndKey) {
		ret = append(ret, fmt.Sprintf("EndKey Changed:{%s} -> {%s}", HexRegionKey(origin.meta.EndKey), HexRegionKey(other.meta.EndKey)))
	} else {
		ret = append(ret, fmt.Sprintf("EndKey:{%s}", HexRegionKey(origin.meta.EndKey)))
	}

	return strings.Join(ret, ", ")
}

// HexRegionKey 将 region key 转换为十六进制格式。用于在日志中格式化 region。
func HexRegionKey(key []byte) []byte {
	return []byte(strings.ToUpper(hex.EncodeToString(key)))
}

// RegionToHexMeta 将 region 元数据的 key 转换为十六进制格式。用于在日志中格式化 region。
func RegionToHexMeta(meta *metapb.Region) HexRegionMeta {
	if meta == nil {
		return HexRegionMeta{}
	}
	meta = proto.Clone(meta).(*metapb.Region)
	meta.StartKey = HexRegionKey(meta.StartKey)
	meta.EndKey = HexRegionKey(meta.EndKey)
	return HexRegionMeta{meta}
}

// HexRegionMeta 是十六进制格式的 region 元数据。用于在日志中格式化 region。
type HexRegionMeta struct {
	*metapb.Region
}

func (h HexRegionMeta) String() string {
	return strings.TrimSpace(proto.CompactTextString(h.Region))
}

// RegionsToHexMeta 将多个 region 的元数据 key 转换为十六进制格式。用于在日志中格式化 region。
func RegionsToHexMeta(regions []*metapb.Region) HexRegionsMeta {
	hexRegionMetas := make([]*metapb.Region, len(regions))
	for i, region := range regions {
		meta := proto.Clone(region).(*metapb.Region)
		meta.StartKey = HexRegionKey(meta.StartKey)
		meta.EndKey = HexRegionKey(meta.EndKey)

		hexRegionMetas[i] = meta
	}
	return HexRegionsMeta(hexRegionMetas)
}

// HexRegionsMeta 是十六进制格式的 region 元数据切片。用于在日志中格式化 region。
type HexRegionsMeta []*metapb.Region

func (h HexRegionsMeta) String() string {
	var b strings.Builder
	for _, r := range h {
		b.WriteString(proto.CompactTextString(r))
	}
	return strings.TrimSpace(b.String())
}
