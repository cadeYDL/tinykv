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
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/schedulerpb"
	"github.com/pingcap/errcode"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// StoreInfo 包含关于 store 的信息。
type StoreInfo struct {
	meta  *metapb.Store
	stats *schedulerpb.StoreStats
	// Blocked 表示 store 被禁止进行负载均衡。
	blocked          bool
	leaderCount      int
	regionCount      int
	leaderSize       int64
	regionSize       int64
	pendingPeerCount int
	lastHeartbeatTS  time.Time
	leaderWeight     float64
	regionWeight     float64
	available        func() bool
}

// NewStoreInfo 使用元数据创建 StoreInfo。
func NewStoreInfo(store *metapb.Store, opts ...StoreCreateOption) *StoreInfo {
	storeInfo := &StoreInfo{
		meta:         store,
		stats:        &schedulerpb.StoreStats{},
		leaderWeight: 1.0,
		regionWeight: 1.0,
	}
	for _, opt := range opts {
		opt(storeInfo)
	}
	return storeInfo
}

// Clone 创建当前 StoreInfo 的副本。
func (s *StoreInfo) Clone(opts ...StoreCreateOption) *StoreInfo {
	meta := proto.Clone(s.meta).(*metapb.Store)
	store := &StoreInfo{
		meta:             meta,
		stats:            s.stats,
		blocked:          s.blocked,
		leaderCount:      s.leaderCount,
		regionCount:      s.regionCount,
		leaderSize:       s.leaderSize,
		regionSize:       s.regionSize,
		pendingPeerCount: s.pendingPeerCount,
		lastHeartbeatTS:  s.lastHeartbeatTS,
		leaderWeight:     s.leaderWeight,
		regionWeight:     s.regionWeight,
		available:        s.available,
	}

	for _, opt := range opts {
		opt(store)
	}
	return store
}

// IsBlocked 返回 store 是否被阻塞。
func (s *StoreInfo) IsBlocked() bool {
	return s.blocked
}

// IsAvailable 返回 store 的限制桶是否可用
func (s *StoreInfo) IsAvailable() bool {
	if s.available == nil {
		return true
	}
	return s.available()
}

// IsUp 检查 store 的状态是否为 Up。
func (s *StoreInfo) IsUp() bool {
	return s.GetState() == metapb.StoreState_Up
}

// IsOffline 检查 store 的状态是否为 Offline。
func (s *StoreInfo) IsOffline() bool {
	return s.GetState() == metapb.StoreState_Offline
}

// IsTombstone 检查 store 的状态是否为 Tombstone。
func (s *StoreInfo) IsTombstone() bool {
	return s.GetState() == metapb.StoreState_Tombstone
}

// DownTime 返回自上次心跳以来经过的时间。
func (s *StoreInfo) DownTime() time.Duration {
	return time.Since(s.GetLastHeartbeatTS())
}

// GetMeta 返回 store 的元信息。
func (s *StoreInfo) GetMeta() *metapb.Store {
	return s.meta
}

// GetState 返回 store 的状态。
func (s *StoreInfo) GetState() metapb.StoreState {
	return s.meta.GetState()
}

// GetAddress 返回 store 的地址。
func (s *StoreInfo) GetAddress() string {
	return s.meta.GetAddress()
}

// GetID 返回 store 的 ID。
func (s *StoreInfo) GetID() uint64 {
	return s.meta.GetId()
}

// GetStoreStats 返回 store 的统计信息。
func (s *StoreInfo) GetStoreStats() *schedulerpb.StoreStats {
	return s.stats
}

// GetCapacity 返回 store 的容量大小。
func (s *StoreInfo) GetCapacity() uint64 {
	return s.stats.GetCapacity()
}

// GetAvailable 返回 store 的可用大小。
func (s *StoreInfo) GetAvailable() uint64 {
	return s.stats.GetAvailable()
}

// GetUsedSize 返回 store 的已用大小。
func (s *StoreInfo) GetUsedSize() uint64 {
	return s.stats.GetUsedSize()
}

// IsBusy 返回 store 是否繁忙。
func (s *StoreInfo) IsBusy() bool {
	return s.stats.GetIsBusy()
}

// GetSendingSnapCount 返回 store 当前发送快照的数量。
func (s *StoreInfo) GetSendingSnapCount() uint32 {
	return s.stats.GetSendingSnapCount()
}

// GetReceivingSnapCount 返回 store 当前接收快照的数量。
func (s *StoreInfo) GetReceivingSnapCount() uint32 {
	return s.stats.GetReceivingSnapCount()
}

// GetApplyingSnapCount 返回 store 当前应用快照的数量。
func (s *StoreInfo) GetApplyingSnapCount() uint32 {
	return s.stats.GetApplyingSnapCount()
}

// GetStartTime 返回 store 的启动时间。
func (s *StoreInfo) GetStartTime() uint32 {
	return s.stats.GetStartTime()
}

// GetLeaderCount 返回 store 的 leader 数量。
func (s *StoreInfo) GetLeaderCount() int {
	return s.leaderCount
}

// GetRegionCount 返回 store 的 Region 数量。
func (s *StoreInfo) GetRegionCount() int {
	return s.regionCount
}

// GetLeaderSize 返回 store 的 leader 大小。
func (s *StoreInfo) GetLeaderSize() int64 {
	return s.leaderSize
}

// GetRegionSize 返回 store 的 Region 大小。
func (s *StoreInfo) GetRegionSize() int64 {
	return s.regionSize
}

// GetPendingPeerCount 返回 store 的 pending peer 数量。
func (s *StoreInfo) GetPendingPeerCount() int {
	return s.pendingPeerCount
}

// GetLeaderWeight 返回 store 的 leader 权重。
func (s *StoreInfo) GetLeaderWeight() float64 {
	return s.leaderWeight
}

// GetRegionWeight 返回 store 的 Region 权重。
func (s *StoreInfo) GetRegionWeight() float64 {
	return s.regionWeight
}

// GetLastHeartbeatTS 返回 store 的上次心跳时间戳。
func (s *StoreInfo) GetLastHeartbeatTS() time.Time {
	return s.lastHeartbeatTS
}

const minWeight = 1e-6

// StorageSize 返回 tikv 报告的 store 已用存储大小。
func (s *StoreInfo) StorageSize() uint64 {
	return s.GetUsedSize()
}

// AvailableRatio 是 store 的 freeSpace/capacity 比率。
func (s *StoreInfo) AvailableRatio() float64 {
	if s.GetCapacity() == 0 {
		return 0
	}
	return float64(s.GetAvailable()) / float64(s.GetCapacity())
}

// IsLowSpace 检查 store 是否空间不足。
func (s *StoreInfo) IsLowSpace(lowSpaceRatio float64) bool {
	return s.GetStoreStats() != nil && s.AvailableRatio() < 1-lowSpaceRatio
}

// ResourceCount 返回 store 中 leader/region 的数量。
func (s *StoreInfo) ResourceCount(kind ResourceKind) uint64 {
	switch kind {
	case LeaderKind:
		return uint64(s.GetLeaderCount())
	case RegionKind:
		return uint64(s.GetRegionCount())
	default:
		return 0
	}
}

// ResourceSize 返回 store 中 leader/region 的大小
func (s *StoreInfo) ResourceSize(kind ResourceKind) int64 {
	switch kind {
	case LeaderKind:
		return s.GetLeaderSize()
	case RegionKind:
		return s.GetRegionSize()
	default:
		return 0
	}
}

// ResourceWeight 返回评分中 leader/region 的权重
func (s *StoreInfo) ResourceWeight(kind ResourceKind) float64 {
	switch kind {
	case LeaderKind:
		leaderWeight := s.GetLeaderWeight()
		if leaderWeight <= 0 {
			return minWeight
		}
		return leaderWeight
	case RegionKind:
		regionWeight := s.GetRegionWeight()
		if regionWeight <= 0 {
			return minWeight
		}
		return regionWeight
	default:
		return 0
	}
}

// GetStartTS 返回启动时间戳。
func (s *StoreInfo) GetStartTS() time.Time {
	return time.Unix(int64(s.GetStartTime()), 0)
}

// GetUptime 返回运行时间。
func (s *StoreInfo) GetUptime() time.Duration {
	uptime := s.GetLastHeartbeatTS().Sub(s.GetStartTS())
	if uptime > 0 {
		return uptime
	}
	return 0
}

var (
	// 如果 store 的上次心跳是 storeDisconnectDuration 之前，该 store 将被标记为断开连接状态。
	// 该值应大于 tikv 的 store 心跳间隔（默认 10s）。
	storeDisconnectDuration = 20 * time.Second
	storeUnhealthDuration   = 10 * time.Minute
)

// IsDisconnected 检查 store 是否断开连接，这意味着 PD 短时间内未收到
// tikv 的 store 心跳，可能是由于进程重启或临时网络故障造成的。
func (s *StoreInfo) IsDisconnected() bool {
	return s.DownTime() > storeDisconnectDuration
}

// IsUnhealth 检查 store 是否不健康。
func (s *StoreInfo) IsUnhealth() bool {
	return s.DownTime() > storeUnhealthDuration
}

type storeNotFoundErr struct {
	storeID uint64
}

func (e storeNotFoundErr) Error() string {
	return fmt.Sprintf("store %v not found", e.storeID)
}

// NewStoreNotFoundErr 用于记录 store 未找到的日志
func NewStoreNotFoundErr(storeID uint64) errcode.ErrorCode {
	return errcode.NewNotFoundErr(storeNotFoundErr{storeID})
}

// StoresInfo 包含所有 store 的信息。
type StoresInfo struct {
	stores map[uint64]*StoreInfo
}

// NewStoresInfo 创建一个 storeID 到 StoreInfo 映射的 StoresInfo
func NewStoresInfo() *StoresInfo {
	return &StoresInfo{
		stores: make(map[uint64]*StoreInfo),
	}
}

// GetStore 返回具有指定 storeID 的 StoreInfo 副本。
func (s *StoresInfo) GetStore(storeID uint64) *StoreInfo {
	store, ok := s.stores[storeID]
	if !ok {
		return nil
	}
	return store
}

// TakeStore 返回具有指定 storeID 的原始 StoreInfo 的指针。
func (s *StoresInfo) TakeStore(storeID uint64) *StoreInfo {
	store, ok := s.stores[storeID]
	if !ok {
		return nil
	}
	return store
}

// SetStore 使用 storeID 设置 StoreInfo。
func (s *StoresInfo) SetStore(store *StoreInfo) {
	s.stores[store.GetID()] = store
}

// BlockStore 阻塞具有 storeID 的 StoreInfo。
func (s *StoresInfo) BlockStore(storeID uint64) errcode.ErrorCode {
	op := errcode.Op("store.block")
	store, ok := s.stores[storeID]
	if !ok {
		return op.AddTo(NewStoreNotFoundErr(storeID))
	}
	if store.IsBlocked() {
		return op.AddTo(StoreBlockedErr{StoreID: storeID})
	}
	s.stores[storeID] = store.Clone(SetStoreBlock())
	return nil
}

// UnblockStore 解除具有 storeID 的 StoreInfo 的阻塞。
func (s *StoresInfo) UnblockStore(storeID uint64) {
	store, ok := s.stores[storeID]
	if !ok {
		log.Fatal("store is unblocked, but it is not found",
			zap.Uint64("store-id", storeID))
	}
	s.stores[storeID] = store.Clone(SetStoreUnBlock())
}

// AttachAvailableFunc 将函数 f 附加到特定 store。
func (s *StoresInfo) AttachAvailableFunc(storeID uint64, f func() bool) {
	if store, ok := s.stores[storeID]; ok {
		s.stores[storeID] = store.Clone(SetAvailableFunc(f))
	}
}

// GetStores 获取完整的 StoreInfo 集合。
func (s *StoresInfo) GetStores() []*StoreInfo {
	stores := make([]*StoreInfo, 0, len(s.stores))
	for _, store := range s.stores {
		stores = append(stores, store)
	}
	return stores
}

// GetMetaStores 获取完整的 metapb.Store 集合。
func (s *StoresInfo) GetMetaStores() []*metapb.Store {
	stores := make([]*metapb.Store, 0, len(s.stores))
	for _, store := range s.stores {
		stores = append(stores, store.GetMeta())
	}
	return stores
}

// DeleteStore 从 store 中删除 tombstone 记录
func (s *StoresInfo) DeleteStore(store *StoreInfo) {
	delete(s.stores, store.GetID())
}

// GetStoreCount 返回 storeInfo 的总数。
func (s *StoresInfo) GetStoreCount() int {
	return len(s.stores)
}

// SetLeaderCount 为 storeInfo 设置 leader 数量。
func (s *StoresInfo) SetLeaderCount(storeID uint64, leaderCount int) {
	if store, ok := s.stores[storeID]; ok {
		s.stores[storeID] = store.Clone(SetLeaderCount(leaderCount))
	}
}

// SetRegionCount 为 storeInfo 设置 region 数量。
func (s *StoresInfo) SetRegionCount(storeID uint64, regionCount int) {
	if store, ok := s.stores[storeID]; ok {
		s.stores[storeID] = store.Clone(SetRegionCount(regionCount))
	}
}

// SetPendingPeerCount 为 storeInfo 设置 pending 数量。
func (s *StoresInfo) SetPendingPeerCount(storeID uint64, pendingPeerCount int) {
	if store, ok := s.stores[storeID]; ok {
		s.stores[storeID] = store.Clone(SetPendingPeerCount(pendingPeerCount))
	}
}

// SetLeaderSize 为 storeInfo 设置 leader 大小。
func (s *StoresInfo) SetLeaderSize(storeID uint64, leaderSize int64) {
	if store, ok := s.stores[storeID]; ok {
		s.stores[storeID] = store.Clone(SetLeaderSize(leaderSize))
	}
}

// SetRegionSize 为 storeInfo 设置 region 大小。
func (s *StoresInfo) SetRegionSize(storeID uint64, regionSize int64) {
	if store, ok := s.stores[storeID]; ok {
		s.stores[storeID] = store.Clone(SetRegionSize(regionSize))
	}
}

// UpdateStoreStatus 更新 store 的信息。
func (s *StoresInfo) UpdateStoreStatus(storeID uint64, leaderCount int, regionCount int, pendingPeerCount int, leaderSize int64, regionSize int64) {
	if store, ok := s.stores[storeID]; ok {
		newStore := store.Clone(SetLeaderCount(leaderCount),
			SetRegionCount(regionCount),
			SetPendingPeerCount(pendingPeerCount),
			SetLeaderSize(leaderSize),
			SetRegionSize(regionSize))
		s.SetStore(newStore)
	}
}
