// Copyright 2017 PingCAP, Inc.
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
	"sync"

	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
)

// BasicCluster 为 tikv 集群提供基本数据成员和接口。
type BasicCluster struct {
	sync.RWMutex
	Stores  *StoresInfo
	Regions *RegionsInfo
}

// NewBasicCluster 创建一个 BasicCluster。
func NewBasicCluster() *BasicCluster {
	return &BasicCluster{
		Stores:  NewStoresInfo(),
		Regions: NewRegionsInfo(),
	}
}

// GetStores 返回集群中的所有 Store。
func (bc *BasicCluster) GetStores() []*StoreInfo {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Stores.GetStores()
}

// GetMetaStores 获取完整的 metapb.Store 集合。
func (bc *BasicCluster) GetMetaStores() []*metapb.Store {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Stores.GetMetaStores()
}

// GetStore 通过 ID 搜索 store。
func (bc *BasicCluster) GetStore(storeID uint64) *StoreInfo {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Stores.GetStore(storeID)
}

// GetRegion 通过 ID 搜索 region。
func (bc *BasicCluster) GetRegion(regionID uint64) *RegionInfo {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Regions.GetRegion(regionID)
}

// GetRegions 从 regionMap 获取所有 RegionInfo。
func (bc *BasicCluster) GetRegions() []*RegionInfo {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Regions.GetRegions()
}

// GetMetaRegions 从 regionMap 获取 metapb.Region 集合。
func (bc *BasicCluster) GetMetaRegions() []*metapb.Region {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Regions.GetMetaRegions()
}

// GetStoreRegions 获取给定 storeID 的所有 RegionInfo。
func (bc *BasicCluster) GetStoreRegions(storeID uint64) []*RegionInfo {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Regions.GetStoreRegions(storeID)
}

// GetRegionStores 返回包含 region peer 的所有 Store。
func (bc *BasicCluster) GetRegionStores(region *RegionInfo) []*StoreInfo {
	bc.RLock()
	defer bc.RUnlock()
	var Stores []*StoreInfo
	for id := range region.GetStoreIds() {
		if store := bc.Stores.GetStore(id); store != nil {
			Stores = append(Stores, store)
		}
	}
	return Stores
}

// GetFollowerStores 返回包含 region follower peer 的所有 Store。
func (bc *BasicCluster) GetFollowerStores(region *RegionInfo) []*StoreInfo {
	bc.RLock()
	defer bc.RUnlock()
	var Stores []*StoreInfo
	for id := range region.GetFollowers() {
		if store := bc.Stores.GetStore(id); store != nil {
			Stores = append(Stores, store)
		}
	}
	return Stores
}

// GetLeaderStore 返回包含 region leader peer 的 Store。
func (bc *BasicCluster) GetLeaderStore(region *RegionInfo) *StoreInfo {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Stores.GetStore(region.GetLeader().GetStoreId())
}

// BlockStore 阻止负载均衡器选择该 store。
func (bc *BasicCluster) BlockStore(storeID uint64) error {
	bc.Lock()
	defer bc.Unlock()
	return bc.Stores.BlockStore(storeID)
}

// UnblockStore 允许负载均衡器选择该 store。
func (bc *BasicCluster) UnblockStore(storeID uint64) {
	bc.Lock()
	defer bc.Unlock()
	bc.Stores.UnblockStore(storeID)
}

// AttachAvailableFunc 将可用函数附加到特定 store。
func (bc *BasicCluster) AttachAvailableFunc(storeID uint64, f func() bool) {
	bc.Lock()
	defer bc.Unlock()
	bc.Stores.AttachAvailableFunc(storeID, f)
}

// UpdateStoreStatus 更新 store 的信息。
func (bc *BasicCluster) UpdateStoreStatus(storeID uint64, leaderCount int, regionCount int, pendingPeerCount int, leaderSize int64, regionSize int64) {
	bc.Lock()
	defer bc.Unlock()
	bc.Stores.UpdateStoreStatus(storeID, leaderCount, regionCount, pendingPeerCount, leaderSize, regionSize)
}

// RandFollowerRegion 返回在该 store 上有 follower 的随机 region。
func (bc *BasicCluster) RandFollowerRegion(storeID uint64, opts ...RegionOption) *RegionInfo {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Regions.RandFollowerRegion(storeID, opts...)
}

// RandLeaderRegion 返回在该 store 上有 leader 的随机 region。
func (bc *BasicCluster) RandLeaderRegion(storeID uint64, opts ...RegionOption) *RegionInfo {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Regions.RandLeaderRegion(storeID, opts...)
}

// RandPendingRegion 返回在该 store 上有 pending peer 的随机 region。
func (bc *BasicCluster) RandPendingRegion(storeID uint64, opts ...RegionOption) *RegionInfo {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Regions.RandPendingRegion(storeID, opts...)
}

// GetPendingRegionsWithLock 通过 storeID 返回 pending region 子树
func (bc *BasicCluster) GetPendingRegionsWithLock(storeID uint64, callback func(RegionsContainer)) {
	bc.RLock()
	defer bc.RUnlock()
	callback(bc.Regions.pendingPeers[storeID])
}

// GetLeadersWithLock 通过 storeID 返回 leader 子树
func (bc *BasicCluster) GetLeadersWithLock(storeID uint64, callback func(RegionsContainer)) {
	bc.RLock()
	defer bc.RUnlock()
	callback(bc.Regions.leaders[storeID])
}

// GetFollowersWithLock 通过 storeID 返回 follower 子树
func (bc *BasicCluster) GetFollowersWithLock(storeID uint64, callback func(RegionsContainer)) {
	bc.RLock()
	defer bc.RUnlock()
	callback(bc.Regions.followers[storeID])
}

// GetRegionCount 获取 regionMap 中 RegionInfo 的总数。
func (bc *BasicCluster) GetRegionCount() int {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Regions.GetRegionCount()
}

// GetStoreCount 返回 storeInfo 的总数。
func (bc *BasicCluster) GetStoreCount() int {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Stores.GetStoreCount()
}

// GetStoreRegionCount 通过 storeID 获取 store 的 leader 和 follower RegionInfo 总数。
func (bc *BasicCluster) GetStoreRegionCount(storeID uint64) int {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Regions.GetStoreLeaderCount(storeID) + bc.Regions.GetStoreFollowerCount(storeID) + bc.Regions.GetStoreLearnerCount(storeID)
}

// GetStoreLeaderCount 获取 store 的 leader RegionInfo 总数。
func (bc *BasicCluster) GetStoreLeaderCount(storeID uint64) int {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Regions.GetStoreLeaderCount(storeID)
}

// GetStoreFollowerCount 获取 store 的 follower RegionInfo 总数。
func (bc *BasicCluster) GetStoreFollowerCount(storeID uint64) int {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Regions.GetStoreFollowerCount(storeID)
}

// GetStorePendingPeerCount 获取包含 pending peer 的 store region 总数。
func (bc *BasicCluster) GetStorePendingPeerCount(storeID uint64) int {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Regions.GetStorePendingPeerCount(storeID)
}

// GetStoreLeaderRegionSize 获取 store 的 leader region 总大小。
func (bc *BasicCluster) GetStoreLeaderRegionSize(storeID uint64) int64 {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Regions.GetStoreLeaderRegionSize(storeID)
}

// GetStoreRegionSize 获取 store 的 region 总大小。
func (bc *BasicCluster) GetStoreRegionSize(storeID uint64) int64 {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Regions.GetStoreLeaderRegionSize(storeID) + bc.Regions.GetStoreFollowerRegionSize(storeID) + bc.Regions.GetStoreLearnerRegionSize(storeID)
}

// GetAverageRegionSize 返回 region 的平均近似大小。
func (bc *BasicCluster) GetAverageRegionSize() int64 {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Regions.GetAverageRegionSize()
}

// PutStore 放入一个 store。
func (bc *BasicCluster) PutStore(store *StoreInfo) {
	bc.Lock()
	defer bc.Unlock()
	bc.Stores.SetStore(store)
}

// DeleteStore 删除一个 store。
func (bc *BasicCluster) DeleteStore(store *StoreInfo) {
	bc.Lock()
	defer bc.Unlock()
	bc.Stores.DeleteStore(store)
}

// TakeStore 返回具有指定 storeID 的原始 StoreInfo 的指针。
func (bc *BasicCluster) TakeStore(storeID uint64) *StoreInfo {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Stores.TakeStore(storeID)
}

// PutRegion 放入一个 region。
func (bc *BasicCluster) PutRegion(region *RegionInfo) []*RegionInfo {
	bc.Lock()
	defer bc.Unlock()
	return bc.Regions.SetRegion(region)
}

// RemoveRegion 从 regionTree 和 regionMap 中移除 RegionInfo。
func (bc *BasicCluster) RemoveRegion(region *RegionInfo) {
	bc.Lock()
	defer bc.Unlock()
	bc.Regions.RemoveRegion(region)
}

// SearchRegion 从 regionTree 中搜索 RegionInfo。
func (bc *BasicCluster) SearchRegion(regionKey []byte) *RegionInfo {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Regions.SearchRegion(regionKey)
}

// SearchPrevRegion 从 regionTree 中搜索前一个 RegionInfo。
func (bc *BasicCluster) SearchPrevRegion(regionKey []byte) *RegionInfo {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Regions.SearchPrevRegion(regionKey)
}

// ScanRange 扫描与 [start key, end key) 相交的 region，最多返回 `limit` 个 region。
// limit <= 0 表示没有限制。
func (bc *BasicCluster) ScanRange(startKey, endKey []byte, limit int) []*RegionInfo {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Regions.ScanRange(startKey, endKey, limit)
}

// GetOverlaps 返回与指定 region 范围重叠的 region。
func (bc *BasicCluster) GetOverlaps(region *RegionInfo) []*RegionInfo {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Regions.GetOverlaps(region)
}

// Length 返回 RegionsInfo 的长度。
func (bc *BasicCluster) Length() int {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Regions.Length()
}

// RegionSetInformer 提供对 region 共享通知器的访问。
type RegionSetInformer interface {
	RandFollowerRegion(storeID uint64, opts ...RegionOption) *RegionInfo
	RandLeaderRegion(storeID uint64, opts ...RegionOption) *RegionInfo
	RandPendingRegion(storeID uint64, opts ...RegionOption) *RegionInfo
	GetPendingRegionsWithLock(storeID uint64, callback func(RegionsContainer))
	GetLeadersWithLock(storeID uint64, callback func(RegionsContainer))
	GetFollowersWithLock(storeID uint64, callback func(RegionsContainer))
	GetAverageRegionSize() int64
	GetStoreRegionCount(storeID uint64) int
	GetRegion(id uint64) *RegionInfo
	ScanRegions(startKey, endKey []byte, limit int) []*RegionInfo
}

// StoreSetInformer 提供对 store 共享通知器的访问。
type StoreSetInformer interface {
	GetStores() []*StoreInfo
	GetStore(id uint64) *StoreInfo

	GetRegionStores(region *RegionInfo) []*StoreInfo
	GetFollowerStores(region *RegionInfo) []*StoreInfo
	GetLeaderStore(region *RegionInfo) *StoreInfo
}

// StoreSetController 用于控制 store 的状态。
type StoreSetController interface {
	BlockStore(id uint64) error
	UnblockStore(id uint64)

	AttachAvailableFunc(id uint64, f func() bool)
}
