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

package server

import (
	"context"
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/schedulerpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/logutil"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/typeutil"
	"github.com/pingcap-incubator/tinykv/scheduler/server/config"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/id"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap/errcode"
	"github.com/pingcap/log"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

var (
	backgroundJobInterval      = time.Minute
	defaultChangedRegionsLimit = 10000
)

// RaftCluster 用于集群配置管理。
// Raft 集群键格式：
// cluster 1 -> /1/raft，值是 metapb.Cluster
// cluster 2 -> /2/raft
// 对于 cluster 1
// store 1 -> /1/raft/s/1，值是 metapb.Store
// region 1 -> /1/raft/r/1，值是 metapb.Region
type RaftCluster struct {
	sync.RWMutex
	ctx context.Context

	s *Server

	running bool

	clusterID   uint64
	clusterRoot string

	// cached cluster info
	core    *core.BasicCluster
	meta    *metapb.Cluster
	opt     *config.ScheduleOption
	storage *core.Storage
	id      id.Allocator

	prepareChecker *prepareChecker

	coordinator *coordinator

	wg   sync.WaitGroup
	quit chan struct{}
}

// ClusterStatus 保存一些状态信息
type ClusterStatus struct {
	RaftBootstrapTime time.Time `json:"raft_bootstrap_time,omitempty"`
	IsInitialized     bool      `json:"is_initialized"`
}

func newRaftCluster(ctx context.Context, s *Server, clusterID uint64) *RaftCluster {
	return &RaftCluster{
		ctx:         ctx,
		s:           s,
		running:     false,
		clusterID:   clusterID,
		clusterRoot: s.getClusterRootPath(),
	}
}

func (c *RaftCluster) loadClusterStatus() (*ClusterStatus, error) {
	bootstrapTime, err := c.loadBootstrapTime()
	if err != nil {
		return nil, err
	}
	var isInitialized bool
	if bootstrapTime != typeutil.ZeroTime {
		isInitialized = c.isInitialized()
	}
	return &ClusterStatus{
		RaftBootstrapTime: bootstrapTime,
		IsInitialized:     isInitialized,
	}, nil
}

func (c *RaftCluster) isInitialized() bool {
	if c.core.GetRegionCount() > 1 {
		return true
	}
	region := c.core.SearchRegion(nil)
	return region != nil &&
		len(region.GetVoters()) >= int(c.s.GetReplicationConfig().MaxReplicas) &&
		len(region.GetPendingPeers()) == 0
}

// loadBootstrapTime 从 etcd 加载保存的 bootstrap 时间。当发生错误或集群尚未 bootstrap 时，
// 它返回 time.Time 的零值。
func (c *RaftCluster) loadBootstrapTime() (time.Time, error) {
	var t time.Time
	data, err := c.s.storage.Load(c.s.storage.ClusterStatePath("raft_bootstrap_time"))
	if err != nil {
		return t, err
	}
	if data == "" {
		return t, nil
	}
	return typeutil.ParseTimestamp([]byte(data))
}

func (c *RaftCluster) initCluster(id id.Allocator, opt *config.ScheduleOption, storage *core.Storage) {
	c.core = core.NewBasicCluster()
	c.opt = opt
	c.storage = storage
	c.id = id
	c.prepareChecker = newPrepareChecker()
}

func (c *RaftCluster) start() error {
	c.Lock()
	defer c.Unlock()

	if c.running {
		log.Warn("raft cluster has already been started")
		return nil
	}

	c.initCluster(c.s.idAllocator, c.s.scheduleOpt, c.s.storage)
	cluster, err := c.loadClusterInfo()
	if err != nil {
		return err
	}
	if cluster == nil {
		return nil
	}

	c.coordinator = newCoordinator(c.ctx, cluster, c.s.hbStreams)
	c.quit = make(chan struct{})

	c.wg.Add(2)
	go c.runCoordinator()
	go c.runBackgroundJobs(backgroundJobInterval)
	c.running = true

	return nil
}

// 如果集群尚未 bootstrap 则返回 nil。
func (c *RaftCluster) loadClusterInfo() (*RaftCluster, error) {
	c.meta = &metapb.Cluster{}
	ok, err := c.storage.LoadMeta(c.meta)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}

	start := time.Now()
	if err := c.storage.LoadStores(c.core.PutStore); err != nil {
		return nil, err
	}
	log.Info("load stores",
		zap.Int("count", c.getStoreCount()),
		zap.Duration("cost", time.Since(start)),
	)
	return c, nil
}

func (c *RaftCluster) runBackgroundJobs(interval time.Duration) {
	defer logutil.LogPanic()
	defer c.wg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.quit:
			log.Info("background jobs has been stopped")
			return
		case <-ticker.C:
			c.checkStores()
		}
	}
}

func (c *RaftCluster) runCoordinator() {
	defer logutil.LogPanic()
	defer c.wg.Done()
	defer func() {
		c.coordinator.wg.Wait()
		log.Info("coordinator has been stopped")
	}()
	c.coordinator.run()
	<-c.coordinator.ctx.Done()
	log.Info("coordinator is stopping")
}

func (c *RaftCluster) stop() {
	c.Lock()

	if !c.running {
		c.Unlock()
		return
	}

	c.running = false

	close(c.quit)
	c.coordinator.stop()
	c.Unlock()
	c.wg.Wait()
}

func (c *RaftCluster) isRunning() bool {
	c.RLock()
	defer c.RUnlock()
	return c.running
}

// GetOperatorController 返回 operator 控制器。
func (c *RaftCluster) GetOperatorController() *schedule.OperatorController {
	c.RLock()
	defer c.RUnlock()
	return c.coordinator.opController
}

// GetHeartbeatStreams 返回心跳流。
func (c *RaftCluster) GetHeartbeatStreams() *heartbeatStreams {
	c.RLock()
	defer c.RUnlock()
	return c.coordinator.hbStreams
}

// GetCoordinator 返回协调器。
func (c *RaftCluster) GetCoordinator() *coordinator {
	c.RLock()
	defer c.RUnlock()
	return c.coordinator
}

// handleStoreHeartbeat 更新 store 状态。
func (c *RaftCluster) handleStoreHeartbeat(stats *schedulerpb.StoreStats) error {
	c.Lock()
	defer c.Unlock()

	storeID := stats.GetStoreId()
	store := c.GetStore(storeID)
	if store == nil {
		return core.NewStoreNotFoundErr(storeID)
	}
	newStore := store.Clone(core.SetStoreStats(stats), core.SetLastHeartbeatTS(time.Now()))
	c.core.PutStore(newStore)
	return nil
}

// processRegionHeartbeat 更新 region 信息。
func (c *RaftCluster) processRegionHeartbeat(region *core.RegionInfo) error {
	// 你的代码在这里 (3C)。

	return nil
}

func (c *RaftCluster) updateStoreStatusLocked(id uint64) {
	leaderCount := c.core.GetStoreLeaderCount(id)
	regionCount := c.core.GetStoreRegionCount(id)
	pendingPeerCount := c.core.GetStorePendingPeerCount(id)
	leaderRegionSize := c.core.GetStoreLeaderRegionSize(id)
	regionSize := c.core.GetStoreRegionSize(id)
	c.core.UpdateStoreStatus(id, leaderCount, regionCount, pendingPeerCount, leaderRegionSize, regionSize)
}

func makeStoreKey(clusterRootPath string, storeID uint64) string {
	return path.Join(clusterRootPath, "s", fmt.Sprintf("%020d", storeID))
}

func makeRaftClusterStatusPrefix(clusterRootPath string) string {
	return path.Join(clusterRootPath, "status")
}

func makeBootstrapTimeKey(clusterRootPath string) string {
	return path.Join(makeRaftClusterStatusPrefix(clusterRootPath), "raft_bootstrap_time")
}

func checkBootstrapRequest(clusterID uint64, req *schedulerpb.BootstrapRequest) error {
	// TODO: do more check for request fields validation.

	storeMeta := req.GetStore()
	if storeMeta == nil {
		return errors.Errorf("missing store meta for bootstrap %d", clusterID)
	} else if storeMeta.GetId() == 0 {
		return errors.New("invalid zero store id")
	}

	return nil
}

func (c *RaftCluster) getClusterID() uint64 {
	c.RLock()
	defer c.RUnlock()
	return c.meta.GetId()
}

func (c *RaftCluster) putMetaLocked(meta *metapb.Cluster) error {
	if c.storage != nil {
		if err := c.storage.SaveMeta(meta); err != nil {
			return err
		}
	}
	c.meta = meta
	return nil
}

// GetRegionByKey 通过 region key 从集群获取 region 和 leader peer。
func (c *RaftCluster) GetRegionByKey(regionKey []byte) (*metapb.Region, *metapb.Peer) {
	region := c.core.SearchRegion(regionKey)
	if region == nil {
		return nil, nil
	}
	return region.GetMeta(), region.GetLeader()
}

// GetPrevRegionByKey 通过 region key 从集群获取前一个 region 和 leader peer。
func (c *RaftCluster) GetPrevRegionByKey(regionKey []byte) (*metapb.Region, *metapb.Peer) {
	region := c.core.SearchPrevRegion(regionKey)
	if region == nil {
		return nil, nil
	}
	return region.GetMeta(), region.GetLeader()
}

// GetRegionInfoByKey 通过 region key 从集群获取 regionInfo。
func (c *RaftCluster) GetRegionInfoByKey(regionKey []byte) *core.RegionInfo {
	return c.core.SearchRegion(regionKey)
}

// ScanRegions 从 start key 开始扫描 region，直到 region 包含 endKey，或
// 总数大于 limit。
func (c *RaftCluster) ScanRegions(startKey, endKey []byte, limit int) []*core.RegionInfo {
	return c.core.ScanRange(startKey, endKey, limit)
}

// GetRegionByID 通过 regionID 从集群获取 region 和 leader peer。
func (c *RaftCluster) GetRegionByID(regionID uint64) (*metapb.Region, *metapb.Peer) {
	region := c.GetRegion(regionID)
	if region == nil {
		return nil, nil
	}
	return region.GetMeta(), region.GetLeader()
}

// GetRegion 通过 ID 搜索 region。
func (c *RaftCluster) GetRegion(regionID uint64) *core.RegionInfo {
	return c.core.GetRegion(regionID)
}

// GetMetaRegions 从集群获取 region。
func (c *RaftCluster) GetMetaRegions() []*metapb.Region {
	return c.core.GetMetaRegions()
}

// GetRegions 返回所有 region 的详细信息。
func (c *RaftCluster) GetRegions() []*core.RegionInfo {
	return c.core.GetRegions()
}

// GetRegionCount 返回 region 的总数量
func (c *RaftCluster) GetRegionCount() int {
	return c.core.GetRegionCount()
}

// GetStoreRegions 返回给定 storeID 的所有 region 信息。
func (c *RaftCluster) GetStoreRegions(storeID uint64) []*core.RegionInfo {
	return c.core.GetStoreRegions(storeID)
}

// RandLeaderRegion 返回在该 store 上有 leader 的随机 region。
func (c *RaftCluster) RandLeaderRegion(storeID uint64, opts ...core.RegionOption) *core.RegionInfo {
	return c.core.RandLeaderRegion(storeID, opts...)
}

// RandFollowerRegion 返回在该 store 上有 follower 的随机 region。
func (c *RaftCluster) RandFollowerRegion(storeID uint64, opts ...core.RegionOption) *core.RegionInfo {
	return c.core.RandFollowerRegion(storeID, opts...)
}

// RandPendingRegion 返回在该 store 上有 pending peer 的随机 region。
func (c *RaftCluster) RandPendingRegion(storeID uint64, opts ...core.RegionOption) *core.RegionInfo {
	return c.core.RandPendingRegion(storeID, opts...)
}

// GetPendingRegionsWithLock 按 storeID 返回 pending regions 子树
func (c *RaftCluster) GetPendingRegionsWithLock(storeID uint64, callback func(core.RegionsContainer)) {
	c.core.GetPendingRegionsWithLock(storeID, callback)
}

// GetLeadersWithLock 按 storeID 返回 leaders 子树
func (c *RaftCluster) GetLeadersWithLock(storeID uint64, callback func(core.RegionsContainer)) {
	c.core.GetLeadersWithLock(storeID, callback)
}

// GetFollowersWithLock 按 storeID 返回 followers 子树
func (c *RaftCluster) GetFollowersWithLock(storeID uint64, callback func(core.RegionsContainer)) {
	c.core.GetFollowersWithLock(storeID, callback)
}

// GetLeaderStore 返回包含 region 的 leader peer 的所有 store。
func (c *RaftCluster) GetLeaderStore(region *core.RegionInfo) *core.StoreInfo {
	return c.core.GetLeaderStore(region)
}

// GetFollowerStores 返回包含 region 的 follower peer 的所有 store。
func (c *RaftCluster) GetFollowerStores(region *core.RegionInfo) []*core.StoreInfo {
	return c.core.GetFollowerStores(region)
}

// GetRegionStores 返回包含 region 的 peer 的所有 store。
func (c *RaftCluster) GetRegionStores(region *core.RegionInfo) []*core.StoreInfo {
	return c.core.GetRegionStores(region)
}

func (c *RaftCluster) getStoreCount() int {
	return c.core.GetStoreCount()
}

// GetStoreRegionCount 返回给定 store 的 region 数量。
func (c *RaftCluster) GetStoreRegionCount(storeID uint64) int {
	return c.core.GetStoreRegionCount(storeID)
}

// GetAverageRegionSize 返回 region 的平均近似大小。
func (c *RaftCluster) GetAverageRegionSize() int64 {
	return c.core.GetAverageRegionSize()
}

// DropCacheRegion 从缓存中移除一个 region。
func (c *RaftCluster) DropCacheRegion(id uint64) {
	c.RLock()
	defer c.RUnlock()
	if region := c.GetRegion(id); region != nil {
		c.core.RemoveRegion(region)
	}
}

// GetMetaStores 从集群获取 store。
func (c *RaftCluster) GetMetaStores() []*metapb.Store {
	return c.core.GetMetaStores()
}

// GetStores 返回集群中的所有 store。
func (c *RaftCluster) GetStores() []*core.StoreInfo {
	return c.core.GetStores()
}

// GetStore 从集群获取 store。
func (c *RaftCluster) GetStore(storeID uint64) *core.StoreInfo {
	return c.core.GetStore(storeID)
}

func (c *RaftCluster) putStore(store *metapb.Store) error {
	c.Lock()
	defer c.Unlock()

	if store.GetId() == 0 {
		return errors.Errorf("invalid put store %v", store)
	}

	// Store 地址不能与其他 store 相同。
	for _, s := range c.GetStores() {
		// 如果旧 store 已被移除，可以在相同地址启动新 store。
		if s.IsTombstone() {
			continue
		}
		if s.GetID() != store.GetId() && s.GetAddress() == store.GetAddress() {
			return errors.Errorf("duplicated store address: %v, already registered by %v", store, s.GetMeta())
		}
	}

	s := c.GetStore(store.GetId())
	if s == nil {
		// 添加新 store。
		s = core.NewStoreInfo(store)
	} else {
		// 更新已存在的 store。
		s = s.Clone(
			core.SetStoreAddress(store.Address),
		)
	}
	return c.putStoreLocked(s)
}

// RemoveStore 将集群中的 store 标记为 offline。
// 状态转换：Up -> Offline。
func (c *RaftCluster) RemoveStore(storeID uint64) error {
	op := errcode.Op("store.remove")
	c.Lock()
	defer c.Unlock()

	store := c.GetStore(storeID)
	if store == nil {
		return op.AddTo(core.NewStoreNotFoundErr(storeID))
	}

	// 移除一个 offline store 应该是可以的，无需操作。
	if store.IsOffline() {
		return nil
	}

	if store.IsTombstone() {
		return op.AddTo(core.StoreTombstonedErr{StoreID: storeID})
	}

	newStore := store.Clone(core.SetStoreState(metapb.StoreState_Offline))
	log.Warn("store has been offline",
		zap.Uint64("store-id", newStore.GetID()),
		zap.String("store-address", newStore.GetAddress()))
	return c.putStoreLocked(newStore)
}

// BuryStore 将集群中的 store 标记为 tombstone。
// 状态转换：
// 情况 1：Up -> Tombstone（如果 force 为 true）；
// 情况 2：Offline -> Tombstone。
func (c *RaftCluster) BuryStore(storeID uint64, force bool) error { // revive:disable-line:flag-parameter
	c.Lock()
	defer c.Unlock()

	store := c.GetStore(storeID)
	if store == nil {
		return core.NewStoreNotFoundErr(storeID)
	}

	// 将一个 tombstone store 再次标记为 tombstone 应该是可以的，无需操作。
	if store.IsTombstone() {
		return nil
	}

	if store.IsUp() {
		if !force {
			return errors.New("store is still up, please remove store gracefully")
		}
		log.Warn("forcedly bury store", zap.Stringer("store", store.GetMeta()))
	}

	newStore := store.Clone(core.SetStoreState(metapb.StoreState_Tombstone))
	log.Warn("store has been Tombstone",
		zap.Uint64("store-id", newStore.GetID()),
		zap.String("store-address", newStore.GetAddress()))
	return c.putStoreLocked(newStore)
}

// BlockStore 阻止 balancer 选择该 store。
func (c *RaftCluster) BlockStore(storeID uint64) error {
	return c.core.BlockStore(storeID)
}

// UnblockStore 允许 balancer 选择该 store。
func (c *RaftCluster) UnblockStore(storeID uint64) {
	c.core.UnblockStore(storeID)
}

// AttachAvailableFunc 将一个可用性函数附加到特定的 store。
func (c *RaftCluster) AttachAvailableFunc(storeID uint64, f func() bool) {
	c.core.AttachAvailableFunc(storeID, f)
}

// SetStoreState 设置 store 的状态。
func (c *RaftCluster) SetStoreState(storeID uint64, state metapb.StoreState) error {
	c.Lock()
	defer c.Unlock()

	store := c.GetStore(storeID)
	if store == nil {
		return core.NewStoreNotFoundErr(storeID)
	}

	newStore := store.Clone(core.SetStoreState(state))
	log.Warn("store update state",
		zap.Uint64("store-id", storeID),
		zap.Stringer("new-state", state))
	return c.putStoreLocked(newStore)
}

// SetStoreWeight 设置 store 的 leader/region 平衡权重。
func (c *RaftCluster) SetStoreWeight(storeID uint64, leaderWeight, regionWeight float64) error {
	c.Lock()
	defer c.Unlock()

	store := c.GetStore(storeID)
	if store == nil {
		return core.NewStoreNotFoundErr(storeID)
	}

	if err := c.s.storage.SaveStoreWeight(storeID, leaderWeight, regionWeight); err != nil {
		return err
	}

	newStore := store.Clone(
		core.SetLeaderWeight(leaderWeight),
		core.SetRegionWeight(regionWeight),
	)

	return c.putStoreLocked(newStore)
}

func (c *RaftCluster) putStoreLocked(store *core.StoreInfo) error {
	if c.storage != nil {
		if err := c.storage.SaveStore(store.GetMeta()); err != nil {
			return err
		}
	}
	c.core.PutStore(store)
	return nil
}

func (c *RaftCluster) checkStores() {
	var offlineStores []*metapb.Store
	var upStoreCount int
	stores := c.GetStores()
	for _, store := range stores {
		// store 已经是 tombstone
		if store.IsTombstone() {
			continue
		}

		if store.IsUp() {
			upStoreCount++
			continue
		}

		offlineStore := store.GetMeta()
		// 如果 store 为空，它可以被标记为 tombstone。
		regionCount := c.core.GetStoreRegionCount(offlineStore.GetId())
		if regionCount == 0 {
			if err := c.BuryStore(offlineStore.GetId(), false); err != nil {
				log.Error("bury store failed",
					zap.Stringer("store", offlineStore),
					zap.Error(err))
			}
		} else {
			offlineStores = append(offlineStores, offlineStore)
		}
	}

	if len(offlineStores) == 0 {
		return
	}

	if upStoreCount < c.GetMaxReplicas() {
		for _, offlineStore := range offlineStores {
			log.Warn("store may not turn into Tombstone, there are no extra up store has enough space to accommodate the extra replica", zap.Stringer("store", offlineStore))
		}
	}
}

// RemoveTombStoneRecords 移除 tombstone 记录。
func (c *RaftCluster) RemoveTombStoneRecords() error {
	c.Lock()
	defer c.Unlock()

	for _, store := range c.GetStores() {
		if store.IsTombstone() {
			// store 已经是 tombstone
			err := c.deleteStoreLocked(store)
			if err != nil {
				log.Error("delete store failed",
					zap.Stringer("store", store.GetMeta()),
					zap.Error(err))
				return err
			}
			log.Info("delete store successed",
				zap.Stringer("store", store.GetMeta()))
		}
	}
	return nil
}

func (c *RaftCluster) deleteStoreLocked(store *core.StoreInfo) error {
	if c.storage != nil {
		if err := c.storage.DeleteStore(store.GetMeta()); err != nil {
			return err
		}
	}
	c.core.DeleteStore(store)
	return nil
}

func (c *RaftCluster) collectHealthStatus() {
	client := c.s.GetClient()
	members, err := GetMembers(client)
	if err != nil {
		log.Error("get members error", zap.Error(err))
	}
	unhealth := c.s.CheckHealth(members)
	for _, member := range members {
		if _, ok := unhealth[member.GetMemberId()]; ok {
			continue
		}
	}
}

func (c *RaftCluster) takeRegionStoresLocked(region *core.RegionInfo) []*core.StoreInfo {
	stores := make([]*core.StoreInfo, 0, len(region.GetPeers()))
	for _, p := range region.GetPeers() {
		if store := c.core.TakeStore(p.StoreId); store != nil {
			stores = append(stores, store)
		}
	}
	return stores
}

func (c *RaftCluster) allocID() (uint64, error) {
	return c.id.Alloc()
}

// AllocPeer 在 store 上分配一个新的 peer。
func (c *RaftCluster) AllocPeer(storeID uint64) (*metapb.Peer, error) {
	peerID, err := c.allocID()
	if err != nil {
		log.Error("failed to alloc peer", zap.Error(err))
		return nil, err
	}
	peer := &metapb.Peer{
		Id:      peerID,
		StoreId: storeID,
	}
	return peer, nil
}

// GetConfig 从集群获取配置。
func (c *RaftCluster) GetConfig() *metapb.Cluster {
	c.RLock()
	defer c.RUnlock()
	return proto.Clone(c.meta).(*metapb.Cluster)
}

func (c *RaftCluster) putConfig(meta *metapb.Cluster) error {
	c.Lock()
	defer c.Unlock()
	if meta.GetId() != c.clusterID {
		return errors.Errorf("invalid cluster %v, mismatch cluster id %d", meta, c.clusterID)
	}
	return c.putMetaLocked(proto.Clone(meta).(*metapb.Cluster))
}

// GetOpt 返回调度选项。
func (c *RaftCluster) GetOpt() *config.ScheduleOption {
	return c.opt
}

// GetLeaderScheduleLimit 返回 leader 调度的限制。
func (c *RaftCluster) GetLeaderScheduleLimit() uint64 {
	return c.opt.GetLeaderScheduleLimit()
}

// GetRegionScheduleLimit 返回 region 调度的限制。
func (c *RaftCluster) GetRegionScheduleLimit() uint64 {
	return c.opt.GetRegionScheduleLimit()
}

// GetReplicaScheduleLimit 返回副本调度的限制。
func (c *RaftCluster) GetReplicaScheduleLimit() uint64 {
	return c.opt.GetReplicaScheduleLimit()
}

// GetPatrolRegionInterval 返回巡检 region 的间隔。
func (c *RaftCluster) GetPatrolRegionInterval() time.Duration {
	return c.opt.GetPatrolRegionInterval()
}

// GetMaxStoreDownTime 返回 store 的最大宕机时间。
func (c *RaftCluster) GetMaxStoreDownTime() time.Duration {
	return c.opt.GetMaxStoreDownTime()
}

// GetMaxReplicas 返回副本数量。
func (c *RaftCluster) GetMaxReplicas() int {
	return c.opt.GetMaxReplicas()
}

// isPrepared 检查集群信息是否已收集
func (c *RaftCluster) isPrepared() bool {
	c.RLock()
	defer c.RUnlock()
	return c.prepareChecker.check(c)
}

func (c *RaftCluster) putRegion(region *core.RegionInfo) error {
	c.Lock()
	defer c.Unlock()
	c.core.PutRegion(region)
	return nil
}

type prepareChecker struct {
	reactiveRegions map[uint64]int
	start           time.Time
	sum             int
	isPrepared      bool
}

func newPrepareChecker() *prepareChecker {
	return &prepareChecker{
		start:           time.Now(),
		reactiveRegions: make(map[uint64]int),
	}
}

// 在启动调度器之前，我们需要考虑每个 store 上 region 的比例。
func (checker *prepareChecker) check(c *RaftCluster) bool {
	if checker.isPrepared || time.Since(checker.start) > collectTimeout {
		return true
	}
	// 活跃 region 数量应该大于所有 store 的 region 总数 * collectFactor
	if float64(c.core.Length())*collectFactor > float64(checker.sum) {
		return false
	}
	for _, store := range c.GetStores() {
		if !store.IsUp() {
			continue
		}
		storeID := store.GetID()
		// 对于每个 store，活跃 region 数量应该大于该 store 的 region 总数 * collectFactor
		if float64(c.core.GetStoreRegionCount(storeID))*collectFactor > float64(checker.reactiveRegions[storeID]) {
			return false
		}
	}
	checker.isPrepared = true
	return true
}

func (checker *prepareChecker) collect(region *core.RegionInfo) {
	for _, p := range region.GetPeers() {
		checker.reactiveRegions[p.GetStoreId()]++
	}
	checker.sum++
}
