package raftstore

import (
	"bytes"
	"sync"
	"time"

	"github.com/Connor1996/badger"
	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/scheduler_client"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/util/worker"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

var _ btree.Item = &regionItem{}

type regionItem struct {
	region *metapb.Region
}

// Less 如果 region 的 start key 小于另一个则返回 true。
func (r *regionItem) Less(other btree.Item) bool {
	left := r.region.GetStartKey()
	right := other.(*regionItem).region.GetStartKey()
	return bytes.Compare(left, right) < 0
}

type storeMeta struct {
	sync.RWMutex
	/// region start key -> region
	regionRanges *btree.BTree
	/// region_id -> region
	regions map[uint64]*metapb.Region
	/// 来自新分裂 Region 的 `MsgRequestVote` 消息如果此 store 中当前没有这样的 Region，
	/// 则不应该被丢弃。所以消息被临时记录，稍后会被处理。
	pendingVotes []*rspb.RaftMessage
}

func newStoreMeta() *storeMeta {
	return &storeMeta{
		regionRanges: btree.New(2),
		regions:      map[uint64]*metapb.Region{},
	}
}

func (m *storeMeta) setRegion(region *metapb.Region, peer *peer) {
	m.regions[region.Id] = region
	peer.SetRegion(region)
}

// getOverlapRegions 获取与指定 region 范围重叠的 region。
func (m *storeMeta) getOverlapRegions(region *metapb.Region) []*metapb.Region {
	item := &regionItem{region: region}
	var result *regionItem
	// find 是一个辅助函数，用于查找包含 region start key 的项。
	m.regionRanges.DescendLessOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem)
		return false
	})

	if result == nil || engine_util.ExceedEndKey(region.GetStartKey(), result.region.GetEndKey()) {
		result = item
	}

	var overlaps []*metapb.Region
	m.regionRanges.AscendGreaterOrEqual(result, func(i btree.Item) bool {
		over := i.(*regionItem)
		if engine_util.ExceedEndKey(over.region.GetStartKey(), region.GetEndKey()) {
			return false
		}
		overlaps = append(overlaps, over.region)
		return true
	})
	return overlaps
}

type GlobalContext struct {
	cfg                  *config.Config
	engine               *engine_util.Engines
	store                *metapb.Store
	storeMeta            *storeMeta
	snapMgr              *snap.SnapManager
	router               *router
	trans                Transport
	schedulerTaskSender  chan<- worker.Task
	regionTaskSender     chan<- worker.Task
	raftLogGCTaskSender  chan<- worker.Task
	splitCheckTaskSender chan<- worker.Task
	schedulerClient      scheduler_client.Client
	tickDriverSender     chan uint64
}

type Transport interface {
	Send(msg *rspb.RaftMessage) error
}

/// loadPeers 加载此 store 中的 peer。它扫描 db 引擎，从中加载所有 region 及其 peer。
/// 警告：此 store 在初始化之前不应被使用。
func (bs *Raftstore) loadPeers() ([]*peer, error) {
	// 扫描 region 元数据以获取保存的 region。
	startKey := meta.RegionMetaMinKey
	endKey := meta.RegionMetaMaxKey
	ctx := bs.ctx
	kvEngine := ctx.engine.Kv
	storeID := ctx.store.Id

	var totalCount, tombStoneCount int
	var regionPeers []*peer

	t := time.Now()
	kvWB := new(engine_util.WriteBatch)
	raftWB := new(engine_util.WriteBatch)
	err := kvEngine.View(func(txn *badger.Txn) error {
		// 从 RegionLocalState 获取所有 region
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(startKey); it.Valid(); it.Next() {
			item := it.Item()
			if bytes.Compare(item.Key(), endKey) >= 0 {
				break
			}
			regionID, suffix, err := meta.DecodeRegionMetaKey(item.Key())
			if err != nil {
				return err
			}
			if suffix != meta.RegionStateSuffix {
				continue
			}
			val, err := item.Value()
			if err != nil {
				return errors.WithStack(err)
			}
			totalCount++
			localState := new(rspb.RegionLocalState)
			err = localState.Unmarshal(val)
			if err != nil {
				return errors.WithStack(err)
			}
			region := localState.Region
			if localState.State == rspb.PeerState_Tombstone {
				tombStoneCount++
				bs.clearStaleMeta(kvWB, raftWB, localState)
				continue
			}

			peer, err := createPeer(storeID, ctx.cfg, ctx.regionTaskSender, ctx.engine, region)
			if err != nil {
				return err
			}
			ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: region})
			ctx.storeMeta.regions[regionID] = region
			// 这里不需要检查重复，因为我们使用 region id 作为 DB 中的 key。
			regionPeers = append(regionPeers, peer)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	kvWB.MustWriteToDB(ctx.engine.Kv)
	raftWB.MustWriteToDB(ctx.engine.Raft)

	log.Infof("start store %d, region_count %d, tombstone_count %d, takes %v",
		storeID, totalCount, tombStoneCount, time.Since(t))
	return regionPeers, nil
}

func (bs *Raftstore) clearStaleMeta(kvWB, raftWB *engine_util.WriteBatch, originState *rspb.RegionLocalState) {
	region := originState.Region
	raftState, err := meta.GetRaftLocalState(bs.ctx.engine.Raft, region.Id)
	if err != nil {
		// 它已经被清理过了。
		return
	}
	err = ClearMeta(bs.ctx.engine, kvWB, raftWB, region.Id, raftState.LastIndex)
	if err != nil {
		panic(err)
	}
	if err := kvWB.SetMeta(meta.RegionStateKey(region.Id), originState); err != nil {
		panic(err)
	}
}

type workers struct {
	raftLogGCWorker  *worker.Worker
	schedulerWorker  *worker.Worker
	splitCheckWorker *worker.Worker
	regionWorker     *worker.Worker
	wg               *sync.WaitGroup
}

type Raftstore struct {
	ctx        *GlobalContext
	storeState *storeState
	router     *router
	workers    *workers
	tickDriver *tickDriver
	closeCh    chan struct{}
	wg         *sync.WaitGroup
}

func (bs *Raftstore) start(
	meta *metapb.Store,
	cfg *config.Config,
	engines *engine_util.Engines,
	trans Transport,
	schedulerClient scheduler_client.Client,
	snapMgr *snap.SnapManager) error {
	y.Assert(bs.workers == nil)
	// TODO：我们之后也可以定期获取集群元数据。
	if err := cfg.Validate(); err != nil {
		return err
	}
	err := snapMgr.Init()
	if err != nil {
		return err
	}
	wg := new(sync.WaitGroup)
	bs.workers = &workers{
		splitCheckWorker: worker.NewWorker("split-check", wg),
		regionWorker:     worker.NewWorker("snapshot-worker", wg),
		raftLogGCWorker:  worker.NewWorker("raft-gc-worker", wg),
		schedulerWorker:  worker.NewWorker("scheduler-worker", wg),
		wg:               wg,
	}
	bs.ctx = &GlobalContext{
		cfg:                  cfg,
		engine:               engines,
		store:                meta,
		storeMeta:            newStoreMeta(),
		snapMgr:              snapMgr,
		router:               bs.router,
		trans:                trans,
		schedulerTaskSender:  bs.workers.schedulerWorker.Sender(),
		regionTaskSender:     bs.workers.regionWorker.Sender(),
		splitCheckTaskSender: bs.workers.splitCheckWorker.Sender(),
		raftLogGCTaskSender:  bs.workers.raftLogGCWorker.Sender(),
		schedulerClient:      schedulerClient,
		tickDriverSender:     bs.tickDriver.newRegionCh,
	}
	regionPeers, err := bs.loadPeers()
	if err != nil {
		return err
	}

	for _, peer := range regionPeers {
		bs.router.register(peer)
	}
	bs.startWorkers(regionPeers)
	return nil
}

func (bs *Raftstore) startWorkers(peers []*peer) {
	ctx := bs.ctx
	workers := bs.workers
	router := bs.router
	bs.wg.Add(2) // raftWorker, storeWorker
	rw := newRaftWorker(ctx, router)
	go rw.run(bs.closeCh, bs.wg)
	sw := newStoreWorker(ctx, bs.storeState)
	go sw.run(bs.closeCh, bs.wg)
	router.sendStore(message.Msg{Type: message.MsgTypeStoreStart, Data: ctx.store})
	for i := 0; i < len(peers); i++ {
		regionID := peers[i].regionId
		_ = router.send(regionID, message.Msg{RegionID: regionID, Type: message.MsgTypeStart})
	}
	engines := ctx.engine
	cfg := ctx.cfg
	workers.splitCheckWorker.Start(runner.NewSplitCheckHandler(engines.Kv, NewRaftstoreRouter(router), cfg))
	workers.regionWorker.Start(runner.NewRegionTaskHandler(engines, ctx.snapMgr))
	workers.raftLogGCWorker.Start(runner.NewRaftLogGCTaskHandler())
	workers.schedulerWorker.Start(runner.NewSchedulerTaskHandler(ctx.store.Id, ctx.schedulerClient, NewRaftstoreRouter(router)))
	go bs.tickDriver.run()
}

func (bs *Raftstore) shutDown() {
	close(bs.closeCh)
	bs.wg.Wait()
	bs.tickDriver.stop()
	if bs.workers == nil {
		return
	}
	workers := bs.workers
	bs.workers = nil
	workers.splitCheckWorker.Stop()
	workers.regionWorker.Stop()
	workers.raftLogGCWorker.Stop()
	workers.schedulerWorker.Stop()
	workers.wg.Wait()
}

func CreateRaftstore(cfg *config.Config) (*RaftstoreRouter, *Raftstore) {
	storeSender, storeState := newStoreState(cfg)
	router := newRouter(storeSender)
	raftstore := &Raftstore{
		router:     router,
		storeState: storeState,
		tickDriver: newTickDriver(cfg.RaftBaseTickInterval, router, storeState.ticker),
		closeCh:    make(chan struct{}),
		wg:         new(sync.WaitGroup),
	}
	return NewRaftstoreRouter(router), raftstore
}
