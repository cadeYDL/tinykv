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

package operator

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	// LeaderOperatorWaitTime 是 leader 操作符的超时时间，
	// 当 leader 操作符存活时间超过此值时，将被视为超时。
	LeaderOperatorWaitTime = 10 * time.Second
	// RegionOperatorWaitTime 是 region 操作符的超时时间，
	// 当 region 操作符存活时间超过此值时，将被视为超时。
	RegionOperatorWaitTime = 10 * time.Minute
)

// Cluster 提供集群 region 分布的概览。
type Cluster interface {
	GetStore(id uint64) *core.StoreInfo
	AllocPeer(storeID uint64) (*metapb.Peer, error)
}

// OpStep 描述不能再细分的基本调度步骤。
type OpStep interface {
	fmt.Stringer
	ConfVerChanged(region *core.RegionInfo) bool
	IsFinish(region *core.RegionInfo) bool
}

// TransferLeader 是一个转移 region leader 的 OpStep。
type TransferLeader struct {
	FromStore, ToStore uint64
}

// ConfVerChanged 如果此步骤更改了配置版本则返回 true
func (tl TransferLeader) ConfVerChanged(region *core.RegionInfo) bool {
	return false // transfer leader 永远不会更改配置版本
}

func (tl TransferLeader) String() string {
	return fmt.Sprintf("transfer leader from store %v to store %v", tl.FromStore, tl.ToStore)
}

// IsFinish 检查当前步骤是否完成。
func (tl TransferLeader) IsFinish(region *core.RegionInfo) bool {
	return region.GetLeader().GetStoreId() == tl.ToStore
}

// AddPeer 是一个添加 region peer 的 OpStep。
type AddPeer struct {
	ToStore, PeerID uint64
}

// ConfVerChanged 如果此步骤更改了配置版本则返回 true
func (ap AddPeer) ConfVerChanged(region *core.RegionInfo) bool {
	if p := region.GetStoreVoter(ap.ToStore); p != nil {
		return p.GetId() == ap.PeerID
	}
	return false
}
func (ap AddPeer) String() string {
	return fmt.Sprintf("add peer %v on store %v", ap.PeerID, ap.ToStore)
}

// IsFinish 检查当前步骤是否完成。
func (ap AddPeer) IsFinish(region *core.RegionInfo) bool {
	if p := region.GetStoreVoter(ap.ToStore); p != nil {
		if p.GetId() != ap.PeerID {
			log.Warn("obtain unexpected peer", zap.String("expect", ap.String()), zap.Uint64("obtain-voter", p.GetId()))
			return false
		}
		return region.GetPendingVoter(p.GetId()) == nil
	}
	return false
}

// RemovePeer 是一个移除 region peer 的 OpStep。
type RemovePeer struct {
	FromStore uint64
}

// ConfVerChanged 如果此步骤更改了配置版本则返回 true
func (rp RemovePeer) ConfVerChanged(region *core.RegionInfo) bool {
	return region.GetStorePeer(rp.FromStore) == nil
}

func (rp RemovePeer) String() string {
	return fmt.Sprintf("remove peer on store %v", rp.FromStore)
}

// IsFinish 检查当前步骤是否完成。
func (rp RemovePeer) IsFinish(region *core.RegionInfo) bool {
	return region.GetStorePeer(rp.FromStore) == nil
}

// Operator 包含调度器生成的执行步骤。
type Operator struct {
	desc        string
	brief       string
	regionID    uint64
	regionEpoch *metapb.RegionEpoch
	kind        OpKind
	steps       []OpStep
	currentStep int32
	createTime  time.Time
	// startTime 用于记录操作符被添加到运行中操作符时的开始时间。
	startTime time.Time
	stepTime  int64
	level     core.PriorityLevel
}

// NewOperator 创建一个新的操作符。
func NewOperator(desc, brief string, regionID uint64, regionEpoch *metapb.RegionEpoch, kind OpKind, steps ...OpStep) *Operator {
	level := core.NormalPriority
	if kind&OpAdmin != 0 {
		level = core.HighPriority
	}
	return &Operator{
		desc:        desc,
		brief:       brief,
		regionID:    regionID,
		regionEpoch: regionEpoch,
		kind:        kind,
		steps:       steps,
		createTime:  time.Now(),
		stepTime:    time.Now().UnixNano(),
		level:       level,
	}
}

func (o *Operator) String() string {
	stepStrs := make([]string, len(o.steps))
	for i := range o.steps {
		stepStrs[i] = o.steps[i].String()
	}
	s := fmt.Sprintf("%s {%s} (kind:%s, region:%v(%v,%v), createAt:%s, startAt:%s, currentStep:%v, steps:[%s])", o.desc, o.brief, o.kind, o.regionID, o.regionEpoch.GetVersion(), o.regionEpoch.GetConfVer(), o.createTime, o.startTime, atomic.LoadInt32(&o.currentStep), strings.Join(stepStrs, ", "))
	if o.IsTimeout() {
		s = s + " timeout"
	}
	if o.IsFinish() {
		s = s + " finished"
	}
	return s
}

// MarshalJSON 将自定义类型序列化为 JSON。
func (o *Operator) MarshalJSON() ([]byte, error) {
	return []byte(`"` + o.String() + `"`), nil
}

// Desc 返回操作符的简短描述。
func (o *Operator) Desc() string {
	return o.desc
}

// SetDesc 为操作符设置描述。
func (o *Operator) SetDesc(desc string) {
	o.desc = desc
}

// AttachKind 为操作符附加操作符类型。
func (o *Operator) AttachKind(kind OpKind) {
	o.kind |= kind
}

// RegionID 返回操作符目标的 region。
func (o *Operator) RegionID() uint64 {
	return o.regionID
}

// RegionEpoch 返回附加到操作符的 region epoch。
func (o *Operator) RegionEpoch() *metapb.RegionEpoch {
	return o.regionEpoch
}

// Kind 返回操作符的类型。
func (o *Operator) Kind() OpKind {
	return o.kind
}

// ElapsedTime 返回自创建以来经过的时间。
func (o *Operator) ElapsedTime() time.Duration {
	return time.Since(o.createTime)
}

// RunningTime 返回自开始运行以来经过的时间。
func (o *Operator) RunningTime() time.Duration {
	return time.Since(o.startTime)
}

// SetStartTime 为操作符设置开始时间。
func (o *Operator) SetStartTime(t time.Time) {
	o.startTime = t
}

// GetStartTime 获取操作符的开始时间。
func (o *Operator) GetStartTime() time.Time {
	return o.startTime
}

// Len 返回操作符的步骤数量。
func (o *Operator) Len() int {
	return len(o.steps)
}

// Step 返回第 i 个步骤。
func (o *Operator) Step(i int) OpStep {
	if i >= 0 && i < len(o.steps) {
		return o.steps[i]
	}
	return nil
}

// Check 检查当前步骤是否完成，返回下一个要执行的步骤。
// 可以被多个 goroutine 并发安全地调用。
func (o *Operator) Check(region *core.RegionInfo) OpStep {
	for step := atomic.LoadInt32(&o.currentStep); int(step) < len(o.steps); step++ {
		if o.steps[int(step)].IsFinish(region) {
			atomic.StoreInt32(&o.currentStep, step+1)
			atomic.StoreInt64(&o.stepTime, time.Now().UnixNano())
		} else {
			return o.steps[int(step)]
		}
	}
	return nil
}

// ConfVerChanged 返回步骤消耗的配置版本号数量
func (o *Operator) ConfVerChanged(region *core.RegionInfo) int {
	total := 0
	current := atomic.LoadInt32(&o.currentStep)
	if current == int32(len(o.steps)) {
		current--
	}
	// 包括当前步骤，它可能在本次心跳中已生效
	for _, step := range o.steps[0 : current+1] {
		if step.ConfVerChanged(region) {
			total++
		}
	}
	return total
}

// SetPriorityLevel 为操作符设置优先级。
func (o *Operator) SetPriorityLevel(level core.PriorityLevel) {
	o.level = level
}

// GetPriorityLevel 获取优先级。
func (o *Operator) GetPriorityLevel() core.PriorityLevel {
	return o.level
}

// IsFinish 检查所有步骤是否已完成。
func (o *Operator) IsFinish() bool {
	return atomic.LoadInt32(&o.currentStep) >= int32(len(o.steps))
}

// IsTimeout 检查操作符的创建时间并确定是否超时。
func (o *Operator) IsTimeout() bool {
	var timeout bool
	if o.IsFinish() {
		return false
	}
	if o.startTime.IsZero() {
		return false
	}
	if o.kind&OpRegion != 0 {
		timeout = time.Since(o.startTime) > RegionOperatorWaitTime
	} else {
		timeout = time.Since(o.startTime) > LeaderOperatorWaitTime
	}
	if timeout {
		return true
	}
	return false
}

// CreateAddPeerOperator 创建一个添加新 peer 的操作符。
func CreateAddPeerOperator(desc string, region *core.RegionInfo, peerID uint64, toStoreID uint64, kind OpKind) *Operator {
	steps := CreateAddPeerSteps(toStoreID, peerID)
	brief := fmt.Sprintf("add peer: store %v", toStoreID)
	return NewOperator(desc, brief, region.GetID(), region.GetRegionEpoch(), kind|OpRegion, steps...)
}

// CreateRemovePeerOperator 创建一个从 region 移除 peer 的操作符。
func CreateRemovePeerOperator(desc string, cluster Cluster, kind OpKind, region *core.RegionInfo, storeID uint64) (*Operator, error) {
	removeKind, steps, err := removePeerSteps(cluster, region, storeID, getRegionFollowerIDs(region))
	if err != nil {
		return nil, err
	}
	brief := fmt.Sprintf("rm peer: store %v", storeID)
	return NewOperator(desc, brief, region.GetID(), region.GetRegionEpoch(), removeKind|kind, steps...), nil
}

// CreateAddPeerSteps 创建添加新 peer 的 OpStep 列表。
func CreateAddPeerSteps(newStore uint64, peerID uint64) []OpStep {
	st := []OpStep{
		AddPeer{ToStore: newStore, PeerID: peerID},
	}
	return st
}

// CreateTransferLeaderOperator 创建一个将 leader 从源 store 转移到目标 store 的操作符。
func CreateTransferLeaderOperator(desc string, region *core.RegionInfo, sourceStoreID uint64, targetStoreID uint64, kind OpKind) *Operator {
	step := TransferLeader{FromStore: sourceStoreID, ToStore: targetStoreID}
	brief := fmt.Sprintf("transfer leader: store %v to %v", sourceStoreID, targetStoreID)
	return NewOperator(desc, brief, region.GetID(), region.GetRegionEpoch(), kind|OpLeader, step)
}

// interleaveStepGroups 交错合并两个步骤组切片。例如：
//
//  a = [[opA1, opA2], [opA3], [opA4, opA5, opA6]]
//  b = [[opB1], [opB2], [opB3, opB4], [opB5, opB6]]
//  c = interleaveStepGroups(a, b, 0)
//  c == [opA1, opA2, opB1, opA3, opB2, opA4, opA5, opA6, opB3, opB4, opB5, opB6]
//
// sizeHint 是返回切片容量的提示。
func interleaveStepGroups(a, b [][]OpStep, sizeHint int) []OpStep {
	steps := make([]OpStep, 0, sizeHint)
	i, j := 0, 0
	for ; i < len(a) && j < len(b); i, j = i+1, j+1 {
		steps = append(steps, a[i]...)
		steps = append(steps, b[j]...)
	}
	for ; i < len(a); i++ {
		steps = append(steps, a[i]...)
	}
	for ; j < len(b); j++ {
		steps = append(steps, b[j]...)
	}
	return steps
}

// CreateMovePeerOperator 创建一个用新 peer 替换旧 peer 的操作符。
func CreateMovePeerOperator(desc string, cluster Cluster, region *core.RegionInfo, kind OpKind, oldStore, newStore uint64, peerID uint64) (*Operator, error) {
	removeKind, steps, err := removePeerSteps(cluster, region, oldStore, append(getRegionFollowerIDs(region), newStore))
	if err != nil {
		return nil, err
	}
	st := CreateAddPeerSteps(newStore, peerID)
	steps = append(st, steps...)
	brief := fmt.Sprintf("mv peer: store %v to %v", oldStore, newStore)
	return NewOperator(desc, brief, region.GetID(), region.GetRegionEpoch(), removeKind|kind|OpRegion, steps...), nil
}

// CreateOfflinePeerOperator 创建一个在下线 store 时用新 peer 替换旧 peer 的操作符。
func CreateOfflinePeerOperator(desc string, cluster Cluster, region *core.RegionInfo, kind OpKind, oldStore, newStore uint64, peerID uint64) (*Operator, error) {
	k, steps, err := transferLeaderStep(cluster, region, oldStore, append(getRegionFollowerIDs(region)))
	if err != nil {
		return nil, err
	}
	kind |= k
	st := CreateAddPeerSteps(newStore, peerID)
	steps = append(steps, st...)
	steps = append(steps, RemovePeer{FromStore: oldStore})
	brief := fmt.Sprintf("mv peer: store %v to %v", oldStore, newStore)
	return NewOperator(desc, brief, region.GetID(), region.GetRegionEpoch(), kind|OpRegion, steps...), nil
}

func getRegionFollowerIDs(region *core.RegionInfo) []uint64 {
	var ids []uint64
	for id := range region.GetFollowers() {
		ids = append(ids, id)
	}
	return ids
}

// removePeerSteps 返回安全移除 peer 的步骤。它通过先转移 leadership 来防止移除 leader。
func removePeerSteps(cluster Cluster, region *core.RegionInfo, storeID uint64, followerIDs []uint64) (kind OpKind, steps []OpStep, err error) {
	kind, steps, err = transferLeaderStep(cluster, region, storeID, followerIDs)
	if err != nil {
		return
	}

	steps = append(steps, RemovePeer{FromStore: storeID})
	kind |= OpRegion
	return
}

func transferLeaderStep(cluster Cluster, region *core.RegionInfo, storeID uint64, followerIDs []uint64) (kind OpKind, steps []OpStep, err error) {
	if region.GetLeader() != nil && region.GetLeader().GetStoreId() == storeID {
		kind, steps, err = transferLeaderToSuitableSteps(cluster, storeID, followerIDs)
		if err != nil {
			log.Debug("failed to create transfer leader step", zap.Uint64("region-id", region.GetID()), zap.Error(err))
			return
		}
	}
	return
}

// findAvailableStore 查找第一个可用的 store。
func findAvailableStore(cluster Cluster, storeIDs []uint64) (int, uint64) {
	for i, id := range storeIDs {
		store := cluster.GetStore(id)
		if store != nil {
			return i, id
		} else {
			log.Debug("nil store", zap.Uint64("store-id", id))
		}
	}
	return -1, 0
}

// transferLeaderToSuitableSteps 返回第一个适合成为 region leader 的 store。
// 如果没有合适的 store，则返回错误。
func transferLeaderToSuitableSteps(cluster Cluster, leaderID uint64, storeIDs []uint64) (OpKind, []OpStep, error) {
	_, id := findAvailableStore(cluster, storeIDs)
	if id != 0 {
		return OpLeader, []OpStep{TransferLeader{FromStore: leaderID, ToStore: id}}, nil
	}
	return 0, nil, errors.New("no suitable store to become region leader")
}
