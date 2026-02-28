// Copyright 2016 PingCAP, Inc.
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
	"fmt"
	"math/rand"

	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

var _ btree.Item = &regionItem{}

type regionItem struct {
	region *RegionInfo
}

// Less 如果 region 的起始 key 小于另一个则返回 true。
func (r *regionItem) Less(other btree.Item) bool {
	left := r.region.GetStartKey()
	right := other.(*regionItem).region.GetStartKey()
	return bytes.Compare(left, right) < 0
}

func (r *regionItem) Contains(key []byte) bool {
	start, end := r.region.GetStartKey(), r.region.GetEndKey()
	return bytes.Compare(key, start) >= 0 && (len(end) == 0 || bytes.Compare(key, end) < 0)
}

const (
	defaultBTreeDegree = 64
)

type regionTree struct {
	tree *btree.BTree
}

func newRegionTree() *regionTree {
	return &regionTree{
		tree: btree.New(defaultBTreeDegree),
	}
}

func (t *regionTree) length() int {
	return t.tree.Len()
}

// getOverlaps 获取与指定 region 范围重叠的 region。
func (t *regionTree) getOverlaps(region *RegionInfo) []*RegionInfo {
	item := &regionItem{region: region}

	// 注意 find() 获取小于或等于该 region 的最后一个项。
	// 在这种情况下：|_______a_______|_____b_____|___c___|
	// 新 region 是      |______d______|
	// find() 将返回 region_a 的 regionItem
	// 并且 region_a 和 region_b 的 startKey 都小于 region_d 的 endKey，
	// 因此它们被视为重叠的 region。
	result := t.find(region)
	if result == nil {
		result = item
	}

	var overlaps []*RegionInfo
	t.tree.AscendGreaterOrEqual(result, func(i btree.Item) bool {
		over := i.(*regionItem)
		if len(region.GetEndKey()) > 0 && bytes.Compare(region.GetEndKey(), over.region.GetStartKey()) <= 0 {
			return false
		}
		overlaps = append(overlaps, over.region)
		return true
	})
	return overlaps
}

// update 使用该 region 更新树。
// 它首先查找并删除所有重叠的 region，然后插入该 region。
func (t *regionTree) update(region *RegionInfo) []*RegionInfo {
	overlaps := t.getOverlaps(region)
	for _, item := range overlaps {
		log.Debug("overlapping region",
			zap.Uint64("region-id", item.GetID()),
			zap.Stringer("delete-region", RegionToHexMeta(item.GetMeta())),
			zap.Stringer("update-region", RegionToHexMeta(region.GetMeta())))
		t.tree.Delete(&regionItem{item})
	}

	t.tree.ReplaceOrInsert(&regionItem{region: region})

	return overlaps
}

// remove 如果 region 在树中则移除它。
// 如果找不到该 region 或找到的 region 与该 region 不同，则不执行任何操作。
func (t *regionTree) remove(region *RegionInfo) {
	if t.length() == 0 {
		return
	}
	result := t.find(region)
	if result == nil || result.region.GetID() != region.GetID() {
		return
	}

	t.tree.Delete(result)
}

// search 返回包含该 key 的 region。
func (t *regionTree) search(regionKey []byte) *RegionInfo {
	region := &RegionInfo{meta: &metapb.Region{StartKey: regionKey}}
	result := t.find(region)
	if result == nil {
		return nil
	}
	return result.region
}

// searchPrev 返回 regionKey 所在 region 的前一个 region。
func (t *regionTree) searchPrev(regionKey []byte) *RegionInfo {
	curRegion := &RegionInfo{meta: &metapb.Region{StartKey: regionKey}}
	curRegionItem := t.find(curRegion)
	if curRegionItem == nil {
		return nil
	}
	prevRegionItem, _ := t.getAdjacentRegions(curRegionItem.region)
	if prevRegionItem == nil {
		return nil
	}
	if !bytes.Equal(prevRegionItem.region.GetEndKey(), curRegionItem.region.GetStartKey()) {
		return nil
	}
	return prevRegionItem.region
}

// find 是一个辅助函数，用于查找包含 region 起始 key 的项。
func (t *regionTree) find(region *RegionInfo) *regionItem {
	item := &regionItem{region: region}

	var result *regionItem
	t.tree.DescendLessOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem)
		return false
	})

	if result == nil || !result.Contains(region.GetStartKey()) {
		return nil
	}

	return result
}

// scanRange 从包含或位于起始 key 之后的第一个 region 开始扫描，
// 直到 f 返回 false
func (t *regionTree) scanRange(startKey []byte, f func(*RegionInfo) bool) {
	region := &RegionInfo{meta: &metapb.Region{StartKey: startKey}}
	// 查找是否存在 key 范围为 [s, d) 且 s < startKey < d 的 region
	startItem := t.find(region)
	if startItem == nil {
		startItem = &regionItem{region: &RegionInfo{meta: &metapb.Region{StartKey: startKey}}}
	}
	t.tree.AscendGreaterOrEqual(startItem, func(item btree.Item) bool {
		return f(item.(*regionItem).region)
	})
}

func (t *regionTree) getAdjacentRegions(region *RegionInfo) (*regionItem, *regionItem) {
	item := &regionItem{region: &RegionInfo{meta: &metapb.Region{StartKey: region.GetStartKey()}}}
	var prev, next *regionItem
	t.tree.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		if bytes.Equal(item.region.GetStartKey(), i.(*regionItem).region.GetStartKey()) {
			return true
		}
		next = i.(*regionItem)
		return false
	})
	t.tree.DescendLessOrEqual(item, func(i btree.Item) bool {
		if bytes.Equal(item.region.GetStartKey(), i.(*regionItem).region.GetStartKey()) {
			return true
		}
		prev = i.(*regionItem)
		return false
	})
	return prev, next
}

// RandomRegion 用于获取与范围 [startKey, endKey) 相交的随机 region。
func (t *regionTree) RandomRegion(startKey, endKey []byte) *RegionInfo {
	if t.length() == 0 {
		return nil
	}

	var endIndex int

	startRegion, startIndex := t.tree.GetWithIndex(&regionItem{region: &RegionInfo{meta: &metapb.Region{StartKey: startKey}}})

	if len(endKey) != 0 {
		_, endIndex = t.tree.GetWithIndex(&regionItem{region: &RegionInfo{meta: &metapb.Region{StartKey: endKey}}})
	} else {
		endIndex = t.tree.Len()
	}

	// 考虑到树中的项可能不连续，
	// 我们需要检查前一个项是否包含该 key。
	if startIndex != 0 && startRegion == nil && t.tree.GetAt(startIndex-1).(*regionItem).Contains(startKey) {
		startIndex--
	}

	if endIndex <= startIndex {
		log.Error("wrong keys",
			zap.String("start-key", fmt.Sprintf("%s", HexRegionKey(startKey))),
			zap.String("end-key", fmt.Sprintf("%s", HexRegionKey(startKey))))
		return nil
	}
	index := rand.Intn(endIndex-startIndex) + startIndex
	return t.tree.GetAt(index).(*regionItem).region
}
