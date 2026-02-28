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

// PriorityLevel 较低的级别表示较高的优先级
type PriorityLevel int

// 内置优先级级别
const (
	LowPriority PriorityLevel = iota
	NormalPriority
	HighPriority
)

// ScheduleKind 区分资源和调度策略。
type ScheduleKind struct {
	Resource ResourceKind
}

// NewScheduleKind 使用资源类型和调度策略创建调度类型。
func NewScheduleKind(Resource ResourceKind) ScheduleKind {
	return ScheduleKind{
		Resource: Resource,
	}
}

// ResourceKind 区分不同类型的资源。
type ResourceKind int

const (
	// LeaderKind 表示 leader 类型资源
	LeaderKind ResourceKind = iota
	// RegionKind 表示 region 类型资源
	RegionKind
)

func (k ResourceKind) String() string {
	switch k {
	case LeaderKind:
		return "leader"
	case RegionKind:
		return "region"
	default:
		return "unknown"
	}
}
