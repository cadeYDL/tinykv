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

package schedule

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"github.com/pingcap/log"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// Scheduler 是调度资源的接口。
type Scheduler interface {
	http.Handler
	GetName() string
	// GetType 应该与传递给 schedule.RegisterScheduler() 的名称一致
	GetType() string
	EncodeConfig() ([]byte, error)
	GetMinInterval() time.Duration
	GetNextInterval(interval time.Duration) time.Duration
	Prepare(cluster opt.Cluster) error
	Cleanup(cluster opt.Cluster)
	Schedule(cluster opt.Cluster) *operator.Operator
	IsScheduleAllowed(cluster opt.Cluster) bool
}

// EncodeConfig 为每个调度器编码自定义配置。
func EncodeConfig(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

// DecodeConfig 为每个调度器解码自定义配置。
func DecodeConfig(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

// ConfigDecoder 用于解码配置。
type ConfigDecoder func(v interface{}) error

// ConfigSliceDecoderBuilder 用于构建配置的切片解码器。
type ConfigSliceDecoderBuilder func([]string) ConfigDecoder

// ConfigJSONDecoder 用于构建配置的 JSON 解码器。
func ConfigJSONDecoder(data []byte) ConfigDecoder {
	return func(v interface{}) error {
		return DecodeConfig(data, v)
	}
}

// ConfigSliceDecoder 配置的默认解码器。
func ConfigSliceDecoder(name string, args []string) ConfigDecoder {
	builder, ok := schedulerArgsToDecoder[name]
	if !ok {
		return func(v interface{}) error {
			return errors.Errorf("the config decoer do not register for %s", name)
		}
	}
	return builder(args)
}

// CreateSchedulerFunc 用于创建调度器。
type CreateSchedulerFunc func(opController *OperatorController, storage *core.Storage, dec ConfigDecoder) (Scheduler, error)

var schedulerMap = make(map[string]CreateSchedulerFunc)
var schedulerArgsToDecoder = make(map[string]ConfigSliceDecoderBuilder)

// RegisterScheduler 绑定调度器创建器。应该在包的 init() 函数中调用。
func RegisterScheduler(typ string, createFn CreateSchedulerFunc) {
	if _, ok := schedulerMap[typ]; ok {
		log.Fatal("duplicated scheduler", zap.String("type", typ))
	}
	schedulerMap[typ] = createFn
}

// RegisterSliceDecoderBuilder 将参数转换为配置。应该在包的 init() 函数中调用。
func RegisterSliceDecoderBuilder(typ string, builder ConfigSliceDecoderBuilder) {
	if _, ok := schedulerArgsToDecoder[typ]; ok {
		log.Fatal("duplicated scheduler", zap.String("type", typ))
	}
	schedulerArgsToDecoder[typ] = builder
}

// IsSchedulerRegistered 检查指定名称的调度器类型是否已注册。
func IsSchedulerRegistered(name string) bool {
	_, ok := schedulerMap[name]
	return ok
}

// CreateScheduler 使用已注册的创建函数创建调度器。
func CreateScheduler(typ string, opController *OperatorController, storage *core.Storage, dec ConfigDecoder) (Scheduler, error) {
	fn, ok := schedulerMap[typ]
	if !ok {
		return nil, errors.Errorf("create func of %v is not registered", typ)
	}

	s, err := fn(opController, storage, dec)
	if err != nil {
		return nil, err
	}
	data, err := s.EncodeConfig()
	if err != nil {
		return nil, err
	}
	err = storage.SaveScheduleConfig(s.GetName(), data)
	return s, err
}

// FindSchedulerTypeByName 根据指定名称查找类型。
func FindSchedulerTypeByName(name string) string {
	var typ string
	for registerdType := range schedulerMap {
		if strings.Index(name, registerdType) != -1 {
			if len(registerdType) > len(typ) {
				typ = registerdType
			}
		}
	}
	return typ
}
