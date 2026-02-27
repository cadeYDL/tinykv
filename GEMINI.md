# TinyKV 项目指南

TinyKV 是一个受 TiKV 和 MIT 6.824 启发的分布式键值存储系统实现。它采用 Go 语言编写，旨在展示分布式数据库存储层的核心原理，包括 Raft 共识算法、多副本管理及分布式事务。

## 项目概览

- **核心组件**:
  - `kv/`: 键值存储实现。包含单机存储（Standalone）和基于 Raft 的分布式存储。
  - `raft/`: Raft 共识算法的核心实现。
  - `scheduler/`: TinyScheduler 实现，负责集群元数据管理、心跳收集及调度任务生成。
  - `proto/`: 基于 gRPC 和 Protocol Buffers 的通信协议定义及生成的 Go 代码。

- **主要技术栈**:
  - Go (1.13+)
  - gRPC / Protobuf
  - Badger (底层本地存储引擎)
  - Raft 共识协议

## 构建与运行

### 编译
使用根目录下的 `Makefile` 进行构建：
```bash
make
```
编译产物将存放在 `bin/` 目录下：
- `tinykv-server`: 存储节点服务。
- `tinyscheduler-server`: 调度中心服务。

### 运行
1. **启动调度器**:
   ```bash
   ./bin/tinyscheduler-server
   ```
2. **启动存储节点**:
   ```bash
   mkdir -p data
   ./bin/tinykv-server -path=data
   ```

## 测试指南

项目测试按实验阶段（Project 1-4）划分，可以通过以下命令运行特定阶段的测试：

- **Project 1 (Standalone KV)**: `make project1`
- **Project 2 (Raft KV)**:
  - 核心 Raft: `make project2a`
  - Raft Store: `make project2b`
  - 快照支持: `make project2c`
- **Project 3 (Multi-Raft KV)**:
  - 成员变更/领导权转移: `make project3a`
  - 分裂与配置变更: `make project3b`
  - 调度器逻辑: `make project3c`
- **Project 4 (Transactions)**: `make project4` (包含 4a, 4b, 4c)

运行所有测试：
```bash
make test
```

## 开发规范

- **代码格式化**: 提交前请运行 `make format` 确保代码符合 Go 标准格式。
- **存储接口**: 所有的存储引擎必须实现 `kv/storage/storage.go` 中的 `Storage` 接口。
- **日志**: 使用项目内置的 `log` 包，日志级别可通过命令行参数 `-loglevel` 调整。
- **协议修改**: 若修改了 `proto/proto/*.proto` 文件，需运行 `make proto` 重新生成 Go 代码。

## 关键目录说明

- `kv/raftstore/`: 核心逻辑，处理 Raft 消息和数据落地。
- `kv/server/`: 处理来自客户端的 RPC 请求（Raw API 和 Transactional API）。
- `kv/transaction/`: 实现 MVCC 和分布式事务逻辑。
- `kv/storage/raft_storage/`: 对接 Raft 集群的存储实现。
- `kv/storage/standalone_storage/`: 用于测试或本地开发的单机存储实现。
