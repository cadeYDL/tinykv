# 项目1 单机KV存储

在本项目中，你将构建一个支持列族的单机键值存储 [gRPC](https://grpc.io/docs/guides/) 服务。单机意味着只有一个节点，不是分布式系统。[列族](https://en.wikipedia.org/wiki/Standard_column_family)（下文简称为 CF）是一个类似于键命名空间的术语，即不同列族中相同键的值是不同的。你可以简单地将多个列族视为独立的小型数据库。它用于支持项目4中的事务模型，届时你将了解为什么 TinyKV 需要支持 CF。

该服务支持四种基本操作：Put/Delete/Get/Scan。它维护一个简单的键值对数据库。键和值都是字符串。`Put` 替换数据库中指定 CF 的特定键的值，`Delete` 删除指定 CF 中键的值，`Get` 获取指定 CF 中键的当前值，`Scan` 获取指定 CF 中一系列键的当前值。

该项目可以分为两个步骤：

1. 实现一个单机存储引擎。
2. 实现原始键值服务处理器。

### 代码结构

`gRPC` 服务器在 `kv/main.go` 中初始化，它包含一个 `tinykv.Server`，该服务器提供名为 `TinyKv` 的 `gRPC` 服务。它通过 [protocol-buffer](https://developers.google.com/protocol-buffers) 在 `proto/proto/tinykvpb.proto` 中定义，RPC 请求和响应的详细信息在 `proto/proto/kvrpcpb.proto` 中定义。

通常，你不需要修改 proto 文件，因为所有必要的字段都已为你定义好了。但如果你仍需要修改，可以修改 proto 文件并运行 `make proto` 来更新 `proto/pkg/xxx/xxx.pb.go` 中相关生成的 Go 代码。

此外，`Server` 依赖于一个 `Storage`，这是你需要为单机存储引擎实现的接口，位于 `kv/storage/standalone_storage/standalone_storage.go`。一旦在 `StandaloneStorage` 中实现了 `Storage` 接口，你就可以用它为 `Server` 实现原始键值服务。

#### 实现单机存储引擎

第一个任务是实现 [badger](https://github.com/dgraph-io/badger) 键值 API 的包装器。gRPC 服务器的服务依赖于 `kv/storage/storage.go` 中定义的 `Storage`。在这种情况下，单机存储引擎只是 badger 键值 API 的包装器，它提供两个方法：

``` go
type Storage interface {
    // 其他内容
    Write(ctx *kvrpcpb.Context, batch []Modify) error
    Reader(ctx *kvrpcpb.Context) (StorageReader, error)
}
```

`Write` 应该提供一种将一系列修改应用于内部状态的方式，在这种情况下，内部状态是一个 badger 实例。

`Reader` 应该返回一个 `StorageReader`，它支持在快照上进行键值的点查询和扫描操作。

你现在不需要考虑 `kvrpcpb.Context`，它将在后续项目中使用。

> 提示：
>
> - 你应该使用 [badger.Txn](https://godoc.org/github.com/dgraph-io/badger#Txn) 来实现 `Reader` 函数，因为 badger 提供的事务处理程序可以提供键和值的一致性快照。
> - Badger 不支持列族。engine_util 包（`kv/util/engine_util`）通过为键添加前缀来模拟列族。例如，属于特定列族 `cf` 的键 `key` 存储为 `${cf}_${key}`。它包装了 `badger` 以提供带有 CF 的操作，并且还提供了许多有用的辅助函数。因此，你应该通过 `engine_util` 提供的方法进行所有读/写操作。请阅读 `util/engine_util/doc.go` 了解更多信息。
> - TinyKV 使用原始 `badger` 版本的一个分支，其中包含一些修复，所以只需使用 `github.com/Connor1996/badger` 而不是 `github.com/dgraph-io/badger`。
> - 不要忘记在丢弃之前调用 badger.Txn 的 `Discard()` 并关闭所有迭代器。

#### 实现服务处理器

该项目的最后一步是使用已实现的存储引擎构建原始键值服务处理器，包括 RawGet/RawScan/RawPut/RawDelete。处理器已经为你定义好了，你只需要在 `kv/server/raw_api.go` 中填写实现。完成后，记得运行 `make project1` 来通过测试套件。
