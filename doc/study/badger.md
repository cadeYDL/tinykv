# Badger 存储库深度解析

Badger 是一个用 Go 语言编写的快速、嵌入式 K/V 数据库。它是 Dgraph 项目的核心组件之一，旨在解决传统 LSM 树数据库在 SSD 上的写放大和读取延迟问题。TinyKV 使用的是其优化版本（`Connor1996/badger`）。

---

## 1. 核心设计：键值分离 (WiscKey 架构)

Badger 最显著的特性是其 **WiscKey** 设计。传统的 LSM 树（如 LevelDB, RocksDB）将 Key 和 Value 都存储在 SSTable 中。随着数据的不断压缩（Compaction），相同的 Value 可能会被多次重写，导致严重的**写放大 (Write Amplification)**。

### Badger 的改进：
- **LSM Tree**: 仅存储 Key、元数据以及指向 Value 存储位置的指针。
- **Value Log (vLog)**: 所有的 Value 都顺序追加到一个或多个只写文件（vLog）中。

**优势**：
- **极小的 LSM 树**：由于不存 Value，LSM 树变得非常轻量，压缩过程极快，写放大显著降低。
- **并行读取**：由于 vLog 是顺序写的，对于 SSD，可以通过偏移量进行高效的随机并发读取。

---

## 2. 核心读写流程

### 2.1 写入操作 (Write Flow)
1. **事务开始**: 调用 `db.Update()`。
2. **预写 Value Log**: 数据被顺序追加到 vLog 文件末尾。
3. **写入 Memtable**: 获取 vLog 中的偏移量和大小，将 Key 及其位置信息（Pointer）写入跳表（SkipList）结构的 Memtable。
4. **Flush**: 当 Memtable 达到阈值时，将其冻结并后台异步刷入磁盘，形成 L0 层的 SSTable。

### 2.2 读取操作 (Read Flow)
1. **Memtable 查询**: 首先在活跃和只读的 Memtable 中查找 Key。
2. **SSTable 查询**: 如果没找到，逐层在 LSM 树（L0 -> Lk）中搜索。
3. **解引用 Value**: LSM 树返回的是 Value 指针（vPointer）。
4. **读取 vLog**: 根据 vPointer 提供的偏移量，从磁盘的 vLog 文件中读取真实的 Value 数据。

---

## 3. 核心特性

- **纯 Go 实现**: 无需 CGO，易于跨平台分发。
- **ACID 事务**: 支持并发的串行化快照隔离（SSI）事务。
- **高度并发**: 支持多读一写，读取性能随 CPU 核心数线性增长。
- **TTL 支持**: 可以为每个 Key 设置过期时间。
- **压缩策略**: 提供灵活的 vLog 垃圾回收机制。

---

## 4. 用户使用场景

### 适用场景：
- **写密集型应用**: 由于写放大极低，特别适合高频写入。
- **大 Value 场景**: 当 Value 较大（如数百 KB 到 MB 级别）时，Badger 的性能优势远超传统 LSM 数据库。
- **分布式系统底层存储**: 如 TinyKV, TiKV, Dgraph 等分布式数据库的本地引擎。
- **边缘计算/嵌入式**: 需要高性能持久化且不想引入 C 库依赖的 Go 项目。

---

## 5. 常用 API 与示例

### 5.1 打开数据库
```go
opts := badger.DefaultOptions
opts.Dir = "/tmp/badger"
opts.ValueDir = "/tmp/badger"
db, err := badger.Open(opts)
if err != nil {
    log.Fatal(err)
}
defer db.Close()
```

### 5.2 写入与更新 (Update)
`Update` 开启一个可读写的事务。
```go
err := db.Update(func(txn *badger.Txn) error {
    err := txn.Set([]byte("key"), []byte("value"))
    return err
})
```

### 5.3 读取 (View)
`View` 开启一个只读事务。
```go
err := db.View(func(txn *badger.Txn) error {
    item, err := txn.Get([]byte("key"))
    if err != nil {
        return err
    }
    err = item.Value(func(val []byte) error {
        fmt.Printf("The value is: %s\n", val)
        return nil
    })
    return err
})
```

### 5.4 遍历 (Iterator)
```go
err := db.View(func(txn *badger.Txn) error {
    it := txn.NewIterator(badger.DefaultIteratorOptions)
    defer it.Close()
    for it.Rewind(); it.Valid(); it.Next() {
        item := it.Item()
        k := item.Key()
        err := item.Value(func(v []byte) error {
            fmt.Printf("key=%s, value=%s\n", k, v)
            return nil
        })
        if err != nil {
            return err
        }
    }
    return nil
})
```

### 5.5 垃圾回收 (vLog GC)
// ... (保持原样)

---

## 6. 关键配置项 (Options) 深度指南

Badger 的性能高度依赖于 `Options` 的调优。以下是核心参数的详细解析：

### 6.1 核心参数列表

| 参数名 | 默认值 | 含义 | 调优建议 |
| :--- | :--- | :--- | :--- |
| `SyncWrites` | `false` | 是否每次写入强制刷盘 | 追求速度选 `false`；追求数据绝对安全选 `true`。 |
| `ValueThreshold` | `1024` | 键值分离阈值 (Byte) | **最核心参数**。Value 大于此值进入 vLog。调大可减少磁盘寻址，调小可降低写放大。 |
| `MaxTableSize` | `64MB` | 单个 SSTable 大小 | 影响压缩频率。对于超大数据集，可调大至 128MB+。 |
| `NumMemtables` | `5` | 内存表数量 | 内存充足时可调大 (如 10-15) 以吸收写入峰值。 |
| `ValueLogFileSize` | `1GB` | vLog 文件切分大小 | 影响文件数量。文件系统对超大目录性能不佳时建议保持默认。 |
| `ReadOnly` | `false` | 只读模式 | 设置为 `true` 后支持多进程并发读取同一个 DB。 |

### 6.2 文件加载模式 (`FileLoadingMode`)
- **`MemoryMap` (默认)**: 利用 OS 的 `mmap`。性能最好，但消耗大量虚拟内存，适合 64 位系统且内存充裕。
- **`FileIO`**: 传统的 `read/write`。性能稍逊，但内存占用可控，适合 32 位系统或内存极度受限场景。

### 6.3 典型场景配置方案

#### 方案一：高频小数据写入 (High TPS)
```go
opts := badger.DefaultOptions
opts.Dir = path
opts.ValueDir = path
opts.SyncWrites = false
opts.ValueThreshold = 256 // 较小的值，减轻 LSM 树负担
opts.NumMemtables = 10    // 增加缓冲区
```

#### 方案二：海量大 Value 存储 (Object Store)
```go
opts := badger.DefaultOptions
opts.Dir = path
opts.ValueDir = path
opts.ValueThreshold = 4096 // 4KB 以下不分离，减少小文件寻址
opts.ValueLogLoadingMode = options.FileIO // 大 Value 建议禁用 mmap 防止地址空间耗尽
```

#### 方案三：嵌入式/低功耗设备 (Low Memory)
```go
opts := badger.DefaultOptions
opts.Dir = path
opts.ValueDir = path
opts.TableLoadingMode = options.FileIO
opts.ValueLogLoadingMode = options.FileIO
opts.MaxCacheSize = 16 << 20 // 限制缓存 16MB
opts.NumMemtables = 2
```

---

## 6. GC (垃圾回收) 深度解析

Badger 的 GC 采用的是 **复制-移动 (Copy-and-Move)** 策略，而非原地清理。

### 详细步骤：
1. **采样 (Sampling)**：选择一个较旧的 vLog 文件，评估其过期数据比例。
2. **校验 (Verification)**：读取文件中的每一条记录，通过 Key 回查 LSM 树。如果 LSM 树中的指针仍指向该位置，则判定为“有效数据”。
3. **搬迁 (Relocation)**：将所有有效数据重新追加写入到**当前的活跃 vLog 文件（Head）**。
4. **更新索引 (Re-indexing)**：在 LSM 树中将这些 Key 指向新的 vLog 偏移量。
5. **物理删除 (Truncation)**：一旦整个旧文件中的有效数据全部移出，**直接从磁盘删除该旧文件**。

### 为什么不违背顺序写原则？
- **空间释放是文件级的**：Badger 不会在文件内部“挖洞”，而是通过搬迁有效数据后直接抹除旧文件。
- **始终向后看**：所有的“重写”操作都发生在日志的最前端，确保了磁盘 IO 始终是顺序的。
- **碎片处理**：虽然 GC 会消耗额外的写入带宽，但它彻底解决了逻辑上的空间浪费，同时避免了物理层面的文件碎片。

---

## 7. 并发安全保障 (Concurrency Safety)

在 GC 搬迁过程中，Badger 如何防止覆盖用户的新写入？

### 核心机制：指针校验 (Pointer Validation)
1. **CAS 更新**：在将数据从旧 vLog 搬迁到新 vLog 后，Badger 会启动一个事务来更新 LSM 树索引。
2. **冲突检测**：在更新索引的那一刻，系统会检查：**“当前 LSM 树中的指针是否仍然指向搬迁前的位置？”**
    - **一致**：说明用户没有修改过这个 Key，安全地将指针更新为新位置。
    - **不一致**：说明在 GC 过程中，用户已经写入了更高版本的数据。此时 GC 事务会**丢弃**这次搬迁结果，以用户的新数据为准。

### 读写安全：
- **MVCC 快照读**：读请求不会被 GC 阻塞。即便索引已更新，只要旧文件的引用计数不为零（仍有读事务在访问），文件就不会被删除。
- **写写互斥**：GC 的索引更新动作与用户的 `Set` 操作共享相同的事务冲突检测逻辑，确保了索引状态的最终一致性。

---

## 8. 深度问答 (Internal FAQ)

### Q1: vLog 和 WAL 的关系是什么？
在 Badger 中，**vLog 实际上替代了传统的 WAL**。因为写入时数据先进入 vLog 再进 Memtable，vLog 包含了恢复内存数据所需的所有信息（Key + Value）。崩溃重启时，Badger 通过重放 vLog 来重建 Memtable。

### Q2: vLog 有去重功能吗？
**没有。** vLog 是严格追加的。同一个 Key 的多次修改会产生多条记录。真实的“去重”发生在读取时（LSM 树只指向最新位置）和 **GC 阶段**（回收旧值占用的空间）。

### Q3: vLog 是如何组织的？
vLog 由多个**分段文件**组成（如 `000001.vlog`）。每个文件大小固定（默认 1GB）。当一个写满时，会自动切换到下一个编号。

### Q4: 索引具体记录了什么？
LSM 树中存储的 `vPointer` 包含：`FileID` (哪一个文件)、`Offset` (起始偏移量) 和 `Len` (数据长度)。

### Q5: 文件末尾的空间碎片如何处理？
如果当前文件剩余空间不足以容纳下一个 Value，Badger 会**直接开启新文件**。剩余的微小空间会被废弃，不会在之后尝试填补。这样做是为了保证**绝对的顺序写入**，从而获得最高性能。

### Q6: 为什么 vLog 不再用一颗 LSM 树来维护？
如果 vLog 也用 LSM 树，Value 就会参与频繁的 Compaction（压缩合并），这会重新引入**写放大**问题。Badger 的核心就是**解耦**：用 LSM 树管理极小的 Key 索引（保持高性能排序），用简单的追加日志（vLog）管理大 Value（保持最高写入吞吐）。

### Q7: vLog 中同时存储 Key 和 Value，岂不是空间浪费？
是的，Key 在 LSM 树和 vLog 中各存了一次。但这是一种**权衡 (Trade-off)**：
1. **数据安全性**：vLog 变成了自描述的备份。即使索引损坏，也能通过 vLog 重建。
2. **GC 必要性**：GC 时需要根据 vLog 中的 Key 去校验索引，判断该条目是否为过期的旧版本。
**结论**：由于 Key 通常远小于 Value，这种少量的空间牺牲换来了极高的写入速度和系统健壮性。
