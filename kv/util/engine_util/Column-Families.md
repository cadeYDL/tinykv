# RocksDB 列族（Column Families）

> 原文：https://github.com/facebook/rocksdb/wiki/Column-Families

## 简介

RocksDB 3.0 版本引入了对列族（Column Families）的支持。

RocksDB 中的每个键值对都恰好关联一个列族。如果没有指定列族，键值对会被关联到 "default" 列族。

列族提供了一种对数据库进行逻辑分区的方式。以下是一些有用的特性：

- 支持跨列族的原子写入。这意味着你可以原子地执行 `Write({cf1, key1, value1}, {cf2, key2, value2})`。
- 支持跨列族的一致性视图。
- 能够对不同的列族进行独立配置。
- 支持动态添加和删除列族，两种操作都相当快。

## API

### 向后兼容性

虽然为了支持列族我们需要对 API 进行重大修改，但我们仍然支持旧的 API。升级到 RocksDB 3.0 时不需要对应用程序做任何更改。通过旧 API 插入的所有键值对都会被插入到 "default" 列族中。降级同理。如果你从未使用过多个列族，磁盘格式不会有任何变化，这意味着你可以安全地回滚到 RocksDB 2.8。这对 Facebook 内部的用户来说非常重要。

### 使用示例

https://github.com/facebook/rocksdb/blob/main/examples/column_families_example.cc

### 参考

```cpp
Options, ColumnFamilyOptions, DBOptions
```

定义在 [include/rocksdb/options.h](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/options.h) 中，`Options` 结构体定义了 RocksDB 的行为和性能表现。之前所有选项都定义在单一的 `Options` 结构体中。今后，特定于单个列族的选项将定义在 `ColumnFamilyOptions` 中，而特定于整个 RocksDB 实例的选项将定义在 `DBOptions` 中。`Options` 结构体同时继承了 `ColumnFamilyOptions` 和 `DBOptions`，因此你仍然可以用它为只有单个（默认）列族的数据库实例定义所有选项。

```cpp
ColumnFamilyHandle
```

列族通过 `ColumnFamilyHandle` 来管理和引用。可以把它理解为一个打开的文件描述符。你需要在删除 DB 指针之前删除所有的 `ColumnFamilyHandle`。一个有趣的特点是：即使 `ColumnFamilyHandle` 指向的是一个已被删除的列族，你仍然可以继续使用它。数据只有在所有关联的 `ColumnFamilyHandle` 都被删除之后才会被真正删除。

```cpp
DB::Open(const DBOptions& db_options, const std::string& name, const std::vector<ColumnFamilyDescriptor>& column_families, std::vector<ColumnFamilyHandle*>* handles, DB** dbptr);
```

以读写模式打开数据库时，你需要指定数据库中当前存在的所有列族。如果不这样做，`DB::Open` 调用将返回 `Status::InvalidArgument()`。你通过 `ColumnFamilyDescriptor` 的 vector 来指定列族。`ColumnFamilyDescriptor` 只是一个包含列族名称和 `ColumnFamilyOptions` 的结构体。Open 调用将返回一个 `Status` 以及一个 `ColumnFamilyHandle` 指针的 vector，之后你可以用它们来引用列族。确保在删除 DB 指针之前删除所有的 `ColumnFamilyHandle`。

```cpp
DB::OpenForReadOnly(const DBOptions& db_options, const std::string& name, const std::vector<ColumnFamilyDescriptor>& column_families, std::vector<ColumnFamilyHandle*>* handles, DB** dbptr, bool error_if_log_file_exist = false)
```

行为类似于 `DB::Open`，但以只读模式打开数据库。一个重要的区别是：以只读模式打开数据库时，不需要指定所有列族——你可以只打开列族的一个子集。

```cpp
DB::ListColumnFamilies(const DBOptions& db_options, const std::string& name, std::vector<std::string>* column_families)
```

`ListColumnFamilies` 是一个静态函数，返回数据库中当前存在的所有列族的列表。

```cpp
CreateColumnFamily(const ColumnFamilyOptions& options, const std::string& column_family_name, ColumnFamilyHandle** handle)
```

使用指定的选项和名称创建一个列族，并通过参数返回 `ColumnFamilyHandle`。

```cpp
DropColumnFamily(ColumnFamilyHandle* column_family)
```

删除由 `ColumnFamilyHandle` 指定的列族。注意，实际数据在客户端调用 `delete column_family;` 之前不会被删除。如果你仍然持有 `ColumnFamilyHandle` 指针，可以继续使用该列族。

```cpp
DB::NewIterators(const ReadOptions& options, const std::vector<ColumnFamilyHandle*>& column_families, std::vector<Iterator*>* iterators)
```

这是一个新的调用，允许你在多个列族上创建具有一致性数据库视图的迭代器。

#### WriteBatch

要原子地执行多个写操作，你需要构建一个 `WriteBatch`。所有 `WriteBatch` 的 API 调用现在也接受 `ColumnFamilyHandle*` 参数，以指定要写入的列族。

#### 其他所有 API 调用

所有其他 API 调用都增加了一个新的 `ColumnFamilyHandle*` 参数，通过它你可以指定列族。

## 实现原理

列族背后的核心思想是：**它们共享预写日志（WAL），但不共享内存表（memtable）和表文件（table file）**。通过共享预写日志，我们获得了原子写入的巨大优势。通过分离内存表和表文件，我们能够独立配置各个列族并快速删除它们。

每当某个列族被刷盘（flush）时，我们会创建一个新的 WAL（预写日志）。所有列族的新写入都会写入到新的 WAL 中。然而，我们仍然不能删除旧的 WAL，因为它包含了来自其他列族的活跃数据。只有当所有列族都已刷盘，且该 WAL 中包含的所有数据都已持久化到表文件中之后，我们才能删除旧的 WAL。这带来了一些有趣的实现细节，也会产生有趣的调优需求。请确保调优你的 RocksDB，使所有列族都能定期刷盘。另外，可以关注 `Options::max_total_wal_size` 选项，通过配置该选项可以自动刷盘不活跃的列族。
