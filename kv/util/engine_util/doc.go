package engine_util

/*
engine 是一个用于在本地存储 key/value 对的底层系统（不包含分布式或任何事务支持等）。
这个包包含与这些引擎交互的代码。

CF 意思是 'column family'（列族）。列族的详细描述可以在 https://github.com/facebook/rocksdb/wiki/Column-Families
（专门针对 RocksDB，但一般概念是通用的）中找到。简而言之，列族是一个 key 命名空间。
多个列族通常被实现为几乎独立的数据库。重要的是每个列族可以单独配置。
写操作可以跨列族原子执行，这对于独立的数据库是做不到的。

engine_util 包含以下包：

* engines：用于保存 unistore 所需引擎的数据结构。
* write_batch：将写操作批量处理成单个原子"事务"的代码。
* cf_iterator：在 badger 中遍历整个列族的代码。
*/
