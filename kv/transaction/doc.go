package transaction

// transaction 包实现了 TinyKV 的"事务"层。它接收来自 kv/server/server.go 的请求作为输入，
// 并将它们转换为对底层 key/value 存储的读写操作（由 kv/storage/storage.go 中的 Storage 定义）。
// 存储引擎处理与其他节点的通信和将数据写入磁盘。事务层必须将高级 TinyKV 命令
// 转换为底层原始 key/value 命令，并确保命令的处理不会干扰其他命令的处理。
//
// 注意这里涉及两种事务：TinySQL 事务是 TinyKV 与其客户端（例如 TinySQL）之间的协作。
// 它们使用多个 TinyKV 请求实现，并确保多个 SQL 命令可以原子执行。
// 还有 mvcc 事务，它是 TinyKV 中这一层的实现细节（由 kv/transaction/mvcc/transaction.go 中的 MvccTxn 表示）。
// 这些确保*单个*请求被原子执行。
//
// *锁*用于实现 TinySQL 事务。在 TinySQL 事务中设置或检查锁会降级为写入底层存储。
//
// *Latch*用于实现 mvcc 事务，对客户端不可见。它们存储在底层存储之外
// （或者等效地，你可以认为每个 key 都有自己的 latch）。详见 latches 包。
//
// 在 `mvcc` 包中，`Lock` 和 `Write` 提供了将锁和写操作降级为简单 key 和 value 的抽象。
//
// ## 编码用户 key/value
//
// mvcc 策略本质上是在每个时间点存储所有数据（已提交和未提交的）。所以例如，如果我们为一个 key
// 存储一个值，然后在稍后的时间存储另一个值（逻辑覆盖），两个值都会保留在底层存储中。
//
// 这是通过将用户 key 与它们的时间戳（写入它们的事务的开始时间戳）编码来实现的，
// 生成一个编码的 key（见 codec.go）。`default` CF 是从编码 key 到它们的值的映射。
//
// 锁定一个 key 意味着写入 `lock` CF。在这个 CF 中，我们使用用户 key（即不是编码的 key，
// 这样一个 key 对所有时间戳都被锁定）。`lock` CF 中的值由事务的"主键"、锁的类型（'put'、
// 'delete' 或 'rollback'）、事务的开始时间戳和锁的 ttl（生存时间）组成。见 lock.go 的实现。
//
// 值的状态存储在 `write` CF 中。这里我们将用提交时间戳编码的 key（即事务提交的时间）
// 映射到包含事务开始时间戳和写类型（'put'、'delete' 或 'rollback'）的值。
// 注意对于回滚的事务，编码 key 中的提交时间戳使用开始时间戳。
