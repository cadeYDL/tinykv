package latches

import (
	"sync"

	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
)

// Latching 提供 TinyKV 命令的原子性。这不应与 SQL 事务混淆，SQL 事务为多个 TinyKV 命令提供原子性。
// 例如，考虑两个 commit 命令，它们写入多个 key/CF，所以如果它们竞争，
// 则可能写入不一致的数据。通过锁住每个命令可能写入的 key，我们确保
// 两个命令不会竞争写入相同的 key。
//
// latch 是一个按 key 的锁。每个用户 key 只有一个 latch，而不是每个 CF 一个或每个编码 key 一个。
// Latch 只在写入时需要。同一时间只有一个线程可以持有 latch，并且一个命令可能写入的所有 key
// 必须同时被锁定。
//
// Latching 使用一个将 key 映射到 Go WaitGroup 的单一 map 来实现。对这个 map 的访问由互斥锁保护
// 以确保 latching 是原子和一致的。由于互斥锁是全局锁，在实际系统中它会导致无法忍受的竞争。

type Latches struct {
	// 在修改 key 的任何属性之前，线程必须持有该 key 的 latch。`Latches` 将每个被锁住的
	// key 映射到一个 WaitGroup。发现 key 被锁定的线程应该在该 WaitGroup 上等待。
	latchMap map[string]*sync.WaitGroup
	// 用于保护 latchMap 的互斥锁。线程在对 latchMap 进行任何更改时必须持有此互斥锁。
	latchGuard sync.Mutex
	// 可选的验证函数，仅用于测试。
	Validation func(txn *mvcc.MvccTxn, keys [][]byte)
}

// NewLatches 创建一个新的 Latches 对象用于管理数据库的 latch。应该只有一个这样的对象，
// 在所有线程之间共享。
func NewLatches() *Latches {
	l := new(Latches)
	l.latchMap = make(map[string]*sync.WaitGroup)
	return l
}

// AcquireLatches 尝试锁定由 keys 指定的所有 Latch。如果成功，返回 nil。如果任何 key 被
// 锁定，则 AcquireLatches 返回一个 WaitGroup，线程可以使用它在锁空闲时被唤醒。
func (l *Latches) AcquireLatches(keysToLatch [][]byte) *sync.WaitGroup {
	l.latchGuard.Lock()
	defer l.latchGuard.Unlock()

	// 检查我们想要写入的 key 是否都没有被锁定。
	for _, key := range keysToLatch {
		if latchWg, ok := l.latchMap[string(key)]; ok {
			// 返回一个要等待的 wait group。
			return latchWg
		}
	}

	// 所有 Latch 都可用，用一个新的 wait group 锁定它们。
	wg := new(sync.WaitGroup)
	wg.Add(1)
	for _, key := range keysToLatch {
		l.latchMap[string(key)] = wg
	}

	return nil
}

// ReleaseLatches 释放 keysToUnlatch 中所有 key 的 latch。它将唤醒任何阻塞在其中一个
// latch 上的线程。keysToUnlatch 中的所有 key 必须在一次 AcquireLatches 调用中一起被锁定。
func (l *Latches) ReleaseLatches(keysToUnlatch [][]byte) {
	l.latchGuard.Lock()
	defer l.latchGuard.Unlock()

	first := true
	for _, key := range keysToUnlatch {
		if first {
			wg := l.latchMap[string(key)]
			wg.Done()
			first = false
		}
		delete(l.latchMap, string(key))
	}
}

// WaitForLatches 尝试使用 AcquireLatches 锁定 keysToLatch 中的所有 key。如果一个 latch 已经被锁定，
// 则 WaitForLatches 将等待它解锁然后重试。因此 WaitForLatches 可能会阻塞无限长的时间。
func (l *Latches) WaitForLatches(keysToLatch [][]byte) {
	for {
		wg := l.AcquireLatches(keysToLatch)
		if wg == nil {
			return
		}
		wg.Wait()
	}
}

// Validate 调用 Validation 中的函数（如果存在）。
func (l *Latches) Validate(txn *mvcc.MvccTxn, latched [][]byte) {
	if l.Validation != nil {
		l.Validation(txn, latched)
	}
}
