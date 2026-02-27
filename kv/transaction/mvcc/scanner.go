package mvcc

// Scanner 用于从存储层读取多个连续的 key/value 对。它了解存储层的实现
// 并返回适合用户的结果。
// 不变量：要么 scanner 已完成且不能再使用，要么它已准备好立即返回一个值。
type Scanner struct {
	// 你的数据在这里 (4C)。
}

// NewScanner 创建一个新的 scanner，准备从 txn 中的快照读取。
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// 你的代码在这里 (4C)。
	return nil
}

func (scan *Scanner) Close() {
	// 你的代码在这里 (4C)。
}

// Next 返回 scanner 中的下一个 key/value 对。如果 scanner 已耗尽，则返回 `nil, nil, nil`。
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// 你的代码在这里 (4C)。
	return nil, nil, nil
}
