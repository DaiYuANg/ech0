package store

func restoreMemoryNextTransactionID(snapshot Snapshot) uint64 {
	next := snapshot.NextTransactionID
	if next == 0 {
		next = 1
	}
	snapshot.Transactions.Range(func(_ int, state TransactionState) bool {
		if state.TxID >= next {
			next = state.TxID + 1
		}
		return true
	})
	return next
}

func cloneBrokerState(state *BrokerState) *BrokerState {
	if state == nil {
		return nil
	}
	cp := *state
	return &cp
}
