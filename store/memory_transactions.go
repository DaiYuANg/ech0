package store

import (
	"cmp"

	collectionlist "github.com/arcgolabs/collectionx/list"
)

func (s *MemoryStore) AllocateTransactionID() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.nextTxID == 0 {
		s.nextTxID = 1
	}
	txID := s.nextTxID
	s.nextTxID++
	return txID, nil
}

func (s *MemoryStore) SaveTransaction(state TransactionState) error {
	if state.TxID == 0 {
		return E(CodeInvalidArgument, "transaction tx_id is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.transactions.Set(state.TxID, cloneTransactionState(state))
	if state.TxID >= s.nextTxID {
		s.nextTxID = state.TxID + 1
	}
	return nil
}

func (s *MemoryStore) LoadTransaction(txID uint64) (*TransactionState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	state, ok := s.transactions.Get(txID)
	if !ok {
		var absent *TransactionState
		return absent, nil
	}
	state = cloneTransactionState(state)
	return &state, nil
}

func (s *MemoryStore) ListTransactions() ([]TransactionState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := collectionlist.NewListWithCapacity[TransactionState](s.transactions.Len())
	s.transactions.Range(func(_ uint64, state TransactionState) bool {
		out.Add(cloneTransactionState(state))
		return true
	})
	return out.
		Sort(func(left, right TransactionState) int {
			return cmp.Compare(left.TxID, right.TxID)
		}).
		Values(), nil
}
