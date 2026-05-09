package store

import (
	"cmp"
	"context"
	"strconv"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/storx/bboltx"
)

func (s *StorxMetadataStore) AllocateTransactionID() (uint64, error) {
	s.txMu.Lock()
	defer s.txMu.Unlock()
	next, err := s.loadNextTransactionID()
	if err != nil {
		return 0, err
	}
	if next == 0 {
		next = 1
	}
	if err := s.transactionCounters.Put(context.Background(), transactionCounterNext, next+1); err != nil {
		return 0, wrapExternal(err, "save next transaction id")
	}
	return next, nil
}

func (s *StorxMetadataStore) SaveTransaction(state TransactionState) error {
	if state.TxID == 0 {
		return E(CodeInvalidArgument, "transaction tx_id is required")
	}
	state = cloneTransactionState(state)
	if err := s.transactions.Put(context.Background(), transactionKey(state.TxID), state); err != nil {
		return wrapExternal(err, "save transaction")
	}
	return s.advanceNextTransactionID(state.TxID + 1)
}

func (s *StorxMetadataStore) LoadTransaction(txID uint64) (*TransactionState, error) {
	state, ok, err := s.transactions.Get(context.Background(), transactionKey(txID))
	if err != nil {
		return nil, wrapExternal(err, "load transaction")
	}
	if !ok {
		var absent *TransactionState
		return absent, nil
	}
	state = cloneTransactionState(state)
	return &state, nil
}

func (s *StorxMetadataStore) ListTransactions() ([]TransactionState, error) {
	out := collectionlist.NewList[TransactionState]()
	err := s.transactions.Walk(context.Background(), func(entry bboltx.Entry[string, TransactionState]) error {
		out.Add(cloneTransactionState(entry.Value))
		return nil
	})
	return out.
		Sort(func(left, right TransactionState) int {
			return cmp.Compare(left.TxID, right.TxID)
		}).
		Values(), wrapExternal(err, "list transactions")
}

func (s *StorxMetadataStore) loadNextTransactionID() (uint64, error) {
	next, ok, err := s.transactionCounters.Get(context.Background(), transactionCounterNext)
	if err != nil {
		return 0, wrapExternal(err, "load next transaction id")
	}
	if ok && next != 0 {
		return next, nil
	}
	return s.computeNextTransactionID()
}

func (s *StorxMetadataStore) computeNextTransactionID() (uint64, error) {
	next := uint64(1)
	err := s.transactions.Walk(context.Background(), func(entry bboltx.Entry[string, TransactionState]) error {
		if entry.Value.TxID >= next {
			next = entry.Value.TxID + 1
		}
		return nil
	})
	return next, wrapExternal(err, "compute next transaction id")
}

func (s *StorxMetadataStore) advanceNextTransactionID(next uint64) error {
	s.txMu.Lock()
	defer s.txMu.Unlock()
	current, err := s.loadNextTransactionID()
	if err != nil {
		return err
	}
	if next <= current {
		return nil
	}
	return wrapExternal(s.transactionCounters.Put(context.Background(), transactionCounterNext, next), "advance next transaction id")
}

func (s *StorxMetadataStore) restoreMetadataTransactions(snapshot Snapshot) error {
	entries := collectionlist.NewListWithCapacity[bboltx.Entry[string, TransactionState]](snapshot.Transactions.Len())
	snapshot.Transactions.Range(func(_ int, state TransactionState) bool {
		if state.TxID != 0 {
			entries.Add(bboltx.Entry[string, TransactionState]{
				Key:   transactionKey(state.TxID),
				Value: cloneTransactionState(state),
			})
		}
		return true
	})
	if err := s.transactions.PutMany(context.Background(), entries.Values()...); err != nil {
		return wrapExternal(err, "restore transactions")
	}
	next := snapshot.NextTransactionID
	if next == 0 {
		computed, err := s.computeNextTransactionID()
		if err != nil {
			return err
		}
		next = computed
	}
	return wrapExternal(s.transactionCounters.Put(context.Background(), transactionCounterNext, next), "restore next transaction id")
}

func transactionKey(txID uint64) string {
	return strconv.FormatUint(txID, 10)
}
