package broker

import (
	"github.com/DaiYuANg/ech0/protocol"
	"github.com/DaiYuANg/ech0/store"
	collectionlist "github.com/arcgolabs/collectionx/list"
)

func storeHeadersFromProtocol(headers []protocol.MessageHeader) []store.RecordHeader {
	out := collectionlist.NewListWithCapacity[store.RecordHeader](len(headers))
	for _, header := range headers {
		out.Add(store.RecordHeader{
			Key:   header.Key,
			Value: append([]byte(nil), header.Value...),
		})
	}
	return out.Values()
}

func protocolHeadersFromStore(headers []store.RecordHeader) []protocol.MessageHeader {
	out := collectionlist.NewListWithCapacity[protocol.MessageHeader](len(headers))
	for _, header := range headers {
		out.Add(protocol.MessageHeader{
			Key:   header.Key,
			Value: append([]byte(nil), header.Value...),
		})
	}
	return out.Values()
}

func isolationFromProtocol(value protocol.FetchIsolation) FetchIsolation {
	if value == protocol.FetchIsolationReadCommitted {
		return FetchIsolationReadCommitted
	}
	return FetchIsolationReadUncommitted
}

func transactionIdentityFromProtocol(identity protocol.TransactionIdentity) TransactionIdentity {
	return TransactionIdentity{
		TxID:          identity.TxID,
		ProducerID:    identity.ProducerID,
		ProducerEpoch: identity.ProducerEpoch,
	}
}
