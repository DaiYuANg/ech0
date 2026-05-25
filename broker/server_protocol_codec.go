package broker

import (
	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/lyonbrown4d/ech0/protocol"
	"github.com/lyonbrown4d/ech0/store"
)

func storeHeadersFromProtocol(headers []protocol.MessageHeader) []store.RecordHeader {
	return collectionlist.MapList(collectionlist.NewList(headers...), func(_ int, header protocol.MessageHeader) store.RecordHeader {
		return store.RecordHeader{
			Key:   header.Key,
			Value: append([]byte(nil), header.Value...),
		}
	}).Values()
}

func protocolHeadersFromStore(headers []store.RecordHeader) []protocol.MessageHeader {
	return collectionlist.MapList(collectionlist.NewList(headers...), func(_ int, header store.RecordHeader) protocol.MessageHeader {
		return protocol.MessageHeader{
			Key:   header.Key,
			Value: append([]byte(nil), header.Value...),
		}
	}).Values()
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
