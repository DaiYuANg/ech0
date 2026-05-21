package broker

import (
	"context"

	"github.com/lyonbrown4d/ech0/direct"
)

func (b *Broker) SendDirect(ctx context.Context, sender, recipient string, conversationID *string, payload []byte) (direct.SendResult, error) {
	identity := b.identity(ctx)
	if err := b.authorize(ctx, identity, ACLActionProduce, directInboxResource(identity, recipient)); err != nil {
		return direct.SendResult{}, err
	}
	req := directCommand{
		Sender:         scopedName(identity, "direct", sender),
		Recipient:      scopedName(identity, "direct", recipient),
		ConversationID: conversationID,
		Payload:        append([]byte(nil), payload...),
	}
	return routePartitionCommand(ctx, b, topicCommandTarget(direct.InternalInboxTopicPrefix), raftCommandDirectSend, req, b.applyDirectSend)
}

func (b *Broker) FetchInbox(recipient string, maxRecords int) (direct.FetchInboxResult, error) {
	return b.FetchInboxFor(context.Background(), recipient, maxRecords)
}

func (b *Broker) FetchInboxFor(ctx context.Context, recipient string, maxRecords int) (direct.FetchInboxResult, error) {
	identity := b.identity(ctx)
	if err := b.authorize(ctx, identity, ACLActionConsume, directInboxResource(identity, recipient)); err != nil {
		return direct.FetchInboxResult{}, err
	}
	if maxRecords <= 0 || maxRecords > b.cfg.Broker.MaxFetchRecords {
		maxRecords = b.cfg.Broker.MaxFetchRecords
	}
	scopedRecipient := scopedName(identity, "direct", recipient)
	inbox, err := b.direct.FetchInbox(scopedRecipient, maxRecords)
	if err != nil {
		return direct.FetchInboxResult{}, wrapBroker("fetch_inbox_failed", err, "fetch inbox")
	}
	inbox.Recipient = recipient
	for index := range inbox.Records {
		message := &inbox.Records[index].Message
		message.Sender = visibleName(identity, "direct", message.Sender)
		message.Recipient = visibleName(identity, "direct", message.Recipient)
	}
	return inbox, nil
}

func (b *Broker) AckDirect(ctx context.Context, recipient string, nextOffset uint64) error {
	identity := b.identity(ctx)
	if err := b.authorize(ctx, identity, ACLActionCommit, directInboxResource(identity, recipient)); err != nil {
		return err
	}
	req := ackDirectCommand{Recipient: scopedName(identity, "direct", recipient), NextOffset: nextOffset}
	_, err := routePartitionCommand(ctx, b, topicCommandTarget(direct.InternalInboxTopicPrefix), raftCommandDirectAck, req, b.applyDirectAck)
	return err
}
