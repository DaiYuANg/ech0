package client

import (
	"context"

	"github.com/lyonbrown4d/ech0/protocol"
)

type Direct struct {
	client *Client
}

func (c *Client) Direct() *Direct {
	return &Direct{client: c}
}

func (d *Direct) Send(
	ctx context.Context,
	sender string,
	recipient string,
	payload []byte,
	conversationID *string,
) (protocol.SendDirectResponse, error) {
	req := protocol.SendDirectRequest{Sender: sender, Recipient: recipient, ConversationID: conversationID, Payload: payload}
	var out protocol.SendDirectResponse
	err := d.client.RoundTrip(ctx, protocol.CmdSendDirectRequest, protocol.CmdSendDirectResponse, req, &out)
	return out, err
}

func (d *Direct) FetchInbox(ctx context.Context, recipient string, maxRecords int) (protocol.FetchInboxResponse, error) {
	req := protocol.FetchInboxRequest{Recipient: recipient, MaxRecords: maxRecords}
	var out protocol.FetchInboxResponse
	err := d.client.RoundTrip(ctx, protocol.CmdFetchInboxRequest, protocol.CmdFetchInboxResponse, req, &out)
	return out, err
}

func (d *Direct) Ack(ctx context.Context, recipient string, nextOffset uint64) (protocol.AckDirectResponse, error) {
	req := protocol.AckDirectRequest{Recipient: recipient, NextOffset: nextOffset}
	var out protocol.AckDirectResponse
	err := d.client.RoundTrip(ctx, protocol.CmdAckDirectRequest, protocol.CmdAckDirectResponse, req, &out)
	return out, err
}
