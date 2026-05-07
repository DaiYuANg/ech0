package broker

import (
	"encoding/json"

	"github.com/DaiYuANg/ech0/direct"
	"github.com/DaiYuANg/ech0/store"
)

type requestReplyEnvelope struct {
	Type          string               `json:"type"`
	Subject       string               `json:"subject"`
	CorrelationID string               `json:"correlation_id"`
	ReplyTo       string               `json:"reply_to,omitempty"`
	SenderID      string               `json:"sender_id,omitempty"`
	ExpiresAtMS   uint64               `json:"expires_at_ms,omitempty"`
	Headers       []store.RecordHeader `json:"headers,omitempty"`
	Error         *string              `json:"error,omitempty"`
	Payload       []byte               `json:"payload,omitempty"`
}

func encodeRequestPayload(subject string, opts RequestOptions, correlationID, replyTo string, expiresAtMS uint64, payload []byte) ([]byte, error) {
	return encodeRequestReplyEnvelope(requestReplyEnvelope{
		Type:          requestEnvelopeType,
		Subject:       subject,
		CorrelationID: correlationID,
		ReplyTo:       replyTo,
		SenderID:      opts.InstanceID,
		ExpiresAtMS:   expiresAtMS,
		Headers:       cloneHeaders(opts.Headers),
		Payload:       append([]byte(nil), payload...),
	})
}

func encodeReplyPayload(req RequestMessage, responderID string, payload []byte, errorMessage *string) ([]byte, error) {
	return encodeRequestReplyEnvelope(requestReplyEnvelope{
		Type:          replyEnvelopeType,
		Subject:       req.Subject,
		CorrelationID: req.CorrelationID,
		SenderID:      responderID,
		Error:         cloneStringPointer(errorMessage),
		Payload:       append([]byte(nil), payload...),
	})
}

func encodeRequestReplyEnvelope(envelope requestReplyEnvelope) ([]byte, error) {
	payload, err := json.Marshal(envelope)
	if err != nil {
		return nil, wrapBroker("request_reply_encode_failed", err, "encode request reply envelope")
	}
	return payload, nil
}

func requestFromRecord(subject string, record store.Record) (RequestMessage, error) {
	envelope, err := decodeRequestReplyEnvelope(record.Payload, requestEnvelopeType)
	if err != nil {
		return RequestMessage{}, err
	}
	return RequestMessage{
		Subject:       subject,
		ReplyTo:       envelope.ReplyTo,
		CorrelationID: envelope.CorrelationID,
		SenderID:      envelope.SenderID,
		ExpiresAtMS:   envelope.ExpiresAtMS,
		Headers:       cloneHeaders(envelope.Headers),
		Payload:       append([]byte(nil), envelope.Payload...),
		Record:        cloneRecord(record),
	}, nil
}

func replyFromDirect(message direct.Message) (ReplyMessage, error) {
	envelope, err := decodeRequestReplyEnvelope(message.Payload, replyEnvelopeType)
	if err != nil {
		return ReplyMessage{}, err
	}
	return ReplyMessage{
		Subject:       envelope.Subject,
		CorrelationID: envelope.CorrelationID,
		SenderID:      envelope.SenderID,
		Error:         cloneStringPointer(envelope.Error),
		Payload:       append([]byte(nil), envelope.Payload...),
		Message:       cloneDirectMessage(message),
	}, nil
}

func decodeRequestReplyEnvelope(payload []byte, wantType string) (requestReplyEnvelope, error) {
	var envelope requestReplyEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return requestReplyEnvelope{}, wrapBroker("request_reply_decode_failed", err, "decode request reply envelope")
	}
	if envelope.Type != wantType {
		return requestReplyEnvelope{}, brokerStoreError(store.CodeCodec, "unexpected request reply envelope type %q", envelope.Type)
	}
	if envelope.CorrelationID == "" {
		return requestReplyEnvelope{}, brokerStoreError(store.CodeCodec, "request reply envelope missing correlation_id")
	}
	return envelope, nil
}

func cloneHeaders(headers []store.RecordHeader) []store.RecordHeader {
	out := make([]store.RecordHeader, 0, len(headers))
	for _, header := range headers {
		out = append(out, store.RecordHeader{Key: header.Key, Value: append([]byte(nil), header.Value...)})
	}
	return out
}

func cloneDirectMessage(message direct.Message) direct.Message {
	return direct.Message{
		MessageID:      message.MessageID,
		ConversationID: message.ConversationID,
		Sender:         message.Sender,
		Recipient:      message.Recipient,
		TimestampMS:    message.TimestampMS,
		Payload:        append([]byte(nil), message.Payload...),
	}
}

func cloneStringPointer(value *string) *string {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}

func cloneRecord(record store.Record) store.Record {
	return store.Record{
		Offset:      record.Offset,
		TimestampMS: record.TimestampMS,
		Key:         append([]byte(nil), record.Key...),
		Headers:     cloneHeaders(record.Headers),
		Attributes:  record.Attributes,
		Payload:     append([]byte(nil), record.Payload...),
	}
}
