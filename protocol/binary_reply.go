package protocol

func encodeReplyRequest(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, req ReplyRequest) error {
		return writeReplyFields(writer, replyFields{
			routing: replyRoutingFields{
				subject: req.Subject, replyTo: req.ReplyTo, correlationID: req.CorrelationID,
				expiresAtMS: req.ExpiresAtMS, responderID: req.ResponderID,
			},
			payload: req.Payload,
		})
	})
}

func decodeReplyRequest(data []byte, target any) error {
	return decodeWith(data, target, func(reader *binaryReader) (ReplyRequest, error) {
		fields, err := readReplyFields(reader)
		return ReplyRequest{
			Subject: fields.routing.subject, ReplyTo: fields.routing.replyTo,
			CorrelationID: fields.routing.correlationID, ExpiresAtMS: fields.routing.expiresAtMS,
			ResponderID: fields.routing.responderID, Payload: fields.payload,
		}, err
	})
}

func encodeReplyResponse(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, resp ReplyResponse) error {
		return writeSendDirectResponse(writer, SendDirectResponse(resp))
	})
}

func decodeReplyResponse(data []byte, target any) error {
	return decodeWith(data, target, func(reader *binaryReader) (ReplyResponse, error) {
		resp, err := readSendDirectResponse(reader)
		return ReplyResponse(resp), err
	})
}

func encodeReplyErrorRequest(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, req ReplyErrorRequest) error {
		if err := writeReplyRouting(writer, replyRoutingFields{
			subject: req.Subject, replyTo: req.ReplyTo, correlationID: req.CorrelationID,
			expiresAtMS: req.ExpiresAtMS, responderID: req.ResponderID,
		}); err != nil {
			return err
		}
		return writer.writeString(req.Error)
	})
}

func decodeReplyErrorRequest(data []byte, target any) error {
	return decodeWith(data, target, func(reader *binaryReader) (ReplyErrorRequest, error) {
		routing, err := readReplyRouting(reader)
		if err != nil {
			return ReplyErrorRequest{}, err
		}
		errorMessage, err := reader.readString()
		return ReplyErrorRequest{
			Subject: routing.subject, ReplyTo: routing.replyTo, CorrelationID: routing.correlationID,
			ExpiresAtMS: routing.expiresAtMS, ResponderID: routing.responderID, Error: errorMessage,
		}, err
	})
}

func encodeReplyErrorResponse(value any) ([]byte, error) {
	return encodeReplyResponse(value)
}

func decodeReplyErrorResponse(data []byte, target any) error {
	return decodeReplyResponse(data, target)
}

type replyRoutingFields struct {
	subject       string
	replyTo       string
	correlationID string
	expiresAtMS   uint64
	responderID   string
}

type replyFields struct {
	routing replyRoutingFields
	payload []byte
}

func writeReplyFields(writer *binaryWriter, fields replyFields) error {
	if err := writeReplyRouting(writer, fields.routing); err != nil {
		return err
	}
	return writer.writeBytes(fields.payload)
}

func readReplyFields(reader *binaryReader) (replyFields, error) {
	routing, err := readReplyRouting(reader)
	if err != nil {
		return replyFields{}, err
	}
	payload, err := reader.readBytes()
	return replyFields{routing: routing, payload: payload}, err
}

func writeReplyRouting(writer *binaryWriter, fields replyRoutingFields) error {
	if err := writer.writeString(fields.subject); err != nil {
		return err
	}
	if err := writer.writeString(fields.replyTo); err != nil {
		return err
	}
	if err := writer.writeString(fields.correlationID); err != nil {
		return err
	}
	writer.writeU64(fields.expiresAtMS)
	return writer.writeString(fields.responderID)
}

func readReplyRouting(reader *binaryReader) (replyRoutingFields, error) {
	subject, err := reader.readString()
	if err != nil {
		return replyRoutingFields{}, err
	}
	replyTo, err := reader.readString()
	if err != nil {
		return replyRoutingFields{}, err
	}
	correlationID, err := reader.readString()
	if err != nil {
		return replyRoutingFields{}, err
	}
	expiresAtMS, err := reader.readU64()
	if err != nil {
		return replyRoutingFields{}, err
	}
	responderID, err := reader.readString()
	return replyRoutingFields{
		subject: subject, replyTo: replyTo, correlationID: correlationID,
		expiresAtMS: expiresAtMS, responderID: responderID,
	}, err
}
