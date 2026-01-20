package protocol

import "encoding/json"

type Envelope struct {
	Api           string          `json:"api"` // publish / subscribe / ack
	Topic         string          `json:"topic"`
	Version       int             `json:"version"`
	CorrelationID string          `json:"correlation_id"`
	Payload       json.RawMessage `json:"payload"`
}

// 可选 helper
func (e *Envelope) Encode() ([]byte, error) {
	return json.Marshal(e)
}

func DecodeEnvelope(data []byte) (*Envelope, error) {
	var env Envelope
	err := json.Unmarshal(data, &env)
	return &env, err
}
