package broker

import (
	"io"

	json "github.com/goccy/go-json"
)

type jsonRawMessage = json.RawMessage

func marshalJSON(value any) ([]byte, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return nil, wrapBroker("json_marshal_failed", err, "marshal json")
	}
	return payload, nil
}

func unmarshalJSON(data []byte, target any) error {
	if err := json.Unmarshal(data, target); err != nil {
		return wrapBroker("json_unmarshal_failed", err, "unmarshal json")
	}
	return nil
}

func newJSONEncoder(w io.Writer) *json.Encoder {
	return json.NewEncoder(w)
}

func newJSONDecoder(r io.Reader) *json.Decoder {
	return json.NewDecoder(r)
}
