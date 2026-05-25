// Package binary exposes the official binary body codec entry points.
package binary

import (
	"fmt"

	"github.com/lyonbrown4d/ech0/protocol"
)

// EncodeBody encodes a protocol message body for command.
func EncodeBody(command uint16, value any) ([]byte, error) {
	body, err := protocol.EncodeBody(command, value)
	if err != nil {
		return nil, fmt.Errorf("encode protocol body: %w", err)
	}
	return body, nil
}

// DecodeBody decodes a protocol message body for command into target.
func DecodeBody(command uint16, data []byte, target any) error {
	if err := protocol.DecodeBody(command, data, target); err != nil {
		return fmt.Errorf("decode protocol body: %w", err)
	}
	return nil
}

// CommandIDs returns every command ID registered by the binary codec.
func CommandIDs() []uint16 {
	return protocol.CommandIDs()
}
