package protocol

import (
	collectionlist "github.com/arcgolabs/collectionx/list"
	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	"github.com/samber/oops"
)

type bodyEncoder func(any) ([]byte, error)
type bodyDecoder func([]byte, any) error

type bodyCodecEntry struct {
	command uint16
	encode  bodyEncoder
	decode  bodyDecoder
}

type bodyCodecRegistry struct {
	encoders *collectionmapping.Map[uint16, bodyEncoder]
	decoders *collectionmapping.Map[uint16, bodyDecoder]
	commands []uint16
}

var bodyCodecs = newBodyCodecRegistry()

func EncodeBody(command uint16, value any) ([]byte, error) {
	encoder, ok := bodyCodecs.encoders.Get(command)
	if !ok {
		return nil, unsupportedCommand(command)
	}
	return encoder(value)
}

func DecodeBody(command uint16, data []byte, target any) error {
	decoder, ok := bodyCodecs.decoders.Get(command)
	if !ok {
		return unsupportedCommand(command)
	}
	return decoder(data, target)
}

func registeredCommandIDs() []uint16 {
	return collectionlist.NewList(bodyCodecs.commands...).Values()
}

func newBodyCodecRegistry() bodyCodecRegistry {
	encoders := collectionmapping.NewMap[uint16, bodyEncoder]()
	decoders := collectionmapping.NewMap[uint16, bodyDecoder]()
	commands := collectionlist.NewList[uint16]()
	for _, entry := range bodyCodecEntries().Values() {
		encoders.Set(entry.command, entry.encode)
		decoders.Set(entry.command, entry.decode)
		commands.Add(entry.command)
	}
	return bodyCodecRegistry{encoders: encoders, decoders: decoders, commands: commands.Values()}
}

func unsupportedCommand(command uint16) error {
	return oops.In("protocol").Code("unsupported_command").With("command", command).New("unsupported command")
}
