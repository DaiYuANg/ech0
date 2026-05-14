package memberlist

import (
	"encoding/json"

	hashimemberlist "github.com/hashicorp/memberlist"
	"github.com/lyonbrown4d/ech0/discovery"
)

type delegate struct {
	provider *Provider
	local    discovery.Node
}

func (d *delegate) NodeMeta(limit int) []byte {
	raw, err := json.Marshal(nodeMetadataFromNode(d.local))
	if err != nil || len(raw) > limit {
		return nil
	}
	return raw
}

func (d *delegate) NotifyMsg([]byte) {}

func (d *delegate) GetBroadcasts(_, _ int) [][]byte {
	return nil
}

func (d *delegate) LocalState(bool) []byte {
	return nil
}

func (d *delegate) MergeRemoteState([]byte, bool) {}

func (d *delegate) NotifyJoin(node *hashimemberlist.Node) {
	d.provider.emit(discovery.EventJoin, nodeFromMember(node))
}

func (d *delegate) NotifyLeave(node *hashimemberlist.Node) {
	d.provider.emit(discovery.EventLeave, nodeFromMember(node))
}

func (d *delegate) NotifyUpdate(node *hashimemberlist.Node) {
	d.provider.emit(discovery.EventUpdate, nodeFromMember(node))
}
