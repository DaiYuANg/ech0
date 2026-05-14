package discovery

import (
	"context"

	collectionlist "github.com/arcgolabs/collectionx/list"
)

type StaticProvider struct {
	local  Node
	nodes  []Node
	events chan Event
}

func NewStaticProvider(local Node, nodes []Node) *StaticProvider {
	return &StaticProvider{
		local:  local.Clone(),
		nodes:  cloneNodes(nodes),
		events: make(chan Event),
	}
}

func (p *StaticProvider) Start(context.Context) error {
	return nil
}

func (p *StaticProvider) Stop(context.Context) error {
	return nil
}

func (p *StaticProvider) LocalNode() Node {
	return p.local.Clone()
}

func (p *StaticProvider) Nodes() []Node {
	return cloneNodes(p.nodes)
}

func (p *StaticProvider) Events() <-chan Event {
	return p.events
}

func cloneNodes(nodes []Node) []Node {
	if len(nodes) == 0 {
		return nil
	}
	out := collectionlist.NewListWithCapacity[Node](len(nodes))
	for _, node := range nodes {
		out.Add(node.Clone())
	}
	return out.Values()
}
