package discovery

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"time"

	collectionlist "github.com/arcgolabs/collectionx/list"
	collectionmapping "github.com/arcgolabs/collectionx/mapping"
)

const defaultWaitInterval = 100 * time.Millisecond

func WaitForAlive(ctx context.Context, provider Provider, minNodes int, interval time.Duration) ([]Node, error) {
	if provider == nil {
		return nil, errors.New("discovery provider is required")
	}
	minNodes, interval = normalizeWaitOptions(minNodes, interval)
	if nodes, ok := enoughAliveNodes(provider, minNodes); ok {
		return nodes, nil
	}
	return waitForAliveLoop(ctx, provider, minNodes, interval)
}

func waitForAliveLoop(ctx context.Context, provider Provider, minNodes int, interval time.Duration) ([]Node, error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	events := provider.Events()
	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("wait for %d discovery nodes: %w", minNodes, ctx.Err())
		case <-events:
			if nodes, ok := enoughAliveNodes(provider, minNodes); ok {
				return nodes, nil
			}
		case <-ticker.C:
			if nodes, ok := enoughAliveNodes(provider, minNodes); ok {
				return nodes, nil
			}
		}
	}
}

func normalizeWaitOptions(minNodes int, interval time.Duration) (int, time.Duration) {
	if minNodes <= 0 {
		minNodes = 1
	}
	if interval <= 0 {
		interval = defaultWaitInterval
	}
	return minNodes, interval
}

func enoughAliveNodes(provider Provider, minNodes int) ([]Node, bool) {
	nodes := AliveNodes(provider.Nodes())
	return nodes, len(nodes) >= minNodes
}

func AliveNodes(nodes []Node) []Node {
	seen := collectionmapping.NewMap[uint64, Node]()
	for _, node := range nodes {
		if !node.Alive() || node.ID == 0 {
			continue
		}
		if existing, ok := seen.Get(node.ID); ok && existing.RaftAddr != "" {
			continue
		}
		seen.Set(node.ID, node.Clone())
	}
	out := collectionlist.NewListWithCapacity[Node](seen.Len())
	seen.Range(func(_ uint64, node Node) bool {
		out.Add(node)
		return true
	})
	return out.Sort(func(left, right Node) int {
		return cmp.Compare(left.ID, right.ID)
	}).Values()
}
