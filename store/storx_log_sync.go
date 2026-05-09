package store

import (
	"errors"
	"time"

	collectionset "github.com/arcgolabs/collectionx/set"
)

const appendGroupCommitDelay = time.Millisecond

type appendDurabilityCoordinator struct {
	mu    chan struct{}
	delay time.Duration
	round *appendDurabilityRound
}

type appendDurabilityRound struct {
	done         chan struct{}
	flushing     bool
	segmentPaths *collectionset.Set[string]
	indexPaths   *collectionset.Set[string]
	err          error
}

func newAppendDurabilityCoordinator() *appendDurabilityCoordinator {
	return &appendDurabilityCoordinator{
		mu:    make(chan struct{}, 1),
		delay: appendGroupCommitDelay,
	}
}

func (c *appendDurabilityCoordinator) commit(s *StorxLogStore, segmentPaths, indexPaths []string) error {
	if c == nil {
		return s.syncAppendWriters(segmentPaths, indexPaths)
	}
	c.lock()
	round := c.round
	if round == nil {
		round = &appendDurabilityRound{
			done:         make(chan struct{}),
			segmentPaths: collectionset.NewSet[string](),
			indexPaths:   collectionset.NewSet[string](),
		}
		c.round = round
		go c.flushAfter(s, round)
	}
	round.segmentPaths.Add(segmentPaths...)
	round.indexPaths.Add(indexPaths...)
	c.unlock()
	return nil
}

func (c *appendDurabilityCoordinator) flushAfter(s *StorxLogStore, round *appendDurabilityRound) {
	time.Sleep(c.delay)
	c.flushRound(s, round)
}

func (c *appendDurabilityCoordinator) flushPending(s *StorxLogStore) error {
	c.lock()
	round := c.round
	c.unlock()
	if round == nil {
		return nil
	}
	c.flushRound(s, round)
	<-round.done
	return round.err
}

func (c *appendDurabilityCoordinator) flushRound(s *StorxLogStore, round *appendDurabilityRound) {
	c.lock()
	if round.flushing {
		done := round.done
		c.unlock()
		<-done
		return
	}
	round.flushing = true
	if c.round == round {
		c.round = nil
	}
	segmentPaths := round.segmentPaths.Values()
	indexPaths := round.indexPaths.Values()
	c.unlock()
	round.err = s.syncAppendWriters(segmentPaths, indexPaths)
	close(round.done)
}

func (c *appendDurabilityCoordinator) lock() {
	c.mu <- struct{}{}
}

func (c *appendDurabilityCoordinator) unlock() {
	<-c.mu
}

func (s *StorxLogStore) syncAppendWrites(segmentPaths, indexPaths []string) error {
	if len(segmentPaths) == 0 && len(indexPaths) == 0 {
		return nil
	}
	return s.appendSyncer.commit(s, segmentPaths, indexPaths)
}

func (s *StorxLogStore) syncAppendWriters(segmentPaths, indexPaths []string) error {
	return errors.Join(
		s.syncSegmentWriters(segmentPaths),
		s.syncSegmentIndexWriters(indexPaths),
	)
}

func (s *StorxLogStore) syncSegmentWriters(paths []string) error {
	if len(paths) == 0 {
		return nil
	}
	var result error
	for _, path := range collectionset.NewSet(paths...).Values() {
		s.writersMu.Lock()
		writer, ok := s.writers.Get(path)
		s.writersMu.Unlock()
		if ok {
			result = errors.Join(result, writer.sync())
		}
	}
	return result
}

func (s *StorxLogStore) syncSegmentIndexWriters(paths []string) error {
	if len(paths) == 0 {
		return nil
	}
	var result error
	for _, path := range collectionset.NewSet(paths...).Values() {
		s.indexWritersMu.Lock()
		writer, ok := s.indexWriters.Get(path)
		s.indexWritersMu.Unlock()
		if ok {
			result = errors.Join(result, writer.sync())
		}
	}
	return result
}
