package store

import collectionlist "github.com/arcgolabs/collectionx/list"

const (
	appendPipelineQueueSize   = 1024
	appendPipelineMaxRequests = 1024
	appendPipelineMaxRecords  = 8192
)

type appendPipeline struct {
	store    *StorxLogStore
	tp       TopicPartition
	requests chan appendPipelineRequest
	done     chan struct{}
}

type appendPipelineRequest struct {
	records []RecordAppend
	result  chan appendPipelineResult
}

type appendPipelineResult struct {
	records []Record
	err     error
}

func (s *StorxLogStore) appendPipeline(topicPartition TopicPartition) *appendPipeline {
	s.appendPipesMu.Lock()
	defer s.appendPipesMu.Unlock()
	pipeline, ok := s.appendPipelines.Get(topicPartition)
	if ok {
		return pipeline
	}
	pipeline = &appendPipeline{
		store:    s,
		tp:       topicPartition,
		requests: make(chan appendPipelineRequest, appendPipelineQueueSize),
		done:     make(chan struct{}),
	}
	s.appendPipelines.Set(topicPartition, pipeline)
	go pipeline.run()
	return pipeline
}

func (p *appendPipeline) append(records []RecordAppend) ([]Record, error) {
	if len(records) == 0 {
		return nil, nil
	}
	request := appendPipelineRequest{
		records: records,
		result:  make(chan appendPipelineResult, 1),
	}
	p.requests <- request
	result := <-request.result
	return result.records, result.err
}

func (p *appendPipeline) run() {
	defer close(p.done)
	for first := range p.requests {
		p.flush(p.drain(first))
	}
}

func (p *appendPipeline) drain(first appendPipelineRequest) []appendPipelineRequest {
	requests := collectionlist.NewListWithCapacity[appendPipelineRequest](appendPipelineMaxRequests)
	requests.Add(first)
	records := len(first.records)
	for requests.Len() < appendPipelineMaxRequests && records < appendPipelineMaxRecords {
		select {
		case request, ok := <-p.requests:
			if !ok {
				return requests.Values()
			}
			requests.Add(request)
			records += len(request.records)
		default:
			return requests.Values()
		}
	}
	return requests.Values()
}

func (p *appendPipeline) flush(requests []appendPipelineRequest) {
	records := flattenAppendPipelineRecords(requests)
	appended, err := p.store.appendRecordsBatchDirect(p.tp, records)
	if err != nil {
		completeAppendPipelineRequests(requests, nil, err)
		return
	}
	if len(appended) != len(records) {
		err := E(CodeCodec, "append pipeline returned %d records, want %d", len(appended), len(records))
		completeAppendPipelineRequests(requests, nil, err)
		return
	}
	completeAppendPipelineRequests(requests, appended, nil)
}

func flattenAppendPipelineRecords(requests []appendPipelineRequest) []RecordAppend {
	total := 0
	for _, request := range requests {
		total += len(request.records)
	}
	records := collectionlist.NewListWithCapacity[RecordAppend](total)
	for _, request := range requests {
		records.Add(request.records...)
	}
	return records.Values()
}

func completeAppendPipelineRequests(requests []appendPipelineRequest, records []Record, err error) {
	cursor := 0
	for _, request := range requests {
		count := len(request.records)
		result := appendPipelineResult{err: err}
		if err == nil {
			result.records = cloneRecords(records[cursor : cursor+count])
		}
		request.result <- result
		cursor += count
	}
}

func (s *StorxLogStore) closeAppendPipelines() {
	if s == nil || s.appendPipelines == nil {
		return
	}
	s.appendPipesMu.Lock()
	pipelines := s.appendPipelines.Values()
	s.appendPipelines.Clear()
	s.appendPipesMu.Unlock()
	for _, pipeline := range pipelines {
		close(pipeline.requests)
	}
	for _, pipeline := range pipelines {
		<-pipeline.done
	}
}
