package broker

import (
	"context"
	"net/http"
	"time"

	clientxhttp "github.com/arcgolabs/clientx/http"
	"github.com/lyonbrown4d/ech0/store"
)

type httpSinkClient = clientxhttp.Client

type httpSinkDelivery struct {
	Kind    string
	Name    string
	Method  string
	URL     string
	Headers http.Header
	Payload []byte
}

func newHTTPSinkClient(kind string, timeout time.Duration) (httpSinkClient, error) {
	client, err := clientxhttp.New(clientxhttp.Config{Timeout: timeout})
	if err != nil {
		return nil, wrapBroker("http_sink_client_create_failed", err, "create %s sink http client", kind)
	}
	return client, nil
}

func closeHTTPSinkClient(kind string, client httpSinkClient) error {
	if client == nil {
		return nil
	}
	return wrapBroker("http_sink_client_close_failed", client.Close(), "close %s sink http client", kind)
}

func deliverHTTPSink(ctx context.Context, client httpSinkClient, delivery httpSinkDelivery) error {
	req := client.R().SetBody(delivery.Payload)
	if delivery.Headers != nil {
		req.SetHeaderMultiValues(delivery.Headers)
	}
	resp, err := client.Execute(ctx, req, delivery.Method, delivery.URL)
	if err != nil {
		return wrapBroker("http_sink_request_failed", err, "deliver %s sink record", delivery.Kind)
	}
	if resp.StatusCode() < http.StatusOK || resp.StatusCode() >= http.StatusMultipleChoices {
		return brokerStoreError(store.CodeUnavailable, "%s sink %q returned status %d", delivery.Kind, delivery.Name, resp.StatusCode())
	}
	return nil
}

func sinkHeaders(contentType string, headers []WebhookSinkHeaderConfig) http.Header {
	out := http.Header{}
	if contentType != "" {
		out.Set("Content-Type", contentType)
	}
	for index := range headers {
		header := headers[index]
		if validRequiredString(header.Key) {
			out.Set(header.Key, header.Value)
		}
	}
	return out
}
