package tracing

import "github.com/segmentio/kafka-go"

// KafkaHeaderCarrier adapts a slice of Kafka message headers to the
// OpenTelemetry TextMapCarrier interface, so the propagator can read and
// write trace context into Kafka message headers.
type KafkaHeaderCarrier struct {
	headers *[]kafka.Header
}

// NewKafkaHeaderCarrier wraps a pointer to a message's headers slice.
// A pointer is needed because Set appends new headers, which reassigns the slice.
func NewKafkaHeaderCarrier(headers *[]kafka.Header) KafkaHeaderCarrier {
	return KafkaHeaderCarrier{headers: headers}
}

// Get returns the value of the header with the given key, or "" if absent.
func (c KafkaHeaderCarrier) Get(key string) string {
	for _, h := range *c.headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// Set adds or replaces a header with the given key and value.
func (c KafkaHeaderCarrier) Set(key, value string) {
	// remove any existing header with this key first
	filtered := (*c.headers)[:0]
	for _, h := range *c.headers {
		if h.Key != key {
			filtered = append(filtered, h)
		}
	}
	// append the new one
	*c.headers = append(filtered, kafka.Header{Key: key, Value: []byte(value)})
}

// Keys returns all header keys present.
func (c KafkaHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(*c.headers))
	for _, h := range *c.headers {
		keys = append(keys, h.Key)
	}
	return keys
}
