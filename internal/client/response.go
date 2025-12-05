package client

// TempoSearchResponse represents the response from Tempo /api/search endpoint
type TempoSearchResponse struct {
	Traces []Trace `json:"traces"`
}

// Trace represents a single trace in the search response
type Trace struct {
	TraceID  string    `json:"traceID"`
	SpanSets []SpanSet `json:"spanSets"`
	// For non-structural queries, spans may be at trace level
	SpanSet *SpanSet `json:"spanSet,omitempty"`
}

// SpanSet represents a set of matching spans within a trace
type SpanSet struct {
	Spans   []Span `json:"spans"`
	Matched int    `json:"matched"`
}

// Span represents a single span in the search response
type Span struct {
	SpanID string `json:"spanID"`
}

