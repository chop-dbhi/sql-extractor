package sqlextractor

import "time"

const (
	BatchStartedEvent         = "batch-started"
	BatchEndedEvent           = "batch-ended"
	ExtractStartedEvent       = "extract-started"
	ExtractEndedEvent         = "extract-ended"
	ExtractUploadStartedEvent = "extract-upload-started"
	ExtractUploadEndedEvent   = "extract-upload-ended"
)

type BatchStarted struct{}

func (*BatchStarted) EventType() string {
	return BatchStartedEvent
}

type BatchEnded struct {
	Duration time.Duration `json:"duration"`
}

func (*BatchEnded) EventType() string {
	return BatchEndedEvent
}

type ExtractStarted struct {
	Query string `json:"query"`
}

func (*ExtractStarted) EventType() string {
	return ExtractStartedEvent
}

type ExtractEnded struct {
	Query    string        `json:"query"`
	Bytes    int64         `json:"bytes"`
	Duration time.Duration `json:"duration"`
	Error    string        `json:"error"`
}

func (*ExtractEnded) EventType() string {
	return ExtractEndedEvent
}

type ExtractUploadStarted struct {
	Query  string `json:"query"`
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
}

func (*ExtractUploadStarted) EventType() string {
	return ExtractUploadStartedEvent
}

type ExtractUploadEnded struct {
	Query    string        `json:"query"`
	Bucket   string        `json:"bucket"`
	Key      string        `json:"key"`
	Duration time.Duration `json:"duration"`
	Error    string        `json:"error"`
}

func (*ExtractUploadEnded) EventType() string {
	return ExtractUploadEndedEvent
}
