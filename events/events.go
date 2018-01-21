package events

import (
	"encoding/json"
	"log"
	"time"

	nats "github.com/nats-io/go-nats"
	"github.com/nats-io/nuid"
)

type Publisher struct {
	Conn   *nats.Conn
	Logger *log.Logger
}

func (p *Publisher) log(err error, topic string, data []byte) {
	b, _ := json.Marshal(map[string]interface{}{
		"type":  "nats-publish-failed",
		"error": err.Error(),
		"topic": topic,
		"data":  json.RawMessage(data),
	})
	p.Logger.Print(b)
}

// publishEvent publishes or logs the event.
func (p *Publisher) Publish(topic string, corrId string, event EventTyper) {
	b, _ := json.Marshal(event)

	data, _ := json.Marshal(&Event{
		ID:            nuid.Next(),
		Type:          event.EventType(),
		Time:          time.Now().Unix(),
		Data:          json.RawMessage(b),
		CorrelationID: corrId,
	})

	// Publish and fallback to logging to stderr.
	if err := p.Conn.Publish(topic, data); err != nil {
		p.log(err, topic, data)
	}
}

type Event struct {
	ID            string          `json:"id"`
	Type          string          `json:"type"`
	Time          int64           `json:"time"`
	Data          json.RawMessage `json:"data"`
	CorrelationID string          `json:"correlation_id"`
}

type EventTyper interface {
	EventType() string
}
