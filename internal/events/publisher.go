// Package events publishes order domain events to RabbitMQ. It replaces
// app/events.py.
package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"migrated-app/internal/config"
)

// EventKey is the JSON field carrying the event name in every message.
const EventKey = "event"

// dialTimeout bounds the TCP connect when ctx carries no deadline.
const dialTimeout = 30 * time.Second

// Publisher sends order domain events. It is the interface HTTP handlers
// should depend on so they can be tested without a broker.
type Publisher interface {
	Publish(ctx context.Context, event string, payload map[string]any) error
}

// AMQPPublisher publishes events to the durable order-events queue through
// the default exchange.
//
// MIGRATION_NOTE: like the Python source, every Publish call opens a fresh
// AMQP connection and always closes it. This keeps behavior identical (no
// shared state, no reconnect logic) at the cost of a TCP+AMQP handshake per
// event. Switch to a long-lived, mutex-guarded connection if throughput
// requires it.
type AMQPPublisher struct {
	url   string
	queue string
}

// NewAMQPPublisher returns a Publisher that connects to amqpURL on every
// publish and sends to config.OrderEventsQueue.
func NewAMQPPublisher(amqpURL string) (*AMQPPublisher, error) {
	if amqpURL == "" {
		return nil, errors.New("events: empty AMQP URL")
	}
	if _, err := amqp.ParseURI(amqpURL); err != nil {
		return nil, fmt.Errorf("events: parse AMQP URL: %w", err)
	}
	return &AMQPPublisher{url: amqpURL, queue: config.OrderEventsQueue}, nil
}

// Publish sends {"event": event, ...payload} as a persistent JSON message.
// As with Python's {"event": event, **payload}, a payload key named "event"
// overrides the event argument.
func (p *AMQPPublisher) Publish(ctx context.Context, event string, payload map[string]any) (err error) {
	body, err := encodeBody(event, payload)
	if err != nil {
		return err
	}

	conn, err := amqp.DialConfig(p.url, amqp.Config{
		Dial: func(network, addr string) (net.Conn, error) {
			d := net.Dialer{Timeout: dialTimeout}
			return d.DialContext(ctx, network, addr)
		},
	})
	if err != nil {
		return fmt.Errorf("events: connect: %w", err)
	}
	defer func() {
		if cerr := conn.Close(); cerr != nil && !errors.Is(cerr, amqp.ErrClosed) && err == nil {
			err = fmt.Errorf("events: close connection: %w", cerr)
		}
	}()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("events: open channel: %w", err)
	}

	// Must match the worker's declaration exactly (durable, no args) or the
	// broker answers PRECONDITION_FAILED.
	if _, err := ch.QueueDeclare(p.queue, true, false, false, false, nil); err != nil {
		return fmt.Errorf("events: declare queue %q: %w", p.queue, err)
	}

	msg := amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	}
	if err := ch.PublishWithContext(ctx, "", p.queue, false, false, msg); err != nil {
		return fmt.Errorf("events: publish %q: %w", event, err)
	}
	return nil
}

func encodeBody(event string, payload map[string]any) ([]byte, error) {
	msg := make(map[string]any, len(payload)+1)
	msg[EventKey] = event
	for k, v := range payload {
		msg[k] = v
	}
	body, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("events: encode %q: %w", event, err)
	}
	return body, nil
}
