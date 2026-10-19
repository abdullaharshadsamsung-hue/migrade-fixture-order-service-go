// Package worker is the background order-events consumer. It replaces
// app/worker.py: it listens on the durable order-events queue and, for every
// "order.created" event, atomically moves the order from "pending" to
// "confirmed" and records a "confirmed" audit entry exactly once.
package worker

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"migrated-app/internal/config"
	"migrated-app/internal/events"
	"migrated-app/internal/store"
)

// EventOrderCreated is the only event the worker acts on.
const EventOrderCreated = "order.created"

// AuditEventConfirmed is the audit event written after a successful confirm.
const AuditEventConfirmed = "confirmed"

const (
	// reconnectDelay mirrors the source's time.sleep(2) between reconnects.
	reconnectDelay = 2 * time.Second
	// retryDelay is the back-off before requeueing a transiently failed message.
	retryDelay = 2 * time.Second
	// dialTimeout bounds the TCP connect to the broker.
	dialTimeout = 30 * time.Second
	// prefetch limits unacknowledged deliveries held by this worker.
	prefetch = 1
)

// ErrMalformedMessage is returned by Handle when the message body cannot be
// processed regardless of retries (bad JSON, missing/invalid order_id).
var ErrMalformedMessage = errors.New("worker: malformed message")

// OrderConfirmer performs the atomic pending -> confirmed transition.
type OrderConfirmer interface {
	// ConfirmOrder reports whether the order changed from pending to
	// confirmed as a result of this call.
	ConfirmOrder(ctx context.Context, orderID int64) (bool, error)
}

// Auditor appends audit events; *audit.Log satisfies it.
type Auditor interface {
	Record(ctx context.Context, orderID int64, event string, detail map[string]any) error
}

// SQLOrders implements OrderConfirmer against the Postgres orders database.
type SQLOrders struct {
	db *sql.DB
}

// NewSQLOrders returns an OrderConfirmer backed by the orders pool of db.
func NewSQLOrders(db *store.DB) *SQLOrders {
	return &SQLOrders{db: db.Orders}
}

// ConfirmOrder runs a single compare-and-set UPDATE so that a redelivered
// event, or two concurrent workers, confirm an order only once.
func (s *SQLOrders) ConfirmOrder(ctx context.Context, orderID int64) (bool, error) {
	res, err := s.db.ExecContext(ctx,
		`UPDATE orders SET status = $1 WHERE id = $2 AND status = $3`,
		store.StatusConfirmed, orderID, store.StatusPending)
	if err != nil {
		return false, fmt.Errorf("confirm order %d: %w", orderID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("confirm order %d: rows affected: %w", orderID, err)
	}
	return n > 0, nil
}

// Handler processes order event message bodies.
type Handler struct {
	Orders OrderConfirmer
	Audit  Auditor
}

// NewHandler builds a Handler from its dependencies.
func NewHandler(orders OrderConfirmer, auditor Auditor) *Handler {
	return &Handler{Orders: orders, Audit: auditor}
}

// Handle processes one raw message body. Events other than "order.created"
// are a no-op. For "order.created" the referenced order is moved from
// pending to confirmed; the "confirmed" audit record is written only when
// that transition actually happened (duplicates are no-ops).
//
// It returns an error wrapping ErrMalformedMessage for unprocessable bodies
// and other errors for transient failures (worth retrying).
func (h *Handler) Handle(ctx context.Context, body []byte) error {
	var msg map[string]json.RawMessage
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(&msg); err != nil {
		return fmt.Errorf("%w: decode json: %v", ErrMalformedMessage, err)
	}

	var event string
	if raw, ok := msg[events.EventKey]; ok {
		// A non-string event can never equal "order.created": ignore it,
		// matching Python's message.get("event") != "order.created".
		if err := json.Unmarshal(raw, &event); err != nil {
			return nil
		}
	}
	if event != EventOrderCreated {
		return nil
	}

	orderID, err := parseOrderID(msg["order_id"])
	if err != nil {
		return err
	}

	confirmed, err := h.Orders.ConfirmOrder(ctx, orderID)
	if err != nil {
		return err
	}
	if !confirmed {
		return nil
	}
	// MIGRATION_NOTE: as in the source, the audit write happens after the DB
	// commit and outside the transaction. A failure here is logged and the
	// message is still acknowledged (retrying would not re-audit because the
	// order is no longer pending). Known gap preserved from the source.
	if err := h.Audit.Record(ctx, orderID, AuditEventConfirmed, nil); err != nil {
		log.Printf("worker: order %d confirmed but audit record failed: %v", orderID, err)
	}
	return nil
}

func parseOrderID(raw json.RawMessage) (int64, error) {
	if len(raw) == 0 {
		return 0, fmt.Errorf("%w: missing order_id", ErrMalformedMessage)
	}
	var v any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		return 0, fmt.Errorf("%w: order_id: %v", ErrMalformedMessage, err)
	}
	switch t := v.(type) {
	case json.Number:
		id, err := t.Int64()
		if err != nil {
			return 0, fmt.Errorf("%w: order_id %s is not an integer", ErrMalformedMessage, t)
		}
		return id, nil
	case string:
		id, err := strconv.ParseInt(t, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("%w: order_id %q is not an integer", ErrMalformedMessage, t)
		}
		return id, nil
	default:
		return 0, fmt.Errorf("%w: order_id has invalid type %T", ErrMalformedMessage, v)
	}
}

// Run connects to RabbitMQ at amqpURL, declares the durable order-events
// queue and consumes it with manual acknowledgement, passing each message to
// h.Handle. On any connection/channel error it waits 2 seconds and
// reconnects, forever, until ctx is cancelled (e.g. via
// signal.NotifyContext for SIGINT/SIGTERM), at which point it returns nil.
func Run(ctx context.Context, amqpURL string, h *Handler) error {
	if h == nil || h.Orders == nil || h.Audit == nil {
		return errors.New("worker: handler dependencies not set")
	}
	if _, err := amqp.ParseURI(amqpURL); err != nil {
		return fmt.Errorf("worker: parse AMQP URL: %w", err)
	}
	for {
		if ctx.Err() != nil {
			return nil
		}
		err := consume(ctx, amqpURL, h)
		if err == nil || ctx.Err() != nil {
			return nil
		}
		log.Printf("worker: %v; reconnecting in %s", err, reconnectDelay)
		if !sleep(ctx, reconnectDelay) {
			return nil
		}
	}
}

// consume runs a single connection's lifetime. It returns nil only when ctx
// is cancelled.
func consume(ctx context.Context, amqpURL string, h *Handler) error {
	conn, err := amqp.DialConfig(amqpURL, amqp.Config{
		Dial: func(network, addr string) (net.Conn, error) {
			d := net.Dialer{Timeout: dialTimeout}
			return d.DialContext(ctx, network, addr)
		},
	})
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = conn.Close() }()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	defer func() { _ = ch.Close() }()

	queue := config.OrderEventsQueue
	// Must match the publisher's declaration exactly (durable, no args).
	if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare queue %q: %w", queue, err)
	}
	if err := ch.Qos(prefetch, 0, false); err != nil {
		return fmt.Errorf("set qos: %w", err)
	}
	deliveries, err := ch.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume %q: %w", queue, err)
	}

	fmt.Println("worker: waiting for order events")

	closed := conn.NotifyClose(make(chan *amqp.Error, 1))
	for {
		select {
		case <-ctx.Done():
			return nil
		case amqpErr, ok := <-closed:
			if !ok || amqpErr == nil {
				return errors.New("connection closed")
			}
			return fmt.Errorf("connection closed: %w", amqpErr)
		case d, ok := <-deliveries:
			if !ok {
				return errors.New("delivery channel closed")
			}
			if err := process(ctx, h, d); err != nil {
				return err
			}
		}
	}
}

// process handles one delivery and settles it: ack on success, nack without
// requeue for malformed messages, nack with requeue (after back-off) for
// transient failures.
func process(ctx context.Context, h *Handler, d amqp.Delivery) error {
	err := h.Handle(ctx, d.Body)
	switch {
	case err == nil:
		if aerr := d.Ack(false); aerr != nil {
			return fmt.Errorf("ack: %w", aerr)
		}
	case errors.Is(err, ErrMalformedMessage):
		// MIGRATION_NOTE: deliberate fix — the source would crash the
		// consumer on a poison message and redeliver it forever.
		log.Printf("worker: dropping message: %v", err)
		if nerr := d.Nack(false, false); nerr != nil {
			return fmt.Errorf("nack: %w", nerr)
		}
	default:
		log.Printf("worker: transient failure, requeueing: %v", err)
		sleep(ctx, retryDelay)
		if nerr := d.Nack(false, true); nerr != nil {
			return fmt.Errorf("nack: %w", nerr)
		}
	}
	return nil
}

// sleep waits for d or until ctx is done; it reports whether the full
// duration elapsed.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
