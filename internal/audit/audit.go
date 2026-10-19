// Package audit provides an append-only audit log of order events stored in
// the MongoDB collection "order_audit". It replaces app/audit.py.
package audit

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"
)

const (
	// CollectionName is the MongoDB collection holding audit events. It is
	// part of the shared contract with the Python service; do not change it.
	CollectionName = "order_audit"

	// defaultDatabase is used when the connection URI names no database,
	// mirroring pymongo's get_default_database("app").
	defaultDatabase = "app"

	// serverSelectionTimeout mirrors serverSelectionTimeoutMS=5000.
	serverSelectionTimeout = 5 * time.Second
)

// Entry is a single audit event as returned by History. It serialises to
// JSON as {"event": ..., "detail": {...}}.
type Entry struct {
	Event  string         `json:"event" bson:"event"`
	Detail map[string]any `json:"detail" bson:"detail"`
}

// record is the on-disk document shape.
type record struct {
	OrderID int64          `bson:"order_id"`
	Event   string         `bson:"event"`
	Detail  map[string]any `bson:"detail"`
	At      time.Time      `bson:"at"`
}

// Log is an append-only audit log backed by a MongoDB collection.
type Log struct {
	client *mongo.Client
	coll   *mongo.Collection
	now    func() time.Time
}

// New connects to MongoDB at uri and returns a Log over the order_audit
// collection of the URI's default database (or "app" if none is given).
//
// MIGRATION_NOTE: the Python module created its client at import time; here
// the caller constructs the Log explicitly and must call Close on shutdown.
// Like pymongo, connection is lazy: server selection errors surface on the
// first operation (after up to 5 s), not here.
func New(ctx context.Context, uri string) (*Log, error) {
	cs, err := connstring.ParseAndValidate(uri)
	if err != nil {
		return nil, fmt.Errorf("audit: parse mongodb uri: %w", err)
	}
	dbName := cs.Database
	if dbName == "" {
		dbName = defaultDatabase
	}

	client, err := mongo.Connect(ctx, options.Client().
		ApplyURI(uri).
		SetServerSelectionTimeout(serverSelectionTimeout))
	if err != nil {
		return nil, fmt.Errorf("audit: connect mongodb: %w", err)
	}

	// Decode embedded documents as maps (not bson.D) so detail values
	// serialise to plain JSON objects, matching pymongo's dict behaviour.
	collOpts := options.Collection().SetBSONOptions(&options.BSONOptions{
		DefaultDocumentM: true,
	})
	coll := client.Database(dbName).Collection(CollectionName, collOpts)

	return &Log{client: client, coll: coll, now: time.Now}, nil
}

// Close disconnects the underlying MongoDB client.
func (l *Log) Close(ctx context.Context) error {
	if l == nil || l.client == nil {
		return nil
	}
	if err := l.client.Disconnect(ctx); err != nil {
		return fmt.Errorf("audit: disconnect mongodb: %w", err)
	}
	return nil
}

// Record appends an audit event for orderID. A nil detail is stored as an
// empty object. The timestamp is the current UTC time.
func (l *Log) Record(ctx context.Context, orderID int64, event string, detail map[string]any) error {
	if l == nil || l.coll == nil {
		return errors.New("audit: log not initialised")
	}
	if detail == nil {
		detail = map[string]any{}
	}
	doc := record{
		OrderID: orderID,
		Event:   event,
		Detail:  detail,
		At:      l.now().UTC(),
	}
	if _, err := l.coll.InsertOne(ctx, doc); err != nil {
		return fmt.Errorf("audit: record %q for order %d: %w", event, orderID, err)
	}
	return nil
}

// History returns every audit event for orderID in chronological order
// (ascending "at"). The result is never nil, so it encodes as a JSON list
// ("[]" when there are no events).
func (l *Log) History(ctx context.Context, orderID int64) ([]Entry, error) {
	if l == nil || l.coll == nil {
		return nil, errors.New("audit: log not initialised")
	}
	opts := options.Find().
		SetSort(bson.D{{Key: "at", Value: 1}}).
		SetProjection(bson.D{
			{Key: "_id", Value: 0},
			{Key: "event", Value: 1},
			{Key: "detail", Value: 1},
		})

	cur, err := l.coll.Find(ctx, bson.D{{Key: "order_id", Value: orderID}}, opts)
	if err != nil {
		return nil, fmt.Errorf("audit: query history for order %d: %w", orderID, err)
	}
	defer cur.Close(ctx)

	entries := make([]Entry, 0)
	for cur.Next(ctx) {
		var e Entry
		if err := cur.Decode(&e); err != nil {
			return nil, fmt.Errorf("audit: decode history for order %d: %w", orderID, err)
		}
		if e.Detail == nil {
			e.Detail = map[string]any{}
		}
		entries = append(entries, e)
	}
	if err := cur.Err(); err != nil {
		return nil, fmt.Errorf("audit: iterate history for order %d: %w", orderID, err)
	}
	return entries, nil
}
