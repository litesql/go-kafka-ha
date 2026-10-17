package kafkaha

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/litesql/go-ha"
	haconnect "github.com/litesql/go-ha/connect"
	"github.com/twmb/franz-go/pkg/kgo"
)

var _ ha.Subscriber = &Consumer{}

type DeliveryPolicy int

const (
	DeliveryPolicyDefault DeliveryPolicy = iota
	DeliveryPolicyEarliest
	DeliveryPolicyLatest
	DeliveryPolicyNone
)

type Consumer struct {
	client    *kgo.Client
	quitChan  chan struct{}
	latestSeq uint64
	db        *sql.DB
}

func NewConsumer(cfg Config, deliverPolicy DeliveryPolicy) (*Consumer, error) {
	opts := cfg.Opts()
	switch deliverPolicy {
	case DeliveryPolicyEarliest:
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
		opts = append(opts, kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()))
	case DeliveryPolicyLatest:
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
		opts = append(opts, kgo.ConsumeStartOffset(kgo.NewOffset().AtEnd()))
	case DeliveryPolicyNone:
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtCommitted()))
		opts = append(opts, kgo.ConsumeStartOffset(kgo.NewOffset().AtCommitted()))
	case DeliveryPolicyDefault:
	default:
		return nil, fmt.Errorf("invalid Kafka replication consumer deliver policy: %q", deliverPolicy)
	}
	opts = append(opts,
		kgo.DisableAutoCommit(),
		kgo.ConsumeTopics(cfg.Topic),
	)
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	s := Consumer{
		client:   client,
		quitChan: make(chan struct{}),
	}

	return &s, nil
}

func (s *Consumer) DB() *sql.DB {
	return s.db
}

func (s *Consumer) SetDB(db *sql.DB) {
	s.db = db
}

func (s *Consumer) Start() error {
	go func() {
		for {
			select {
			case <-s.quitChan:
				return
			default:
				fetches := s.client.PollFetches(context.Background())
				if fetches.IsClientClosed() {
					return
				}
				fetches.EachError(func(t string, p int32, err error) {
					slog.Error("Replication Consumer fetch error", "topic", t, "partition", p, "error", err)
				})
				var rs []*kgo.Record
				fetches.EachRecord(func(r *kgo.Record) {
					err := s.handler(r)
					if err != nil {
						slog.Error("Replication Consumer handler", "error", err)
						return
					}
					s.latestSeq = uint64(r.Offset)
					rs = append(rs, r)
				})
				if len(rs) == 0 {
					continue
				}
				if err := s.client.CommitRecords(context.Background(), rs...); err != nil {
					slog.Error("Replciation consumer commit records", "error", err)
				}
			}
		}
	}()
	return nil
}

func (s *Consumer) LatestSeq() uint64 {
	return s.latestSeq
}

func (s *Consumer) RemoveConsumer(ctx context.Context, name string) error {
	return errors.New("not implemented")
}

func (s *Consumer) DeliveredInfo(ctx context.Context, name string) (any, error) {
	return nil, nil
}

func (s *Consumer) HistoryBySeq(ctx context.Context, startSeq uint64) ([]haconnect.HistoryItem, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *Consumer) HistoryByTime(ctx context.Context, duration time.Duration) ([]haconnect.HistoryItem, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *Consumer) UndoBySeq(ctx context.Context, startSeq uint64, filterType haconnect.UndoFilter, filterEntities map[string][]int64) error {
	return fmt.Errorf("not implemented")
}

func (s *Consumer) UndoByTime(ctx context.Context, duration time.Duration, filterType haconnect.UndoFilter, filterEntities map[string][]int64) error {
	return fmt.Errorf("not implemented")
}

func (s *Consumer) Close() error {
	close(s.quitChan)
	s.client.Close()
	return nil
}

func (s *Consumer) handler(r *kgo.Record) error {
	var cs ha.ChangeSet
	err := json.Unmarshal(r.Value, &cs)
	if err != nil {
		slog.Error("failed to unmarshal replication message", "error", err)
		return nil //ack
	}
	slog.Info("received replication message", "topic", r.Topic, "origin", cs.Node, "filename", cs.Filename, "changes", len(cs.Changes))
	if s.db == nil {
		slog.Error("Replication consumer: database not found")
		return fmt.Errorf("db not found")
	}
	err = cs.Apply(s.db)
	if err != nil {
		return fmt.Errorf("failed to apply replication message of %q: %w", cs.Filename, err)
	}
	return nil
}
