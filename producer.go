package kafkaha

import (
	"context"
	"encoding/json"
	"path/filepath"
	"time"

	"github.com/litesql/go-ha"
	"github.com/twmb/franz-go/pkg/kgo"
)

var _ ha.Publisher = &Producer{}

type Producer struct {
	client  *kgo.Client
	topic   string
	offset  int64
	timeout time.Duration
}

func NewProducer(cfg Config, timeout time.Duration) (*Producer, error) {
	opts := cfg.Opts()
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &Producer{
		client:  client,
		topic:   cfg.Topic,
		timeout: timeout,
	}, nil
}

func (p *Producer) Publish(cs *ha.ChangeSet) error {
	jsonData, err := json.Marshal(cs)
	if err != nil {
		return err
	}
	record := &kgo.Record{
		Topic: p.topic,
		Key:   []byte(filepath.Base(cs.Filename)),
		Value: jsonData,
	}
	ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
	defer cancel()
	results := p.client.ProduceSync(ctx, record)
	r, _ := results.First()
	if r != nil {
		p.offset = r.Offset
	}
	return results.FirstErr()
}

func (p *Producer) Close() {
	p.client.Close()
}

func (p *Producer) Sequence() uint64 {
	return uint64(p.offset)
}
