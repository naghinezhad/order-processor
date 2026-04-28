package kafka

import (
	"context"
	"encoding/json"
	"math/rand"
	"sync"
	"time"

	"github.com/naghinezhad/order-processor/internal/model"
	kafka "github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

type Producer struct {
	writer         *kafka.Writer
	partitionCount int
	mu             sync.Mutex
	rnd            *rand.Rand
	logger         *zap.Logger
}

func NewProducer(brokers []string, topic string, partitionCount int, logger *zap.Logger) *Producer {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		RequiredAcks: kafka.RequireOne,
	}

	return &Producer{
		writer:         writer,
		partitionCount: partitionCount,
		rnd:            rand.New(rand.NewSource(time.Now().UnixNano())),
		logger:         logger,
	}
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

func (p *Producer) PublishOrderRequested(ctx context.Context, event *model.OrderRequestedEvent) (int, error) {
	payload, err := json.Marshal(event)
	if err != nil {
		return -1, err
	}

	partition := p.randomPartition()
	msg := kafka.Message{
		Key:       []byte(event.RequestID),
		Value:     payload,
		Partition: partition,
		Time:      time.Now().UTC(),
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return partition, err
	}

	if p.logger != nil {
		p.logger.Info("produced event",
			zap.String("event_id", event.EventID),
			zap.String("request_id", event.RequestID),
			zap.Int("partition", partition),
		)
	}

	return partition, nil
}

func (p *Producer) randomPartition() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.partitionCount <= 1 {
		return 0
	}

	return p.rnd.Intn(p.partitionCount)
}
