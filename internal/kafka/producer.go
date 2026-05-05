package kafka

import (
	"context"
	"encoding/json"
	"time"

	"github.com/naghinezhad/order-processor/internal/model"
	kafka "github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

type Producer struct {
	writer *kafka.Writer
	logger *zap.Logger
}

func NewProducer(brokers []string, topic string, logger *zap.Logger) *Producer {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		RequiredAcks: kafka.RequireOne,
		Balancer:     &kafka.RoundRobin{},
	}

	return &Producer{
		writer: writer,
		logger: logger,
	}
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

func (p *Producer) PublishOrderRequested(ctx context.Context, event *model.OrderRequestedEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	msg := kafka.Message{
		Value: payload,
		Time:  time.Now().UTC(),
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return err
	}

	if p.logger != nil {
		p.logger.Info("produced event",
			zap.String("event_id", event.EventID),
			zap.String("request_id", event.RequestID),
			zap.Int("partition", msg.Partition),
		)
	}

	return nil
}
