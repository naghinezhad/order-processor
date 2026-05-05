package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jinzhu/copier"
	"github.com/naghinezhad/order-processor/internal/model"
	"github.com/naghinezhad/order-processor/internal/redis"
	"github.com/naghinezhad/order-processor/internal/repository"
	"go.uber.org/zap"
)

var ErrDuplicateEvent = errors.New("duplicate event")
var ErrLockNotAcquired = errors.New("lock not acquired")
var ErrRequestNotFound = repository.ErrRequestNotFound

// EventPublisher allows the API to publish Kafka events without importing the kafka package.
type EventPublisher interface {
	PublishOrderRequested(ctx context.Context, event *model.OrderRequestedEvent) error
}

type OrderService struct {
	db        *pgxpool.Pool
	requests  *repository.RequestRepository
	orders    *repository.OrderRepository
	redis     redis.Client
	publisher EventPublisher
	lockTTL   time.Duration
	logger    *zap.Logger
}

func NewOrderService(
	db *pgxpool.Pool,
	redisClient redis.Client,
	publisher EventPublisher,
	lockTTL time.Duration,
	logger *zap.Logger,
) *OrderService {
	return &OrderService{
		db:        db,
		requests:  repository.NewRequestRepository(db),
		orders:    repository.NewOrderRepository(db),
		redis:     redisClient,
		publisher: publisher,
		lockTTL:   lockTTL,
		logger:    logger,
	}
}

func (s *OrderService) CreateOrderRequest(ctx context.Context, input *model.CreateOrderInput) (*model.OrderRequest, error) {
	if s.publisher == nil {
		return nil, errors.New("publisher not configured")
	}
	if input == nil {
		return nil, errors.New("input is required")
	}

	requestID := uuid.NewString()
	request := &model.OrderRequest{}
	if err := copier.Copy(request, input); err != nil {
		return nil, err
	}
	request.RequestID = requestID
	request.Status = model.StatusPending
	request.CreatedAt = time.Now().UTC()
	request.UpdatedAt = request.CreatedAt

	if err := s.requests.Create(ctx, request); err != nil {
		return nil, err
	}

	event := &model.OrderRequestedEvent{}
	if err := copier.Copy(event, input); err != nil {
		return nil, err
	}
	event.EventID = uuid.NewString()
	event.RequestID = requestID
	event.CreatedAt = time.Now().UTC()

	if err := s.publisher.PublishOrderRequested(ctx, event); err != nil {
		_ = s.requests.UpdateStatus(ctx, requestID, model.StatusFailed, nil)
		return nil, err
	}

	return request, nil
}

func (s *OrderService) GetRequestStatus(ctx context.Context, requestID string) (*model.OrderRequest, error) {
	return s.requests.GetByID(ctx, requestID)
}

func (s *OrderService) GetRequestStatusByOrderID(ctx context.Context, orderID int64) (*model.OrderRequest, error) {
	return s.requests.GetByOrderID(ctx, orderID)
}

func (s *OrderService) ProcessEvent(ctx context.Context, event *model.OrderRequestedEvent) error {
	lockKey := fmt.Sprintf("lock:request:%s", event.RequestID)
	lockToken, err := s.redis.AcquireLock(ctx, lockKey, s.lockTTL)
	if err != nil {
		return err
	}

	if lockToken == "" {
		return ErrLockNotAcquired
	}

	if s.logger != nil {
		s.logger.Info("lock acquired for request",
			zap.String("request_id", event.RequestID),
		)
	}

	defer func() {
		if _, err := s.redis.ReleaseLock(ctx, lockKey, lockToken); err != nil && s.logger != nil {
			s.logger.Warn("failed to release lock",
				zap.String("request_id", event.RequestID),
				zap.Error(err),
			)
		} else if s.logger != nil {
			s.logger.Info("lock released for request",
				zap.String("request_id", event.RequestID),
			)
		}
	}()

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	requestRepo := repository.NewRequestRepository(tx)
	orderRepo := repository.NewOrderRepository(tx)

	inserted, err := requestRepo.InsertProcessedEvent(ctx, event.RequestID, event.EventID)
	if err != nil {
		return err
	}

	if !inserted {
		return ErrDuplicateEvent
	}

	orderID, err := orderRepo.Create(ctx, event)
	if err != nil {
		_ = s.requests.UpdateStatus(ctx, event.RequestID, model.StatusFailed, nil)
		return err
	}

	if err := requestRepo.UpdateStatus(ctx, event.RequestID, model.StatusCompleted, &orderID); err != nil {
		_ = s.requests.UpdateStatus(ctx, event.RequestID, model.StatusFailed, nil)
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		_ = s.requests.UpdateStatus(ctx, event.RequestID, model.StatusFailed, nil)
		return err
	}

	return nil
}
