package repository

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/naghinezhad/order-processor/internal/model"
)

type RequestRepository struct {
	db DBTX
}

var ErrRequestNotFound = errors.New("request not found")

func NewRequestRepository(db DBTX) *RequestRepository {
	return &RequestRepository{db: db}
}

func (r *RequestRepository) Create(ctx context.Context, req *model.OrderRequest) error {
	_, err := r.db.Exec(ctx, `
		INSERT INTO order_requests (request_id, user_id, product_id, quantity, status, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, req.RequestID, req.UserID, req.ProductID, req.Quantity, req.Status, req.CreatedAt, req.UpdatedAt)
	return err
}

func (r *RequestRepository) UpdateStatus(ctx context.Context, requestID string, status string, orderID *int64) error {
	tag, err := r.db.Exec(ctx, `
		UPDATE order_requests
		SET status = $1, order_id = $2, updated_at = $3
		WHERE request_id = $4
	`, status, orderID, time.Now().UTC(), requestID)
	if err != nil {
		return err
	}

	if tag.RowsAffected() == 0 {
		return ErrRequestNotFound
	}

	return nil
}

func (r *RequestRepository) GetByID(ctx context.Context, requestID string) (*model.OrderRequest, error) {
	var req model.OrderRequest
	var orderID *int64

	err := r.db.QueryRow(ctx, `
		SELECT request_id, user_id, product_id, quantity, status, order_id, created_at, updated_at
		FROM order_requests
		WHERE request_id = $1
	`, requestID).Scan(
		&req.RequestID,
		&req.UserID,
		&req.ProductID,
		&req.Quantity,
		&req.Status,
		&orderID,
		&req.CreatedAt,
		&req.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrRequestNotFound
		}
		return nil, err
	}

	req.OrderID = orderID
	return &req, nil
}

func (r *RequestRepository) GetByOrderID(ctx context.Context, orderID int64) (*model.OrderRequest, error) {
	var req model.OrderRequest
	var dbOrderID *int64

	err := r.db.QueryRow(ctx, `
		SELECT request_id, user_id, product_id, quantity, status, order_id, created_at, updated_at
		FROM order_requests
		WHERE order_id = $1
	`, orderID).Scan(
		&req.RequestID,
		&req.UserID,
		&req.ProductID,
		&req.Quantity,
		&req.Status,
		&dbOrderID,
		&req.CreatedAt,
		&req.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrRequestNotFound
		}
		return nil, err
	}

	req.OrderID = dbOrderID
	return &req, nil
}

func (r *RequestRepository) InsertProcessedEvent(ctx context.Context, requestID string, eventID string) (bool, error) {
	tag, err := r.db.Exec(ctx, `
		INSERT INTO processed_events (request_id, event_id)
		VALUES ($1, $2)
		ON CONFLICT (request_id) DO NOTHING
	`, requestID, eventID)
	if err != nil {
		return false, err
	}

	return tag.RowsAffected() == 1, nil
}
