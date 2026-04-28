package repository

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/naghinezhad/order-processor/internal/model"
)

type OrderRepository struct {
	db DBTX
}

func NewOrderRepository(db DBTX) *OrderRepository {
	return &OrderRepository{db: db}
}

func (r *OrderRepository) Create(ctx context.Context, event *model.OrderRequestedEvent) (int64, error) {
	var id int64
	query := `
		INSERT INTO orders (request_id, user_id, product_id, quantity)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (request_id) DO NOTHING
		RETURNING id
	`

	err := r.db.QueryRow(ctx, query, event.RequestID, event.UserID, event.ProductID, event.Quantity).Scan(&id)
	if err == nil {
		return id, nil
	}

	if err == pgx.ErrNoRows {
		return r.GetIDByRequestID(ctx, event.RequestID)
	}

	return 0, err
}

func (r *OrderRepository) GetIDByRequestID(ctx context.Context, requestID string) (int64, error) {
	var id int64
	query := `
		SELECT id FROM orders WHERE request_id = $1
	`

	if err := r.db.QueryRow(ctx, query, requestID).Scan(&id); err != nil {
		return 0, err
	}

	return id, nil
}
