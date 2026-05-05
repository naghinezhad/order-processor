package model

import "time"

const (
	StatusPending   = "PENDING"
	StatusCompleted = "COMPLETED"
	StatusFailed    = "FAILED"
)

type OrderRequest struct {
	RequestID string    `json:"requestId"`
	UserID    string    `json:"userId"`
	ProductID string    `json:"productId"`
	Quantity  int       `json:"quantity"`
	Status    string    `json:"status"`
	OrderID   *int64    `json:"orderId,omitempty"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

type CreateOrderInput struct {
	UserID    string `json:"userId" binding:"required"`
	ProductID string `json:"productId" binding:"required"`
	Quantity  int    `json:"quantity" binding:"required"`
}
