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
