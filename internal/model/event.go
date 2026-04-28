package model

import "time"

type OrderRequestedEvent struct {
	EventID   string    `json:"eventId"`
	RequestID string    `json:"requestId"`
	UserID    string    `json:"userId"`
	ProductID string    `json:"productId"`
	Quantity  int       `json:"quantity"`
	CreatedAt time.Time `json:"createdAt"`
}
