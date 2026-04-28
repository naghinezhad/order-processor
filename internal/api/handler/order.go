package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/naghinezhad/order-processor/internal/model"
	"github.com/naghinezhad/order-processor/internal/service"
	"go.uber.org/zap"
)

type OrderHandler struct {
	service *service.OrderService
	logger  *zap.Logger
}

func NewOrderHandler(s *service.OrderService, logger *zap.Logger) *OrderHandler {
	return &OrderHandler{service: s, logger: logger}
}

type CreateOrderRequest struct {
	UserID    string `json:"userId" binding:"required"`
	ProductID string `json:"productId" binding:"required"`
	Quantity  int    `json:"quantity" binding:"required"`
}

func (h *OrderHandler) CreateOrder(c *gin.Context) {
	var req CreateOrderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.logger.Warn("invalid create order request", zap.Error(err))
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	orderReq, err := h.service.CreateOrderRequest(c, req.UserID, req.ProductID, req.Quantity)
	if err != nil {
		h.logger.Error("failed to create order request", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create order request"})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{
		"requestId": orderReq.RequestID,
		"status":    orderReq.Status,
	})
}

func (h *OrderHandler) GetRequestStatus(c *gin.Context) {
	requestID := c.Param("requestId")
	if requestID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "requestId is required"})
		return
	}

	orderReq, err := h.service.GetRequestStatus(c, requestID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	response := gin.H{
		"requestId": orderReq.RequestID,
		"status":    orderReq.Status,
	}

	if orderReq.Status == model.StatusCompleted && orderReq.OrderID != nil {
		response["orderId"] = *orderReq.OrderID
	}

	c.JSON(http.StatusOK, response)
}
