package handler

import (
	"errors"
	"net/http"
	"strconv"

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

func (h *OrderHandler) CreateOrder(c *gin.Context) {
	var req model.CreateOrderInput
	if err := c.ShouldBindJSON(&req); err != nil {
		h.logger.Warn("invalid create order request", zap.Error(err))
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	orderReq, err := h.service.CreateOrderRequest(c, &req)
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
	orderIDParam := c.Param("orderId")
	if orderIDParam == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "orderId is required"})
		return
	}

	orderID, err := strconv.ParseInt(orderIDParam, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "orderId must be a number"})
		return
	}

	orderReq, err := h.service.GetRequestStatusByOrderID(c, orderID)
	if err != nil {
		if errors.Is(err, service.ErrRequestNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}
		h.logger.Error("failed to get request status", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get request status"})
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

func (h *OrderHandler) GetRequestStatusByRequestID(c *gin.Context) {
	requestID := c.Param("requestId")
	if requestID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "requestId is required"})
		return
	}

	orderReq, err := h.service.GetRequestStatus(c, requestID)
	if err != nil {
		if errors.Is(err, service.ErrRequestNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}
		h.logger.Error("failed to get request status", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get request status"})
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
