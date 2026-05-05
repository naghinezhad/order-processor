package api

import (
	"github.com/gin-gonic/gin"
	"github.com/naghinezhad/order-processor/internal/api/handler"
	"github.com/naghinezhad/order-processor/internal/service"
	"go.uber.org/zap"
)

func SetupRouter(orderService *service.OrderService, logger *zap.Logger) *gin.Engine {
	r := gin.Default()

	orderHandler := handler.NewOrderHandler(orderService, logger)

	r.POST("/api/orders", orderHandler.CreateOrder)
	r.GET("/api/orders/:orderId", orderHandler.GetRequestStatus)
	r.GET("/api/orders/requests/:requestId", orderHandler.GetRequestStatusByRequestID)

	return r
}
