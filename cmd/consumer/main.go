package main

import (
	"context"
	"time"

	"github.com/naghinezhad/order-processor/config"
	"github.com/naghinezhad/order-processor/internal/database"
	"github.com/naghinezhad/order-processor/internal/kafka"
	"github.com/naghinezhad/order-processor/internal/redis"
	"github.com/naghinezhad/order-processor/internal/service"
	"github.com/naghinezhad/order-processor/internal/utils/logger"
	"go.uber.org/zap"
)

func main() {
	cfg := config.LoadConfig()
	logger.Init()

	ctx := context.Background()

	db, err := database.NewPostgres(ctx, cfg.PostgresDSN)
	if err != nil {
		logger.Log.Fatal("failed to connect to postgres", zap.Error(err))
	}
	defer db.Close()

	redisClient, err := redis.NewRedis(ctx, cfg.RedisAddr, cfg.RedisPass)
	if err != nil {
		logger.Log.Fatal("failed to connect to redis", zap.Error(err))
	}

	orderService := service.NewOrderService(
		db,
		redisClient,
		nil,
		time.Duration(cfg.LockTTLSeconds)*time.Second,
		logger.Log,
	)

	consumer := kafka.NewConsumer(
		cfg.KafkaBrokers,
		cfg.KafkaTopic,
		cfg.ConsumerGroupID,
		cfg.ConsumerID,
		logger.Log,
		orderService.ProcessEvent,
	)
	defer consumer.Close()

	logger.Log.Info("consumer started", zap.String("consumer_id", cfg.ConsumerID))

	if err := consumer.Run(ctx); err != nil {
		logger.Log.Fatal("consumer error", zap.Error(err))
	}
}
