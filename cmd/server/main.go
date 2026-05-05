package main

import (
	"context"
	"flag"
	"time"

	"github.com/naghinezhad/order-processor/config"
	"github.com/naghinezhad/order-processor/internal/api"
	"github.com/naghinezhad/order-processor/internal/database"
	"github.com/naghinezhad/order-processor/internal/kafka"
	"github.com/naghinezhad/order-processor/internal/redis"
	"github.com/naghinezhad/order-processor/internal/service"
	"github.com/naghinezhad/order-processor/internal/utils/logger"
	"go.uber.org/zap"
)

func main() {
	runMigration := flag.Bool("migration", false, "run database migrations and exit")
	flag.Parse()

	cfg := config.LoadConfig()
	logger.Init()

	ctx := context.Background()

	db, err := database.NewPostgres(ctx, cfg.PostgresDSN)
	if err != nil {
		logger.Log.Fatal("failed to connect to postgres", zap.Error(err))
	}
	defer db.Close()

	if *runMigration {
		if err := database.RunMigrations(ctx, db); err != nil {
			logger.Log.Fatal("failed to run migrations", zap.Error(err))
		}
		logger.Log.Info("migrations applied")
		return
	}

	redisClient, err := redis.NewRedis(ctx, cfg.RedisAddr, cfg.RedisPass)
	if err != nil {
		logger.Log.Fatal("failed to connect to redis", zap.Error(err))
	}

	producer := kafka.NewProducer(cfg.KafkaBrokers, cfg.KafkaTopic, logger.Log)
	defer producer.Close()

	orderService := service.NewOrderService(
		db,
		redisClient,
		producer,
		time.Duration(cfg.LockTTLSeconds)*time.Second,
		logger.Log,
	)

	router := api.SetupRouter(orderService, logger.Log)

	logger.Log.Info("http server started", zap.String("port", cfg.ServerPort))
	if err := router.Run(":" + cfg.ServerPort); err != nil {
		logger.Log.Fatal("server error", zap.Error(err))
	}
}
