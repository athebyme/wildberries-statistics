package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"wbmonitoring/monitoring/internal/config"
	"wbmonitoring/monitoring/internal/monitoring"
)

func main() {
	// Получаем настройки из переменных окружения
	cfg := config.LoadConfig()

	// Проверяем обязательные параметры
	if cfg.ApiKey == "" {
		log.Fatal("WB_API_KEY environment variable is required")
	}

	if cfg.TelegramToken == "" {
		log.Fatal("TELEGRAM_TOKEN environment variable is required")
	}

	if cfg.TelegramChatID == 0 {
		log.Fatal("TELEGRAM_CHAT_ID environment variable is required")
	}

	service, err := monitoring.NewMonitoringService(cfg)
	if err != nil {
		log.Fatalf("Failed to create monitoring service: %v", err)
	}

	// Инициализируем БД
	if err := service.InitDB(); err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}

	// Проверяем, нужно ли инициализировать продукты при первом запуске
	ctx := context.Background()
	productCount, err := service.GetProductCount(ctx)
	if err != nil {
		log.Fatalf("Failed to check product count: %v", err)
	}

	if productCount == 0 {
		log.Println("Products table is empty, starting initial product load...")
		err = service.SendTelegramAlert(fmt.Sprintf("Products table is empty, starting initial product load..."))
		if err != nil {
			log.Printf("Failed to send Telegram alert: %v", err)
		}
		if err := service.UpdateProducts(ctx); err != nil {
			log.Printf("Initial product load failed: %v", err)
			err = service.SendTelegramAlert(fmt.Sprintf("Initial product load failed: %v", err))
			log.Fatalf("Initial product load failed: %v", err) // Stop if initial load fails critically
		} else {
			log.Println("Initial product load completed successfully.")
			err = service.SendTelegramAlert(fmt.Sprintf("Initial product load completed successfully."))
			if err != nil {
				return
			}
		}
	} else {
		log.Printf("Products table already contains %d products. Skipping initial load.", productCount)
	}

	log.Println("Starting Wildberries Monitoring Service")

	err = service.SendGreetings(ctx)
	if err != nil {
		log.Fatalf("Monitoring service stopped with error: %v", err)
	}
	err = service.SendTelegramAlert(fmt.Sprintf("Мониторинг запущен"))
	if err != nil {
		log.Fatalf("Monitoring service stopped with error: %v", err)
	}

	// Запускаем мониторинг и product updater
	go func() {
		if err := service.RunProductUpdater(ctx); err != nil {
			log.Fatalf("Product updater stopped with error: %v", err)
		}
	}()

	if err := service.RunMonitoring(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("Monitoring service stopped with error: %v", err)
	}
}
