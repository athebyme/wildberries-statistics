package main

import (
	"context"
	"errors"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	"wbmonitoring/monitoring/internal/config"
	"wbmonitoring/monitoring/internal/monitoring"
	"wbmonitoring/monitoring/internal/stats"
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

	if err := service.InitDB(); err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	productCount, err := service.GetProductCount(ctx)
	if err != nil {
		log.Fatalf("Failed to check product count: %v", err)
	}

	if productCount == 0 {
		log.Println("Products table is empty, starting initial product load...")
		if err := service.UpdateProducts(ctx); err != nil {
			log.Fatalf("Initial product load failed: %v", err)
		} else {
			log.Println("Initial product load completed successfully.")
		}
	} else {
		log.Printf("Products table already contains %d products. Skipping initial load.", productCount)
	}

	log.Println("Starting Wildberries Monitoring Service")

	// Запускаем мониторинг в фоновом режиме
	go func() {
		if err := service.RunProductUpdater(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Fatalf("Product updater stopped with error: %v", err)
		}
	}()

	go func() {
		if err := service.RunMonitoring(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Fatalf("Monitoring service stopped with error: %v", err)
		}
	}()

	// Создаем и настраиваем веб-сервер
	router := mux.NewRouter()

	// Инициализируем обработчики статистики
	statsHandlers := stats.NewHandlers(service.GetDB())

	// Регистрируем маршруты для статистики
	statsHandlers.RegisterRoutes(router)

	// Статические файлы
	fs := http.FileServer(http.Dir("./public"))
	router.PathPrefix("/src/").Handler(http.StripPrefix("/src/", fs))

	// Настройка сервера
	srv := &http.Server{
		Addr:         ":8080",
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Запуск сервера в горутине
	go func() {
		log.Println("Запуск веб-сервера на порту :8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Ошибка при запуске сервера: %v", err)
		}
	}()

	// Ожидание сигнала для завершения
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	// Создаем контекст с таймаутом для graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()

	// Завершаем работу сервера
	log.Println("Завершение работы сервера...")
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("Ошибка при завершении работы сервера: %v", err)
	}

	// Отменяем контекст мониторинга
	cancel()
	log.Println("Сервер успешно остановлен")
}
