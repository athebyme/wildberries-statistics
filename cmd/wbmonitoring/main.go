package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	"wbmonitoring/monitoring/internal/config"
	"wbmonitoring/monitoring/internal/monitoring"
	"wbmonitoring/monitoring/internal/stats"

	"github.com/gorilla/mux"
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

	// Получаем порт из переменных окружения или используем порт по умолчанию
	port := os.Getenv("SERVER_PORT")
	if port == "" {
		port = "8080" // Порт по умолчанию
	}

	// Получаем базовый путь для API из переменных окружения
	basePath := os.Getenv("API_BASE_PATH")
	if basePath == "" {
		basePath = "" // Пустой префикс по умолчанию
	}

	// Получаем идентификатор этого экземпляра
	instanceID := os.Getenv("INSTANCE_ID")
	if instanceID == "" {
		instanceID = "default"
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

	log.Printf("Starting Wildberries Monitoring Service (Instance: %s)", instanceID)

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

	router := mux.NewRouter()

	var apiRouter *mux.Router
	if basePath != "" {
		apiRouter = router.PathPrefix("/" + basePath).Subrouter()
		log.Printf("API будет доступен по базовому пути: /%s", basePath)
	} else {
		apiRouter = router
	}

	statsHandlers := stats.NewHandlers(service.GetDB())

	statsHandlers.RegisterRoutes(apiRouter)

	fs := http.FileServer(http.Dir("./public"))

	if basePath != "" {
		router.PathPrefix("/src/").Handler(http.StripPrefix("/src/", fs))
		apiRouter.PathPrefix("/src/").Handler(http.StripPrefix("/"+basePath+"/src/", fs))
	} else {
		router.PathPrefix("/src/").Handler(http.StripPrefix("/src/", fs))
	}

	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Printf("Запуск веб-сервера на порту :%s (Instance: %s)", port, instanceID)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Ошибка при запуске сервера: %v", err)
		}
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()

	log.Println("Завершение работы сервера...")
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("Ошибка при завершении работы сервера: %v", err)
	}

	cancel()
	log.Printf("Сервер успешно остановлен (Instance: %s)", instanceID)
}
