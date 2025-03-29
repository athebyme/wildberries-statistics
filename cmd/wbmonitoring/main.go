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
	"wbmonitoring/monitoring/internal/stats/sse"
)

func main() {
	cfg := config.LoadConfig()

	if cfg.ApiKey == "" {
		log.Fatal("WB_API_KEY environment variable is required")
	}

	if cfg.TelegramToken == "" {
		log.Fatal("TELEGRAM_TOKEN environment variable is required")
	}

	if cfg.TelegramChatID == 0 {
		log.Fatal("TELEGRAM_CHAT_ID environment variable is required")
	}

	port := os.Getenv("SERVER_PORT")
	if port == "" {
		port = "8080"
	}

	basePath := os.Getenv("API_BASE_PATH")
	if basePath == "" {
		basePath = "" // Пустой префикс по умолчанию
	}

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

	SetupSSE(router, service)

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

// SetupSSE initializes and connects SSE functionality
func SetupSSE(router *mux.Router, monitoringService *monitoring.Service) {
	sse.InitializeSSE()

	sse.RegisterSSERoutes(router)

	if monitoringService != nil {
		sseManager := sse.GetSSEManager()
		monitoringService.AddSSENotification(sseManager)
		log.Println("Monitoring service connected to SSE")
	} else {
		log.Println("Warning: Monitoring service not available for SSE integration")
	}
}
