package config

import (
	"fmt"
	"log"
	"os"
	"time"
)

// Конфигурация сервиса
const (
	WorkerCount          = 5
	MaxRetries           = 3
	RetryInterval        = 2 * time.Second
	RequestTimeout       = 100 * time.Second
	MonitoringInterval   = 10 * time.Minute
	PriceChangeThreshold = 20  // процент изменения цены для уведомления
	StockChangeThreshold = 100 // процент изменения количества для уведомления

	ProductUpdateInterval = 5 * time.Hour // Интервал обновления продуктов
)

// Config represents the service configuration.
type Config struct {
	WorkerCount           int
	MaxRetries            int
	RetryInterval         time.Duration
	RequestTimeout        time.Duration
	MonitoringInterval    time.Duration
	ProductUpdateInterval time.Duration
	ApiKey                string
	TelegramToken         string
	TelegramChatID        int64
	WarehouseID           int64
	PGConnString          string
	PriceThreshold        float64
	StockThreshold        float64
	AllowedUserIDs        []int64
}

// LoadConfig loads the configuration from environment variables.
func LoadConfig() Config {
	return Config{
		WorkerCount:           getEnvInt("WORKER_COUNT", WorkerCount),
		MaxRetries:            getEnvInt("MAX_RETRIES", MaxRetries),
		RetryInterval:         time.Duration(getEnvInt("RETRY_INTERVAL_SEC", int(RetryInterval.Seconds()))) * time.Second,
		RequestTimeout:        time.Duration(getEnvInt("REQUEST_TIMEOUT_SEC", int(RequestTimeout.Seconds()))) * time.Second,
		MonitoringInterval:    time.Duration(getEnvInt("MONITORING_INTERVAL_MIN", int(MonitoringInterval.Minutes()))) * time.Minute,
		ProductUpdateInterval: time.Duration(getEnvInt("PRODUCT_UPDATE_INTERVAL_HOUR", int(ProductUpdateInterval.Hours()))) * time.Hour, // Интервал обновления продуктов
		ApiKey:                getEnvString("WB_API_KEY", ""),
		TelegramToken:         getEnvString("TELEGRAM_TOKEN", ""),
		TelegramChatID:        int64(getEnvInt("TELEGRAM_CHAT_ID", 0)),
		WarehouseID:           int64(getEnvInt("WAREHOUSE_ID", 0)),
		PGConnString:          getEnvString("PG_CONN_STRING", "postgres://bananzza:bananzza_monitor@localhost:5432/wbmonitoring?sslmode=disable"),
		PriceThreshold:        getEnvFloat("PRICE_THRESHOLD", PriceChangeThreshold),
		StockThreshold:        getEnvFloat("STOCK_THRESHOLD", StockChangeThreshold),
		AllowedUserIDs:        []int64{int64(getEnvInt("TELEGRAM_CHAT_ID", 0))},
	}
}

// Вспомогательные функции для получения переменных окружения
func getEnvString(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func getEnvInt(key string, defaultValue int) int {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}

	intValue := defaultValue
	_, err := fmt.Sscanf(value, "%d", &intValue)
	if err != nil {
		log.Printf("Warning: invalid value for %s: %s, using default: %d", key, value, defaultValue)
		return defaultValue
	}

	return intValue
}

func getEnvFloat(key string, defaultValue float64) float64 {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}

	floatValue := defaultValue
	_, err := fmt.Sscanf(value, "%f", &floatValue)
	if err != nil {
		log.Printf("Warning: invalid value for %s: %s, using default: %f", key, value, defaultValue)
		return defaultValue
	}

	return floatValue
}
