package sse

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"time"
	"wbmonitoring/monitoring/internal/stats"
)

// Global SSE manager instance
var sseManager *SSEManager

// Initialize initializes the SSE manager
func InitializeSSE() {
	sseManager = NewSSEManager()
	log.Println("SSE manager initialized")
}

// GetSSEManager returns the SSE manager instance
func GetSSEManager() *SSEManager {
	if sseManager == nil {
		InitializeSSE()
	}
	return sseManager
}

// RegisterSSERoutes registers SSE routes on the given router
func RegisterSSERoutes(router *mux.Router) {
	// Make sure SSE manager is initialized
	if sseManager == nil {
		InitializeSSE()
	}

	// Create a subrouter for SSE endpoints
	sseRouter := router.PathPrefix("/api/sse").Subrouter()

	// Apply CORS middleware
	sseRouter.Use(stats.CORSMiddleware)

	// Register SSE endpoints
	sseRouter.HandleFunc("/price-changes", sseManager.HandleSSE).Methods("GET", "OPTIONS")
	sseRouter.HandleFunc("/stock-changes", sseManager.HandleSSE).Methods("GET", "OPTIONS")
	sseRouter.HandleFunc("/product/{id}", handleProductSSE).Methods("GET", "OPTIONS")
	sseRouter.HandleFunc("/warehouse/{id}", handleWarehouseSSE).Methods("GET", "OPTIONS")

	// Register test endpoint
	sseRouter.HandleFunc("/test", TestEventHandler).Methods("GET", "OPTIONS")

	log.Println("SSE routes registered")
}

// handleProductSSE handles SSE for a specific product
func handleProductSSE(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	productID, err := strconv.Atoi(vars["id"])
	if err != nil {
		http.Error(w, "Invalid product ID", http.StatusBadRequest)
		return
	}

	// Set headers for SSE
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	// Create a client channel
	client := sseManager.Register()
	defer sseManager.Unregister(client)

	// Add initial message
	fmt.Fprintf(w, "event: connected\n")
	fmt.Fprintf(w, "data: {\"productId\":%d,\"status\":\"connected\"}\n\n", productID)
	w.(http.Flusher).Flush()

	// Filter events for this product ID
	notify := r.Context().Done()
	for {
		select {
		case <-notify:
			return
		case event := <-client:
			// Check if event is for this product
			if isEventForProduct(event, productID) {
				sendSSEEvent(w, event)
			}
		}
	}
}

// handleWarehouseSSE handles SSE for a specific warehouse
func handleWarehouseSSE(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	warehouseIDStr := vars["id"]
	warehouseID, err := strconv.ParseInt(warehouseIDStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid warehouse ID", http.StatusBadRequest)
		return
	}

	// Set headers for SSE
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	// Create a client channel
	client := sseManager.Register()
	defer sseManager.Unregister(client)

	// Add initial message
	fmt.Fprintf(w, "event: connected\n")
	fmt.Fprintf(w, "data: {\"warehouseId\":%d,\"status\":\"connected\"}\n\n", warehouseID)
	w.(http.Flusher).Flush()

	// Filter events for this warehouse ID
	notify := r.Context().Done()
	for {
		select {
		case <-notify:
			return
		case event := <-client:
			// Check if event is for this warehouse
			if isEventForWarehouse(event, warehouseID) {
				sendSSEEvent(w, event)
			}
		}
	}
}

// Helper functions to filter and send events

func isEventForProduct(event Event, productID int) bool {
	// Extract product ID from event data
	// This is a simple implementation - adjust based on your actual data structure
	data, ok := event.Data.(map[string]interface{})
	if !ok {
		return false
	}

	eventProductID, ok := data["productId"].(float64)
	if !ok {
		return false
	}

	return int(eventProductID) == productID
}

func isEventForWarehouse(event Event, warehouseID int64) bool {
	// Extract warehouse ID from event data
	// This is a simple implementation - adjust based on your actual data structure
	data, ok := event.Data.(map[string]interface{})
	if !ok {
		return false
	}

	eventWarehouseID, ok := data["warehouseId"].(float64)
	if !ok {
		return false
	}

	return int64(eventWarehouseID) == warehouseID
}

func sendSSEEvent(w http.ResponseWriter, event Event) {
	// Convert event data to JSON
	data, err := json.Marshal(event.Data)
	if err != nil {
		log.Printf("Error serializing event data: %v", err)
		return
	}

	// Send SSE message
	fmt.Fprintf(w, "event: %s\n", event.Type)
	fmt.Fprintf(w, "id: %s\n", event.ID)
	fmt.Fprintf(w, "data: %s\n\n", string(data))
	w.(http.Flusher).Flush()
}

// TestEventHandler обрабатывает запросы на отправку тестовых событий
func TestEventHandler(w http.ResponseWriter, r *http.Request) {
	// Простая проверка авторизации - в реальном приложении замените на полноценную проверку
	if r.Header.Get("X-API-Key") != "test-api-key" {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	eventType := r.URL.Query().Get("type")
	if eventType == "" {
		eventType = "price-change" // По умолчанию тестируем изменение цены
	}

	if sseManager == nil {
		http.Error(w, "SSE manager not initialized", http.StatusInternalServerError)
		return
	}

	// Создаем тестовое событие в зависимости от типа
	switch eventType {
	case "price-change":
		sendTestPriceChange()
		w.Write([]byte("Test price change event sent"))
	case "stock-change":
		sendTestStockChange()
		w.Write([]byte("Test stock change event sent"))
	default:
		http.Error(w, "Unknown event type", http.StatusBadRequest)
	}
}

// Sends a test price change event
func sendTestPriceChange() {
	if sseManager == nil {
		log.Println("SSE manager not initialized, cannot send test price change")
		return
	}

	// Generate random price change data
	oldPrice := rand.Intn(5000) + 1000
	changePercent := float64(rand.Intn(30) + 5)
	isIncrease := rand.Intn(2) == 1

	var newPrice int
	if isIncrease {
		newPrice = int(float64(oldPrice) * (1 + changePercent/100))
	} else {
		newPrice = int(float64(oldPrice) * (1 - changePercent/100))
		changePercent = -changePercent
	}

	// Create event data
	priceChange := map[string]interface{}{
		"productId":     rand.Intn(1000) + 1,
		"productName":   fmt.Sprintf("Тестовый товар %d", rand.Intn(100)),
		"vendorCode":    fmt.Sprintf("id-%d-%d", rand.Intn(100000), rand.Intn(10000)),
		"oldPrice":      oldPrice,
		"newPrice":      newPrice,
		"changeAmount":  newPrice - oldPrice,
		"changePercent": changePercent,
		"date":          time.Now().Format(time.RFC3339),
	}

	// Log the event for debugging
	jsonData, _ := json.MarshalIndent(priceChange, "", "  ")
	log.Printf("Sending test price change event: \n%s", string(jsonData))

	// Broadcast the event
	sseManager.BroadcastPriceChange(priceChange)
}

// Sends a test stock change event
func sendTestStockChange() {
	if sseManager == nil {
		log.Println("SSE manager not initialized, cannot send test stock change")
		return
	}

	// Generate random stock change data
	oldAmount := rand.Intn(100) + 10
	changePercent := float64(rand.Intn(50) + 10)
	isIncrease := rand.Intn(2) == 1

	var newAmount int
	if isIncrease {
		newAmount = int(float64(oldAmount) * (1 + changePercent/100))
	} else {
		newAmount = int(float64(oldAmount) * (1 - changePercent/100))
		if newAmount < 0 {
			newAmount = 0
		}
		changePercent = -changePercent
	}

	// Create warehouse data
	warehouseIDs := []int64{575679, 575682, 507123, 117501}
	warehouseNames := []string{"Склад SPB", "Склад MSK", "Склад RND", "Склад VLD"}
	idx := rand.Intn(len(warehouseIDs))

	// Create event data
	stockChange := map[string]interface{}{
		"productId":     rand.Intn(1000) + 1,
		"productName":   fmt.Sprintf("Тестовый товар %d", rand.Intn(100)),
		"vendorCode":    fmt.Sprintf("id-%d-%d", rand.Intn(100000), rand.Intn(10000)),
		"warehouseId":   warehouseIDs[idx],
		"warehouseName": warehouseNames[idx],
		"oldAmount":     oldAmount,
		"newAmount":     newAmount,
		"changeAmount":  newAmount - oldAmount,
		"changePercent": changePercent,
		"date":          time.Now().Format(time.RFC3339),
	}

	// Log the event for debugging
	jsonData, _ := json.MarshalIndent(stockChange, "", "  ")
	log.Printf("Sending test stock change event: \n%s", string(jsonData))

	// Broadcast the event
	sseManager.BroadcastStockChange(stockChange)
}

// RegisterTestEndpoint регистрирует тестовый эндпоинт для отправки событий SSE
func RegisterTestEndpoint(router *mux.Router) {
	router.HandleFunc("/api/sse/test", TestEventHandler).Methods("GET")
	log.Println("SSE test endpoint registered")
}
