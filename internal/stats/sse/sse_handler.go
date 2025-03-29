package sse

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"strconv"
	"wbmonitoring/monitoring/internal/stats"
)

var sseManager *SSEManager

func InitializeSSE() {
	sseManager = NewSSEManager()
	log.Println("SSE manager initialized")
}

func GetSSEManager() *SSEManager {
	if sseManager == nil {
		InitializeSSE()
	}
	return sseManager
}

func RegisterSSERoutes(router *mux.Router) {
	if sseManager == nil {
		InitializeSSE()
	}

	sseRouter := router.PathPrefix("/api/sse").Subrouter()

	sseRouter.Use(stats.CORSMiddleware)

	// Register SSE endpoints
	sseRouter.HandleFunc("/price-changes", sseManager.HandleSSE).Methods("GET", "OPTIONS")
	sseRouter.HandleFunc("/stock-changes", sseManager.HandleSSE).Methods("GET", "OPTIONS")
	sseRouter.HandleFunc("/product/{id}", handleProductSSE).Methods("GET", "OPTIONS")
	sseRouter.HandleFunc("/warehouse/{id}", handleWarehouseSSE).Methods("GET", "OPTIONS")

	log.Println("SSE routes registered")
}

func handleProductSSE(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	productID, err := strconv.Atoi(vars["id"])
	if err != nil {
		http.Error(w, "Invalid product ID", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	client := sseManager.Register()
	defer sseManager.Unregister(client)

	// Add initial message
	fmt.Fprintf(w, "event: connected\n")
	fmt.Fprintf(w, "data: {\"productId\":%d,\"status\":\"connected\"}\n\n", productID)
	w.(http.Flusher).Flush()

	notify := r.Context().Done()
	for {
		select {
		case <-notify:
			return
		case event := <-client:
			if isEventForProduct(event, productID) {
				sendSSEEvent(w, event)
			}
		}
	}
}

func handleWarehouseSSE(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	warehouseIDStr := vars["id"]
	warehouseID, err := strconv.ParseInt(warehouseIDStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid warehouse ID", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	client := sseManager.Register()
	defer sseManager.Unregister(client)

	// Add initial message
	fmt.Fprintf(w, "event: connected\n")
	fmt.Fprintf(w, "data: {\"warehouseId\":%d,\"status\":\"connected\"}\n\n", warehouseID)
	w.(http.Flusher).Flush()

	notify := r.Context().Done()
	for {
		select {
		case <-notify:
			return
		case event := <-client:
			if isEventForWarehouse(event, warehouseID) {
				sendSSEEvent(w, event)
			}
		}
	}
}

func isEventForProduct(event Event, productID int) bool {
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
	data, err := json.Marshal(event.Data)
	if err != nil {
		log.Printf("Error serializing event data: %v", err)
		return
	}

	fmt.Fprintf(w, "event: %s\n", event.Type)
	fmt.Fprintf(w, "id: %s\n", event.ID)
	fmt.Fprintf(w, "data: %s\n\n", string(data))
	w.(http.Flusher).Flush()
}
