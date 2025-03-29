package sse

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

// Event represents a server-sent event
type Event struct {
	Type string      `json:"type"`
	Data interface{} `json:"data"`
	ID   string      `json:"id,omitempty"`
}

// SSEManager manages SSE client connections and broadcasts events
type SSEManager struct {
	clients        map[chan Event]bool
	registerChan   chan chan Event
	unregisterChan chan chan Event
	broadcastChan  chan Event
	mutex          sync.Mutex
}

// NewSSEManager creates a new SSE manager
func NewSSEManager() *SSEManager {
	manager := &SSEManager{
		clients:        make(map[chan Event]bool),
		registerChan:   make(chan chan Event),
		unregisterChan: make(chan chan Event),
		broadcastChan:  make(chan Event),
	}

	go manager.run()
	return manager
}

// run listens for client registrations and broadcasts
func (m *SSEManager) run() {
	for {
		select {
		case client := <-m.registerChan:
			m.mutex.Lock()
			m.clients[client] = true
			m.mutex.Unlock()
			log.Printf("SSE client connected, total: %d", len(m.clients))

		case client := <-m.unregisterChan:
			m.mutex.Lock()
			if _, ok := m.clients[client]; ok {
				delete(m.clients, client)
				close(client)
			}
			m.mutex.Unlock()
			log.Printf("SSE client disconnected, total: %d", len(m.clients))

		case event := <-m.broadcastChan:
			m.mutex.Lock()
			for client := range m.clients {
				select {
				case client <- event:
					// Event sent successfully
				default:
					// Client is not receiving, unregister it
					delete(m.clients, client)
					close(client)
				}
			}
			m.mutex.Unlock()
		}
	}
}

// Register registers a new client
func (m *SSEManager) Register() chan Event {
	client := make(chan Event, 5) // Buffer a few events
	m.registerChan <- client
	return client
}

// Unregister unregisters a client
func (m *SSEManager) Unregister(client chan Event) {
	m.unregisterChan <- client
}

// Broadcast broadcasts an event to all registered clients
func (m *SSEManager) Broadcast(eventType string, data interface{}) {
	event := Event{
		Type: eventType,
		Data: data,
		ID:   fmt.Sprintf("%d", time.Now().UnixNano()),
	}
	m.broadcastChan <- event
}

// BroadcastPriceChange broadcasts a price change event
func (m *SSEManager) BroadcastPriceChange(priceChange interface{}) {
	m.Broadcast("price-change", priceChange)
}

// BroadcastStockChange broadcasts a stock change event
func (m *SSEManager) BroadcastStockChange(stockChange interface{}) {
	m.Broadcast("stock-change", stockChange)
}

// HandleSSE handles SSE connections
func (m *SSEManager) HandleSSE(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-API-Key")

	requestPath := r.URL.Path
	clientIP := r.RemoteAddr
	userAgent := r.UserAgent()
	log.Printf("New SSE connection established: %s from %s (%s)", requestPath, clientIP, userAgent)

	client := m.Register()
	defer m.Unregister(client)

	notify := r.Context().Done()
	go func() {
		<-notify
		m.Unregister(client)
		log.Printf("SSE connection closed: %s from %s", requestPath, clientIP)
	}()

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	fmt.Fprintf(w, "event: connected\ndata: {\"status\":\"connected\",\"path\":\"%s\"}\n\n", requestPath)
	w.(http.Flusher).Flush()
	log.Printf("Sent initial connected event to client: %s", requestPath)

	for {
		select {
		case <-notify:
			return
		case event := <-client:
			data, err := json.Marshal(event.Data)
			if err != nil {
				log.Printf("Error serializing event data: %v", err)
				continue
			}

			logData, _ := json.Marshal(map[string]interface{}{
				"type": event.Type,
				"id":   event.ID,
				"path": requestPath,
			})
			log.Printf("Sending SSE event: %s", string(logData))

			fmt.Fprintf(w, "event: %s\n", event.Type)
			fmt.Fprintf(w, "id: %s\n", event.ID)
			fmt.Fprintf(w, "data: %s\n\n", data)
			w.(http.Flusher).Flush()
		case <-ticker.C:
			pingTime := time.Now().Format(time.RFC3339)
			fmt.Fprintf(w, ": ping %s\n\n", pingTime)
			w.(http.Flusher).Flush()

		}
	}
}
