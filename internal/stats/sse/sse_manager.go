package sse

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

type Event struct {
	Type string      `json:"type"`
	Data interface{} `json:"data"`
	ID   string      `json:"id,omitempty"`
}

type SSEManager struct {
	clients        map[chan Event]bool
	registerChan   chan chan Event
	unregisterChan chan chan Event
	broadcastChan  chan Event
	mutex          sync.Mutex
}

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
					// event sent successfully
				default:
					// client is not receiving, unregister it
					delete(m.clients, client)
					close(client)
				}
			}
			m.mutex.Unlock()
		}
	}
}

func (m *SSEManager) Register() chan Event {
	client := make(chan Event, 5) // Buffer a few events
	m.registerChan <- client
	return client
}

func (m *SSEManager) Unregister(client chan Event) {
	m.unregisterChan <- client
}

func (m *SSEManager) Broadcast(eventType string, data interface{}) {
	event := Event{
		Type: eventType,
		Data: data,
		ID:   fmt.Sprintf("%d", time.Now().UnixNano()),
	}
	m.broadcastChan <- event
}

func (m *SSEManager) BroadcastPriceChange(priceChange interface{}) {
	m.Broadcast("price-change", priceChange)
}

func (m *SSEManager) BroadcastStockChange(stockChange interface{}) {
	m.Broadcast("stock-change", stockChange)
}

func (m *SSEManager) HandleSSE(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	client := m.Register()
	defer m.Unregister(client)

	notify := r.Context().Done()
	go func() {
		<-notify
		m.Unregister(client)
	}()

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	fmt.Fprintf(w, "event: connected\ndata: {\"status\":\"connected\"}\n\n")
	w.(http.Flusher).Flush()

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

			fmt.Fprintf(w, "event: %s\n", event.Type)
			fmt.Fprintf(w, "id: %s\n", event.ID)
			fmt.Fprintf(w, "data: %s\n\n", data)
			w.(http.Flusher).Flush()
		case <-ticker.C:
			fmt.Fprintf(w, ": ping %s\n\n", time.Now().Format(time.RFC3339))
			w.(http.Flusher).Flush()
		}
	}
}
