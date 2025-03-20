package stats

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/jmoiron/sqlx"
)

// Handlers содержит обработчики HTTP-запросов для API статистики
type Handlers struct {
	service *Service
}

// NewHandlers создает новый набор обработчиков для API статистики
func NewHandlers(db *sqlx.DB) *Handlers {
	// Создаем сервис статистики с кэшем на 5 минут и 5 рабочими горутинами
	service := NewService(db, 5*time.Minute, 5)

	return &Handlers{
		service: service,
	}
}

// RegisterRoutes регистрирует обработчики API в маршрутизаторе
func (h *Handlers) RegisterRoutes(router *mux.Router) {
	// Обработчики API
	statsAPI := router.PathPrefix("/api/stats").Subrouter()

	statsAPI.HandleFunc("/overview", h.GetOverviewStats).Methods("GET")
	statsAPI.HandleFunc("/products", h.GetTopProducts).Methods("GET")
	statsAPI.HandleFunc("/price-changes", h.GetPriceChanges).Methods("GET")
	statsAPI.HandleFunc("/stock-changes", h.GetStockChanges).Methods("GET")
	statsAPI.HandleFunc("/price-history/{id}", h.GetPriceHistory).Methods("GET")
	statsAPI.HandleFunc("/stock-history/{id}/{warehouseId}", h.GetStockHistory).Methods("GET")

	// Обработчик для страницы статистики (рендерит HTML-шаблон)
	router.HandleFunc("/stats", h.StatsPage).Methods("GET")

	log.Println("Зарегистрированы маршруты API статистики")
}

// StatsPage обрабатывает запрос на страницу статистики
func (h *Handlers) StatsPage(w http.ResponseWriter, r *http.Request) {
	// Здесь должен быть код для рендеринга шаблона
	// Но в данном случае мы просто отправляем JSON-ответ, так как
	// рендеринг шаблонов будет реализован в другом модуле

	// В реальном проекте здесь должен быть вызов функции рендеринга шаблона
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	response := map[string]interface{}{
		"success": true,
		"message": "Страница статистики",
	}

	json.NewEncoder(w).Encode(response)
}

// RegisterAPIRoutes регистрирует API маршруты для статистики
func (h *Handlers) RegisterAPIRoutes(router *mux.Router) {
	// API маршруты
	statsAPI := router.PathPrefix("/api/stats").Subrouter()

	statsAPI.HandleFunc("/overview", h.GetOverviewStats).Methods("GET")
	statsAPI.HandleFunc("/products", h.GetTopProducts).Methods("GET")
	statsAPI.HandleFunc("/price-changes", h.GetPriceChanges).Methods("GET")
	statsAPI.HandleFunc("/stock-changes", h.GetStockChanges).Methods("GET")
	statsAPI.HandleFunc("/price-history/{id}", h.GetPriceHistory).Methods("GET")
	statsAPI.HandleFunc("/stock-history/{id}/{warehouseId}", h.GetStockHistory).Methods("GET")
	statsAPI.HandleFunc("/warehouses", h.GetWarehouses).Methods("GET")

	log.Println("Зарегистрированы API маршруты статистики")
}

// GetWarehouses возвращает список всех складов
func (h *Handlers) GetWarehouses(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// Получаем список складов из базы данных
	warehouses, err := h.service.GetAllWarehouses(ctx)
	if err != nil {
		log.Printf("Ошибка при получении списка складов: %v", err)
		http.Error(w, "Ошибка при получении списка складов", http.StatusInternalServerError)
		return
	}

	// Отправляем ответ клиенту
	sendJSONResponse(w, warehouses)
}

// GetOverviewStats возвращает общую статистику как API
func (h *Handlers) GetOverviewStats(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// Проверяем параметр refresh для принудительного обновления кэша
	refresh := r.URL.Query().Get("refresh") == "true"

	stats, err := h.service.GetOverviewStats(ctx, refresh)
	if err != nil {
		log.Printf("Ошибка при получении общей статистики: %v", err)
		http.Error(w, "Ошибка при получении статистики", http.StatusInternalServerError)
		return
	}

	sendJSONResponse(w, stats)
}

// GetTopProducts возвращает список топовых продуктов как API
func (h *Handlers) GetTopProducts(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// Получаем лимит из параметров запроса
	limitStr := r.URL.Query().Get("limit")
	limit := 10 // Значение по умолчанию

	if limitStr != "" {
		parsedLimit, err := strconv.Atoi(limitStr)
		if err == nil && parsedLimit > 0 {
			limit = parsedLimit
		}
	}

	// Проверяем параметр refresh для принудительного обновления кэша
	refresh := r.URL.Query().Get("refresh") == "true"

	products, err := h.service.GetTopProducts(ctx, limit, refresh)
	if err != nil {
		log.Printf("Ошибка при получении топовых продуктов: %v", err)
		http.Error(w, "Ошибка при получении продуктов", http.StatusInternalServerError)
		return
	}

	sendJSONResponse(w, products)
}

// GetPriceChanges возвращает недавние изменения цен как API
func (h *Handlers) GetPriceChanges(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// Получаем лимит из параметров запроса
	limitStr := r.URL.Query().Get("limit")
	limit := 20 // Значение по умолчанию

	if limitStr != "" {
		parsedLimit, err := strconv.Atoi(limitStr)
		if err == nil && parsedLimit > 0 {
			limit = parsedLimit
		}
	}

	// Проверяем параметр refresh для принудительного обновления кэша
	refresh := r.URL.Query().Get("refresh") == "true"

	changes, err := h.service.GetRecentPriceChanges(ctx, limit, refresh)
	if err != nil {
		log.Printf("Ошибка при получении изменений цен: %v", err)
		http.Error(w, "Ошибка при получении изменений цен", http.StatusInternalServerError)
		return
	}

	sendJSONResponse(w, changes)
}

// GetStockChanges возвращает недавние изменения остатков как API
func (h *Handlers) GetStockChanges(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// Получаем лимит из параметров запроса
	limitStr := r.URL.Query().Get("limit")
	limit := 20 // Значение по умолчанию

	if limitStr != "" {
		parsedLimit, err := strconv.Atoi(limitStr)
		if err == nil && parsedLimit > 0 {
			limit = parsedLimit
		}
	}

	// Проверяем параметр refresh для принудительного обновления кэша
	refresh := r.URL.Query().Get("refresh") == "true"

	changes, err := h.service.GetRecentStockChanges(ctx, limit, refresh)
	if err != nil {
		log.Printf("Ошибка при получении изменений остатков: %v", err)
		http.Error(w, "Ошибка при получении изменений остатков", http.StatusInternalServerError)
		return
	}

	sendJSONResponse(w, changes)
}

// GetPriceHistory возвращает историю цен для указанного продукта
func (h *Handlers) GetPriceHistory(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// Получаем ID продукта из параметров запроса
	vars := mux.Vars(r)
	productIDStr := vars["id"]

	productID, err := strconv.Atoi(productIDStr)
	if err != nil {
		http.Error(w, "Некорректный ID продукта", http.StatusBadRequest)
		return
	}

	// Получаем количество дней из параметров запроса
	daysStr := r.URL.Query().Get("days")
	days := 30 // Значение по умолчанию

	if daysStr != "" {
		parsedDays, err := strconv.Atoi(daysStr)
		if err == nil && parsedDays > 0 {
			days = parsedDays
		}
	}

	history, err := h.service.GetPriceHistory(ctx, productID, days)
	if err != nil {
		log.Printf("Ошибка при получении истории цен: %v", err)
		http.Error(w, "Ошибка при получении истории цен", http.StatusInternalServerError)
		return
	}

	sendJSONResponse(w, history)
}

// GetStockHistory возвращает историю остатков для указанного продукта
func (h *Handlers) GetStockHistory(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// Получаем ID продукта и склада из параметров запроса
	vars := mux.Vars(r)
	productIDStr := vars["id"]
	warehouseIDStr := vars["warehouseId"]

	productID, err := strconv.Atoi(productIDStr)
	if err != nil {
		http.Error(w, "Некорректный ID продукта", http.StatusBadRequest)
		return
	}

	warehouseID, err := strconv.ParseInt(warehouseIDStr, 10, 64)
	if err != nil {
		http.Error(w, "Некорректный ID склада", http.StatusBadRequest)
		return
	}

	// Получаем количество дней из параметров запроса
	daysStr := r.URL.Query().Get("days")
	days := 30 // Значение по умолчанию

	if daysStr != "" {
		parsedDays, err := strconv.Atoi(daysStr)
		if err == nil && parsedDays > 0 {
			days = parsedDays
		}
	}

	history, err := h.service.GetStockHistory(ctx, productID, warehouseID, days)
	if err != nil {
		log.Printf("Ошибка при получении истории остатков: %v", err)
		http.Error(w, "Ошибка при получении истории остатков", http.StatusInternalServerError)
		return
	}

	sendJSONResponse(w, history)
}

// sendJSONResponse отправляет JSON-ответ клиенту
func sendJSONResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("Ошибка при кодировании JSON: %v", err)
		http.Error(w, "Ошибка при формировании ответа", http.StatusInternalServerError)
		return
	}
}
