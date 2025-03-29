package stats

import (
	"context"
	"encoding/json"
	"io"
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

// PriceChangeFilter содержит параметры фильтрации для изменений цен
type PriceChangeFilter struct {
	MinChangePercent *float64   `json:"minChangePercent"` // Минимальное изменение в процентах
	MaxChangePercent *float64   `json:"maxChangePercent"` // Максимальное изменение в процентах
	MinChangeAmount  *int       `json:"minChangeAmount"`  // Минимальное абсолютное изменение
	Since            *time.Time `json:"since"`            // Начальная дата периода
	OnlyIncreases    *bool      `json:"onlyIncreases"`    // Только повышения цен
	OnlyDecreases    *bool      `json:"onlyDecreases"`    // Только понижения цен
}

// RegisterRoutes регистрирует обработчики API в маршрутизаторе
func (h *Handlers) RegisterRoutes(router *mux.Router) {
	// Обработчики API
	statsAPI := router.PathPrefix("/api/stats").Subrouter()

	// Применяем CORS-middleware ко всем запросам к API
	statsAPI.Use(CORSMiddleware)

	// Регистрируем маршруты с поддержкой OPTIONS для preflight-запросов
	statsAPI.HandleFunc("/overview", h.GetOverviewStats).Methods("GET", "OPTIONS")
	statsAPI.HandleFunc("/products", h.GetTopProducts).Methods("GET", "OPTIONS")

	// Добавляем новый маршрут для получения списка складов
	statsAPI.HandleFunc("/warehouses", h.GetWarehouses).Methods("GET", "OPTIONS")

	// Добавляем новый маршрут для пагинированных запросов
	statsAPI.HandleFunc("/stock-changes", h.GetStockChangesWithPaginationPost).Methods("POST", "OPTIONS")
	statsAPI.HandleFunc("/price-changes", h.GetPriceChangesWithPaginationPost).Methods("POST", "OPTIONS")

	statsAPI.HandleFunc("/price-history/{id}", h.GetPriceHistory).Methods("GET", "OPTIONS")
	statsAPI.HandleFunc("/stock-history/{id}/{warehouseId}", h.GetStockHistory).Methods("GET", "OPTIONS")

	// Обработчик для страницы статистики (рендерит HTML-шаблон)
	router.HandleFunc("/stats", h.StatsPage).Methods("GET", "OPTIONS")

	log.Println("Зарегистрированы маршруты API статистики с поддержкой CORS")
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

// RefreshCacheHandler принудительно обновляет кэш статистики
func (h *Handlers) RefreshCacheHandler(w http.ResponseWriter, r *http.Request) {
	_, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Получаем тип обновления из параметров запроса
	refreshType := r.URL.Query().Get("type")

	// Проверяем, что запрос содержит секретный ключ для авторизации
	// (Простой механизм защиты, в реальности это должно быть заменено на настоящую аутентификацию)
	apiKey := r.Header.Get("X-API-Key")
	if apiKey != "your-secret-api-key" { // В реальном приложении замените на безопасное значение
		http.Error(w, "Неверный ключ API", http.StatusUnauthorized)
		return
	}

	// В зависимости от типа обновляем разные части кэша
	switch refreshType {
	case "paginated":
		// Обновляем только пагинированные данные
		go h.service.RefreshPaginatedCache(context.Background())
	case "full":
		// Обновляем весь кэш
		go h.service.RefreshCache(context.Background())
	default:
		// По умолчанию обновляем весь кэш
		go h.service.RefreshCache(context.Background())
	}

	// Отправляем ответ с поддержкой CORS
	response := map[string]interface{}{
		"success": true,
		"message": "Обновление кэша запущено",
	}

	SendJSONResponseWithCORS(w, response)
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

	vars := mux.Vars(r)
	productIDStr := vars["id"]

	productID, err := strconv.Atoi(productIDStr)
	if err != nil {
		http.Error(w, "Некорректный ID продукта", http.StatusBadRequest)
		return
	}

	daysStr := r.URL.Query().Get("days")
	days := 30

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

	daysStr := r.URL.Query().Get("days")
	days := 30

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

// GetPriceChangesWithPaginationPost обрабатывает POST-запросы для получения изменений цен
// Принимает JSON с параметрами пагинации и фильтрации
func (h *Handlers) GetPriceChangesWithPaginationPost(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	var request struct {
		Limit   int               `json:"limit"`
		Cursor  string            `json:"cursor"`
		Refresh bool              `json:"refresh"`
		Filter  PriceChangeFilter `json:"filter,omitempty"`
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		log.Printf("Ошибка чтения тела запроса для изменений цен: %v", err)
		http.Error(w, "Ошибка чтения тела запроса", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if err := json.Unmarshal(body, &request); err != nil {
		log.Printf("Ошибка декодирования JSON для изменений цен: %v", err)
		http.Error(w, "Ошибка декодирования JSON", http.StatusBadRequest)
		return
	}

	if request.Filter.Since == nil && r.URL.Query().Get("since") != "" {
		sinceStr := r.URL.Query().Get("since")
		since, err := time.Parse("2006-01-02", sinceStr)
		if err == nil {
			request.Filter.Since = &since
		} else {
			log.Printf("Ошибка парсинга даты since: %v", err)
		}
	}

	limit := 20
	if request.Limit > 0 {
		limit = request.Limit
	}
	if limit > 500 {
		limit = 500
	}

	log.Printf("Запрос изменений цен: limit=%d, cursor=%s, refresh=%v, filter=%+v",
		limit, request.Cursor, request.Refresh, request.Filter)

	startTime := time.Now()

	result, err := h.service.GetPriceChangesWithCursorAndFilter(
		ctx,
		limit,
		request.Cursor,
		request.Refresh,
		request.Filter,
	)

	requestTime := time.Since(startTime)
	log.Printf("API POST-запрос изменений цен выполнен за %v (limit=%d)", requestTime, limit)

	if err != nil {
		log.Printf("Ошибка при получении изменений цен: %v", err)
		http.Error(w, "Ошибка при получении изменений цен", http.StatusInternalServerError)
		return
	}

	// Формируем и отправляем ответ используя SendJSONResponseWithCORS
	SendJSONResponseWithCORS(w, result)
}

func (h *Handlers) GetStockChangesWithPaginationPost(w http.ResponseWriter, r *http.Request) {
	// Устанавливаем короткий таймаут для ускорения ответа
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Парсим JSON из тела запроса
	var request struct {
		Limit            int      `json:"limit"`
		Cursor           string   `json:"cursor"`
		Refresh          bool     `json:"refresh"`
		WarehouseID      *int64   `json:"warehouseId"`
		MinChangePercent *float64 `json:"minChangePercent"`
		MinChangeAmount  *int     `json:"minChangeAmount"`
		Since            *string  `json:"since"`
	}

	// Предотвращаем чтение слишком большого тела запроса
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20)) // лимит в 1MB
	if err != nil {
		http.Error(w, "Ошибка чтения тела запроса", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if err := json.Unmarshal(body, &request); err != nil {
		http.Error(w, "Ошибка декодирования JSON", http.StatusBadRequest)
		return
	}

	// Настройка значений по умолчанию
	limit := 20
	if request.Limit > 0 {
		limit = request.Limit
	}

	// Создаем объект фильтра
	filter := StockChangeFilter{
		WarehouseID:      request.WarehouseID,
		MinChangePercent: request.MinChangePercent,
		MinChangeAmount:  request.MinChangeAmount,
	}

	// Парсим дату начала периода, если указана
	if request.Since != nil {
		since, err := time.Parse("2006-01-02", *request.Since)
		if err == nil {
			filter.Since = &since
		} else {
			log.Printf("Ошибка парсинга даты since: %v", err)
		}
	}

	// Измеряем время запроса для логирования производительности
	startTime := time.Now()

	// Получаем данные с пагинацией и фильтрацией
	result, err := h.service.GetStockChangesWithCursor(ctx, limit, request.Cursor, filter, request.Refresh)

	requestTime := time.Since(startTime)
	log.Printf("API запрос изменений остатков выполнен за %v (limit=%d)", requestTime, limit)

	if err != nil {
		log.Printf("Ошибка при получении изменений остатков с пагинацией: %v", err)
		http.Error(w, "Ошибка при получении изменений остатков", http.StatusInternalServerError)
		return
	}

	SendJSONResponseWithCORS(w, result)
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

// CORSMiddleware добавляет CORS-заголовки ко всем ответам
func CORSMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Устанавливаем CORS-заголовки
		w.Header().Set("Access-Control-Allow-Origin", "*") // Разрешаем запросы с любых источников
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key")

		// Обработка preflight-запросов
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		// Продолжаем выполнение следующего обработчика
		next.ServeHTTP(w, r)
	})
}

// SendJSONResponseWithCORS отправляет JSON-ответ клиенту с CORS-заголовками
func SendJSONResponseWithCORS(w http.ResponseWriter, data interface{}) {
	// Устанавливаем CORS-заголовки
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS, PUT, DELETE")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key")

	// Устанавливаем тип контента
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	// Кодируем и отправляем данные
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("Ошибка при кодировании JSON: %v", err)
		http.Error(w, "Ошибка при формировании ответа", http.StatusInternalServerError)
		return
	}
}
