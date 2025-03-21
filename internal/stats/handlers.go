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

	// Добавляем новый маршрут для пагинированных запросов
	statsAPI.HandleFunc("/stock-changes/paginated", h.GetStockChangesWithPagination).Methods("POST")
	statsAPI.HandleFunc("/price-changes/paginated", h.GetPriceChangesWithPagination).Methods("POST")

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

	// Стандартный обработчик изменений цен (для обратной совместимости)
	statsAPI.HandleFunc("/price-changes", h.GetPriceChanges).Methods("GET")

	// Новые обработчики с пагинацией
	statsAPI.HandleFunc("/price-changes/paginated", h.GetPriceChangesWithPagination).Methods("GET")

	statsAPI.HandleFunc("/stock-changes", h.GetStockChanges).Methods("GET")
	statsAPI.HandleFunc("/stock-changes/paginated", h.GetStockChangesWithPagination).Methods("GET")
	statsAPI.HandleFunc("/price-history/{id}", h.GetPriceHistory).Methods("GET")
	statsAPI.HandleFunc("/stock-history/{id}/{warehouseId}", h.GetStockHistory).Methods("GET")
	statsAPI.HandleFunc("/warehouses", h.GetWarehouses).Methods("GET")

	// Маршрут для принудительного обновления кэша
	statsAPI.HandleFunc("/refresh-cache", h.RefreshCacheHandler).Methods("POST")

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

	// Отправляем ответ
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	response := map[string]interface{}{
		"success": true,
		"message": "Обновление кэша запущено",
	}

	json.NewEncoder(w).Encode(response)
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

// GetPriceChangesWithPagination возвращает недавние изменения цен с поддержкой пагинации
func (h *Handlers) GetPriceChangesWithPagination(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second) // Увеличиваем таймаут до 10 секунд
	defer cancel()

	// Получаем параметры запроса
	limitStr := r.URL.Query().Get("limit")
	cursor := r.URL.Query().Get("cursor")
	refresh := r.URL.Query().Get("refresh") == "true"

	limit := 20 // Значение по умолчанию
	if limitStr != "" {
		parsedLimit, err := strconv.Atoi(limitStr)
		if err == nil && parsedLimit > 0 {
			limit = parsedLimit
		}
	}

	// Ограничиваем максимальный размер страницы
	if limit > 100 {
		limit = 100
	}

	// Получаем данные с пагинацией
	result, err := h.service.GetPriceChangesWithCursor(ctx, limit, cursor, refresh)
	if err != nil {
		log.Printf("Ошибка при получении изменений цен с пагинацией: %v", err)
		http.Error(w, "Ошибка при получении изменений цен", http.StatusInternalServerError)
		return
	}

	sendJSONResponse(w, result)
}

// GetStockChangesWithPagination возвращает недавние изменения остатков с поддержкой пагинации и фильтрации
func (h *Handlers) GetStockChangesWithPagination(w http.ResponseWriter, r *http.Request) {
	// Устанавливаем короткий таймаут для ускорения ответа
	ctx, cancel := context.WithTimeout(r.Context(), 12*time.Second)
	defer cancel()

	// Получаем параметры запроса
	limitStr := r.URL.Query().Get("limit")
	cursor := r.URL.Query().Get("cursor")
	refresh := r.URL.Query().Get("refresh") == "true"

	// Параметры фильтрации
	warehouseIDStr := r.URL.Query().Get("warehouseId")
	minChangePercentStr := r.URL.Query().Get("minChangePercent") // Приоритетный параметр
	minChangeAmountStr := r.URL.Query().Get("minChangeAmount")   // Запасной параметр
	sinceStr := r.URL.Query().Get("since")                       // Формат: YYYY-MM-DD

	limit := 20 // Значение по умолчанию
	if limitStr != "" {
		parsedLimit, err := strconv.Atoi(limitStr)
		if err == nil && parsedLimit > 0 {
			limit = parsedLimit
		}
	}

	// Снимаем жесткое ограничение на размер страницы
	// Это позволит возвращать больше записей, если запрошено

	// Создаем объект фильтра
	filter := StockChangeFilter{}

	// Парсим warehouse_id
	if warehouseIDStr != "" {
		warehouseID, err := strconv.ParseInt(warehouseIDStr, 10, 64)
		if err == nil {
			filter.WarehouseID = &warehouseID
		} else {
			log.Printf("Ошибка парсинга warehouse_id: %v", err)
		}
	}

	// В первую очередь парсим минимальное процентное изменение
	if minChangePercentStr != "" {
		minChangePercent, err := strconv.ParseFloat(minChangePercentStr, 64)
		if err == nil && minChangePercent > 0 {
			filter.MinChangePercent = &minChangePercent
		} else {
			log.Printf("Ошибка парсинга min_change_percent: %v", err)
		}
	}

	// Парсим минимальное абсолютное изменение только если процентное не задано
	if minChangeAmountStr != "" && filter.MinChangePercent == nil {
		minChangeAmount, err := strconv.Atoi(minChangeAmountStr)
		if err == nil && minChangeAmount > 0 {
			filter.MinChangeAmount = &minChangeAmount
		} else {
			log.Printf("Ошибка парсинга min_change_amount: %v", err)
		}
	}

	// Парсим дату начала периода
	if sinceStr != "" {
		since, err := time.Parse("2006-01-02", sinceStr)
		if err == nil {
			filter.Since = &since
		} else {
			log.Printf("Ошибка парсинга даты since: %v", err)
		}
	}

	// Измеряем время запроса для логирования производительности
	startTime := time.Now()

	// Получаем данные с пагинацией и фильтрацией
	result, err := h.service.GetStockChangesWithCursor(ctx, limit, cursor, filter, refresh)

	requestTime := time.Since(startTime)
	log.Printf("API запрос изменений остатков выполнен за %v (limit=%d)", requestTime, limit)

	if err != nil {
		log.Printf("Ошибка при получении изменений остатков с пагинацией: %v", err)
		http.Error(w, "Ошибка при получении изменений остатков", http.StatusInternalServerError)
		return
	}

	// Добавляем заголовки кэширования для браузера
	w.Header().Set("Cache-Control", "private, max-age=60")

	// Отправляем ответ
	sendJSONResponse(w, result)
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
