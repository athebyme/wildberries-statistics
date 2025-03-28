package stats

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
	"wbmonitoring/monitoring/internal/db"
	"wbmonitoring/monitoring/internal/models"

	"github.com/jmoiron/sqlx"
)

// Service представляет сервис статистики
type Service struct {
	db         *sqlx.DB
	cache      *Cache
	workerPool *WorkerPool
	mu         sync.RWMutex
}

// NewService создает новый экземпляр сервиса статистики
func NewService(database *sqlx.DB, cacheExpiration time.Duration, numWorkers int) *Service {
	service := &Service{
		db:         database,
		cache:      NewCache(cacheExpiration),
		workerPool: NewWorkerPool(numWorkers),
	}

	// Запускаем фоновое обновление кэша
	go service.refreshCachePeriodically(context.Background(), 15*time.Minute)

	return service
}

// Обновление кэша с заданным интервалом
func (s *Service) refreshCachePeriodically(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.RefreshCache(ctx)
		case <-ctx.Done():
			return
		}
	}
}

// RefreshCache обновляет все кэшированные данные
func (s *Service) RefreshCache(ctx context.Context) {
	log.Println("Начало обновления кэша статистики...")

	// Обновляем разные типы кэшированных данных параллельно
	var wg sync.WaitGroup
	wg.Add(6) // Теперь у нас 6 параллельных задач вместо 4

	go func() {
		defer wg.Done()
		_, err := s.GetOverviewStats(ctx, true)
		if err != nil {
			log.Printf("Ошибка при обновлении кэша общей статистики: %v", err)
		}
	}()

	go func() {
		defer wg.Done()
		_, err := s.GetTopProducts(ctx, 10, true)
		if err != nil {
			log.Printf("Ошибка при обновлении кэша топовых продуктов: %v", err)
		}
	}()

	go func() {
		defer wg.Done()
		_, err := s.GetRecentPriceChanges(ctx, 20, true)
		if err != nil {
			log.Printf("Ошибка при обновлении кэша изменений цен: %v", err)
		}
	}()

	go func() {
		defer wg.Done()
		_, err := s.GetRecentStockChanges(ctx, 20, true)
		if err != nil {
			log.Printf("Ошибка при обновлении кэша изменений остатков: %v", err)
		}
	}()

	// Добавляем обновление пагинированных данных
	go func() {
		defer wg.Done()
		_, err := s.GetPriceChangesWithCursor(ctx, 20, "", true)
		if err != nil {
			log.Printf("Ошибка при обновлении кэша пагинированных изменений цен: %v", err)
		}
	}()

	go func() {
		defer wg.Done()
		// Пустой фильтр для получения всех данных
		emptyFilter := StockChangeFilter{}
		_, err := s.GetStockChangesWithCursor(ctx, 20, "", emptyFilter, true)
		if err != nil {
			log.Printf("Ошибка при обновлении кэша пагинированных изменений остатков: %v", err)
		}
	}()

	wg.Wait()
	log.Println("Обновление кэша статистики завершено")
}

// GetAllWarehouses возвращает список всех складов
func (s *Service) GetAllWarehouses(ctx context.Context) ([]models.Warehouse, error) {
	// Проверяем кэш
	cacheKey := "all_warehouses"
	if cachedWarehouses, found := s.cache.Get(cacheKey); found {
		return cachedWarehouses.([]models.Warehouse), nil
	}

	// Если в кэше нет, получаем из базы данных
	warehouses, err := db.GetAllWarehouses(ctx, s.db)
	if err != nil {
		return nil, fmt.Errorf("ошибка получения списка складов: %w", err)
	}

	// Сохраняем в кэш
	s.cache.Set(cacheKey, warehouses)

	return warehouses, nil
}

// RefreshPaginatedCache обновляет только кэш для пагинированных данных
// Полезно для частого обновления только этой части кэша
func (s *Service) RefreshPaginatedCache(ctx context.Context) {
	log.Println("Начало обновления кэша пагинированных данных...")

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		_, err := s.GetPriceChangesWithCursor(ctx, 20, "", true)
		if err != nil {
			log.Printf("Ошибка при обновлении кэша пагинированных изменений цен: %v", err)
		}
	}()

	go func() {
		defer wg.Done()
		// Пустой фильтр для получения всех данных
		emptyFilter := StockChangeFilter{}
		_, err := s.GetStockChangesWithCursor(ctx, 20, "", emptyFilter, true)
		if err != nil {
			log.Printf("Ошибка при обновлении кэша пагинированных изменений остатков: %v", err)
		}
	}()

	wg.Wait()
	log.Println("Обновление кэша пагинированных данных завершено")
}

// OverviewStats содержит общую статистику
type OverviewStats struct {
	TotalProducts     int       `json:"totalProducts"`
	TotalWarehouses   int       `json:"totalWarehouses"`
	AvgPrice          float64   `json:"avgPrice"`
	TotalStock        int       `json:"totalStock"`
	AvgStock          float64   `json:"avgStock"`
	LastUpdated       time.Time `json:"lastUpdated"`
	MostExpensiveItem string    `json:"mostExpensiveItem"`
	CheapestItem      string    `json:"cheapestItem"`
	LowStockItems     int       `json:"lowStockItems"`     // Товары с остатками менее 10
	LowStockThreshold int       `json:"lowStockThreshold"` // Порог для определения низкого остатка
}

// GetOverviewStats - оптимизированная версия
func (s *Service) GetOverviewStats(ctx context.Context, forceRefresh bool) (*OverviewStats, error) {
	// Добавляем короткий таймаут
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	cacheKey := "overview_stats"

	// Проверяем кэш
	if !forceRefresh {
		if cachedStats, found := s.cache.Get(cacheKey); found {
			return cachedStats.(*OverviewStats), nil
		}
	}

	// Оптимизированный запрос - объединяем все в один запрос
	query := `
        WITH latest_prices AS (
            SELECT DISTINCT ON (product_id)
                product_id, final_price
            FROM prices
            ORDER BY product_id, recorded_at DESC
        ),
        price_stats AS (
            SELECT 
                COALESCE(AVG(final_price), 0) as avg_price,
                (SELECT name FROM products WHERE id = (
                    SELECT product_id FROM latest_prices ORDER BY final_price DESC LIMIT 1
                )) as most_expensive_item,
                (SELECT name FROM products WHERE id = (
                    SELECT product_id FROM latest_prices ORDER BY final_price ASC LIMIT 1
                )) as cheapest_item
            FROM latest_prices
        ),
        latest_stocks AS (
            SELECT DISTINCT ON (product_id, warehouse_id)
                product_id, warehouse_id, amount
            FROM stocks
            ORDER BY product_id, warehouse_id, recorded_at DESC
        ),
        stock_stats AS (
            SELECT 
                SUM(amount) as total_stock,
                AVG(amount) as avg_stock
            FROM latest_stocks
        ),
        low_stock_count AS (
            SELECT COUNT(DISTINCT product_id) as count
            FROM (
                SELECT 
                    product_id,
                    SUM(amount) as total_amount
                FROM latest_stocks
                GROUP BY product_id
                HAVING SUM(amount) < 10
            ) s
        ),
        counts AS (
            SELECT 
                (SELECT COUNT(*) FROM products) as product_count,
                (SELECT COUNT(*) FROM warehouses) as warehouse_count
        )
        SELECT 
            counts.product_count,
            counts.warehouse_count,
            COALESCE(price_stats.avg_price, 0) as avg_price,
            COALESCE(stock_stats.total_stock, 0) as total_stock,
            COALESCE(stock_stats.avg_stock, 0) as avg_stock,
            COALESCE(price_stats.most_expensive_item, '') as most_expensive_item,
            COALESCE(price_stats.cheapest_item, '') as cheapest_item,
            COALESCE(low_stock_count.count, 0) as low_stock_items
        FROM
            counts,
            price_stats,
            stock_stats,
            low_stock_count
    `

	startTime := time.Now()

	type statsResult struct {
		ProductCount      int     `db:"product_count"`
		WarehouseCount    int     `db:"warehouse_count"`
		AvgPrice          float64 `db:"avg_price"`
		TotalStock        int     `db:"total_stock"`
		AvgStock          float64 `db:"avg_stock"`
		MostExpensiveItem string  `db:"most_expensive_item"`
		CheapestItem      string  `db:"cheapest_item"`
		LowStockItems     int     `db:"low_stock_items"`
	}

	var result statsResult
	err := s.db.GetContext(ctx, &result, query)
	queryTime := time.Since(startTime)

	if err != nil {
		log.Printf("Ошибка запроса общей статистики (время: %v): %v", queryTime, err)
		return nil, fmt.Errorf("ошибка получения общей статистики: %w", err)
	}

	log.Printf("Запрос общей статистики выполнен за %v", queryTime)

	// Формируем результат
	stats := &OverviewStats{
		TotalProducts:     result.ProductCount,
		TotalWarehouses:   result.WarehouseCount,
		AvgPrice:          result.AvgPrice,
		TotalStock:        result.TotalStock,
		AvgStock:          result.AvgStock,
		LastUpdated:       time.Now(),
		MostExpensiveItem: result.MostExpensiveItem,
		CheapestItem:      result.CheapestItem,
		LowStockItems:     result.LowStockItems,
		LowStockThreshold: 10,
	}

	// Кэшируем результат с небольшим TTL
	s.cache.SetWithTTL(cacheKey, stats, 5*time.Minute)

	return stats, nil
}

// ProductStats содержит статистику по продукту
type ProductStats struct {
	ID           int     `json:"id"`
	NmID         int     `json:"nmId"`
	VendorCode   string  `json:"vendorCode"`
	Name         string  `json:"name"`
	CurrentPrice int     `json:"currentPrice"`
	PriceChange  float64 `json:"priceChange"` // В процентах за последние 7 дней
	TotalStock   int     `json:"totalStock"`
	StockChange  float64 `json:"stockChange"` // В процентах за последние 7 дней
	LastUpdated  string  `json:"lastUpdated"` // Дата последнего обновления данных
}

// GetTopProducts - оптимизированная версия
func (s *Service) GetTopProducts(ctx context.Context, limit int, forceRefresh bool) ([]ProductStats, error) {
	// Короткий таймаут
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()

	cacheKey := fmt.Sprintf("top_products_%d", limit)

	// Проверяем кэш
	if !forceRefresh {
		if cachedStats, found := s.cache.Get(cacheKey); found {
			return cachedStats.([]ProductStats), nil
		}
	}

	// Оптимизированный запрос - объединяем все в один запрос
	query := `
        WITH latest_prices AS (
            SELECT DISTINCT ON (product_id)
                product_id, price, final_price, recorded_at
            FROM prices
            ORDER BY product_id, recorded_at DESC
        ),
        week_ago_prices AS (
            SELECT DISTINCT ON (product_id)
                product_id, price
            FROM prices 
            WHERE recorded_at <= NOW() - INTERVAL '7 days'
            ORDER BY product_id, recorded_at DESC
        ),
        latest_stocks AS (
            SELECT 
                product_id, 
                SUM(amount) as total_stock
            FROM (
                SELECT DISTINCT ON (product_id, warehouse_id)
                    product_id, warehouse_id, amount
                FROM stocks
                ORDER BY product_id, warehouse_id, recorded_at DESC
            ) s
            GROUP BY product_id
        ),
        week_ago_stocks AS (
            SELECT 
                product_id, 
                SUM(amount) as total_stock
            FROM (
                SELECT DISTINCT ON (product_id, warehouse_id)
                    product_id, warehouse_id, amount
                FROM stocks
                WHERE recorded_at <= NOW() - INTERVAL '7 days'
                ORDER BY product_id, warehouse_id, recorded_at DESC
            ) s
            GROUP BY product_id
        ),
        product_stats AS (
            SELECT 
                p.id,
                p.nm_id,
                p.vendor_code,
                p.name,
                lp.price as current_price,
                CASE 
                    WHEN wap.price IS NOT NULL AND wap.price > 0 
                    THEN ((lp.price - wap.price)::float / wap.price) * 100 
                    ELSE 0 
                END as price_change,
                COALESCE(ls.total_stock, 0) as total_stock,
                CASE 
                    WHEN was.total_stock IS NOT NULL AND was.total_stock > 0 
                    THEN ((ls.total_stock - was.total_stock)::float / was.total_stock) * 100 
                    ELSE 0 
                END as stock_change,
                to_char(lp.recorded_at, 'DD.MM.YYYY HH24:MI') as last_updated
            FROM products p
            JOIN latest_prices lp ON p.id = lp.product_id
            LEFT JOIN week_ago_prices wap ON p.id = wap.product_id
            LEFT JOIN latest_stocks ls ON p.id = ls.product_id
            LEFT JOIN week_ago_stocks was ON p.id = was.product_id
        )
        SELECT 
            id,
            nm_id as "nmId",
            vendor_code as "vendorCode",
            name,
            current_price as "currentPrice",
            price_change as "priceChange",
            total_stock as "totalStock",
            stock_change as "stockChange",
            last_updated as "lastUpdated"
        FROM product_stats
        ORDER BY ABS(price_change) DESC, ABS(stock_change) DESC
        LIMIT $1
    `

	startTime := time.Now()

	var result []ProductStats
	err := s.db.SelectContext(ctx, &result, query, limit)
	queryTime := time.Since(startTime)

	if err != nil {
		log.Printf("Ошибка запроса топовых продуктов (время: %v): %v", queryTime, err)
		return nil, fmt.Errorf("ошибка получения списка продуктов: %w", err)
	}

	log.Printf("Запрос топовых продуктов выполнен за %v", queryTime)

	// Кэшируем результат с небольшим TTL
	s.cache.SetWithTTL(cacheKey, result, 5*time.Minute)

	return result, nil
}

// PriceChange представляет значительное изменение цены
type PriceChange struct {
	ProductID     int       `db:"product_id" json:"productId"`
	ProductName   string    `db:"product_name" json:"productName"`
	VendorCode    string    `db:"vendor_code" json:"vendorCode"`
	OldPrice      int       `db:"old_price" json:"oldPrice"`
	NewPrice      int       `db:"new_price" json:"newPrice"`
	ChangeAmount  int       `db:"change_amount" json:"changeAmount"`
	ChangePercent float64   `db:"change_percent" json:"changePercent"`
	Date          time.Time `db:"change_date" json:"date"`
}

// GetRecentPriceChanges - оптимизированная версия
func (s *Service) GetRecentPriceChanges(ctx context.Context, limit int, forceRefresh bool) ([]PriceChange, error) {
	// Короткий таймаут
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	cacheKey := fmt.Sprintf("recent_price_changes_%d", limit)

	// Проверяем кэш
	if !forceRefresh {
		if cachedData, found := s.cache.Get(cacheKey); found {
			return cachedData.([]PriceChange), nil
		}
	}

	// Оптимизированный запрос - используем оконные функции
	query := `
        WITH recent_prices AS (
            SELECT 
                product_id,
                price,
                recorded_at,
                LAG(price) OVER (PARTITION BY product_id ORDER BY recorded_at) AS prev_price,
                LAG(recorded_at) OVER (PARTITION BY product_id ORDER BY recorded_at) AS prev_recorded_at
            FROM prices
            WHERE recorded_at > NOW() - INTERVAL '30 days'
        ),
        significant_changes AS (
            SELECT DISTINCT ON (product_id)
                product_id,
                prev_price as old_price,
                price as new_price,
                price - prev_price as change_amount,
                CASE WHEN prev_price > 0 THEN ((price - prev_price)::float / prev_price) * 100 ELSE 0 END as change_percent,
                recorded_at as change_date
            FROM recent_prices
            WHERE prev_price IS NOT NULL
            AND ABS(((price - prev_price)::float / NULLIF(prev_price, 0)) * 100) >= 5
            ORDER BY product_id, ABS(((price - prev_price)::float / NULLIF(prev_price, 0)) * 100) DESC
        )
        SELECT 
            sc.product_id,
            p.name as product_name,
            p.vendor_code,
            sc.old_price,
            sc.new_price,
            sc.change_amount,
            sc.change_percent,
            sc.change_date
        FROM significant_changes sc
        JOIN products p ON sc.product_id = p.id
        ORDER BY ABS(sc.change_percent) DESC, sc.change_date DESC
        LIMIT $1
    `

	startTime := time.Now()

	var changes []PriceChange
	err := s.db.SelectContext(ctx, &changes, query, limit)
	queryTime := time.Since(startTime)

	if err != nil {
		log.Printf("Ошибка запроса изменений цен (время: %v): %v", queryTime, err)
		return nil, fmt.Errorf("ошибка получения изменений цен: %w", err)
	}

	log.Printf("Запрос изменений цен выполнен за %v", queryTime)

	// Кэшируем с небольшим TTL
	s.cache.SetWithTTL(cacheKey, changes, 3*time.Minute)

	return changes, nil
}

// StockChange представляет значительное изменение остатков
type StockChange struct {
	ProductID     int       `json:"productId" db:"product_id"`
	ProductName   string    `json:"productName" db:"product_name"`
	VendorCode    string    `json:"vendorCode" db:"vendor_code"`
	WarehouseID   int64     `json:"warehouseId" db:"warehouse_id"`
	WarehouseName string    `json:"warehouseName" db:"warehouse_name"`
	OldAmount     int       `json:"oldAmount" db:"old_amount"`
	NewAmount     int       `json:"newAmount" db:"new_amount"`
	ChangeAmount  int       `json:"changeAmount" db:"change_amount"`
	ChangePercent float64   `json:"changePercent" db:"change_percent"`
	Date          time.Time `json:"date" db:"change_date"`
}

// GetRecentStockChanges - оптимизированная версия
func (s *Service) GetRecentStockChanges(ctx context.Context, limit int, forceRefresh bool) ([]StockChange, error) {
	// Короткий таймаут
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	cacheKey := fmt.Sprintf("recent_stock_changes_%d", limit)

	// Проверяем кэш
	if !forceRefresh {
		if cachedData, found := s.cache.Get(cacheKey); found {
			return cachedData.([]StockChange), nil
		}
	}

	// Оптимизированный запрос с оконными функциями
	query := `
        WITH recent_stocks AS (
            SELECT 
                product_id,
                warehouse_id,
                amount,
                recorded_at,
                LAG(amount) OVER (PARTITION BY product_id, warehouse_id ORDER BY recorded_at) AS prev_amount,
                LAG(recorded_at) OVER (PARTITION BY product_id, warehouse_id ORDER BY recorded_at) AS prev_recorded_at
            FROM stocks
            WHERE recorded_at > NOW() - INTERVAL '30 days'
        ),
        significant_changes AS (
            SELECT DISTINCT ON (product_id, warehouse_id)
                product_id,
                warehouse_id,
                prev_amount as old_amount,
                amount as new_amount,
                amount - prev_amount as change_amount,
                CASE 
                    WHEN prev_amount > 0 THEN ((amount - prev_amount)::float / prev_amount) * 100 
                    WHEN prev_amount = 0 AND amount > 0 THEN 100
                    ELSE 0 
                END as change_percent,
                recorded_at as change_date
            FROM recent_stocks
            WHERE prev_amount IS NOT NULL
            AND (
                ABS(amount - prev_amount) >= 10 OR 
                ABS(((amount - prev_amount)::float / NULLIF(prev_amount, 0)) * 100) >= 20
            )
            ORDER BY product_id, warehouse_id, ABS(((amount - prev_amount)::float / NULLIF(prev_amount, 0)) * 100) DESC
        )
        SELECT 
            sc.product_id,
            p.name as product_name,
            p.vendor_code,
            sc.warehouse_id,
            w.name as warehouse_name,
            sc.old_amount,
            sc.new_amount,
            sc.change_amount,
            sc.change_percent,
            sc.change_date
        FROM significant_changes sc
        JOIN products p ON sc.product_id = p.id
        JOIN warehouses w ON sc.warehouse_id = w.id
        ORDER BY ABS(sc.change_percent) DESC, sc.change_date DESC
        LIMIT $1
    `

	startTime := time.Now()

	var changes []StockChange
	err := s.db.SelectContext(ctx, &changes, query, limit)
	queryTime := time.Since(startTime)

	if err != nil {
		log.Printf("Ошибка запроса изменений остатков (время: %v): %v", queryTime, err)
		return nil, fmt.Errorf("ошибка получения изменений остатков: %w", err)
	}

	log.Printf("Запрос изменений остатков выполнен за %v", queryTime)

	// Кэшируем с небольшим TTL
	s.cache.SetWithTTL(cacheKey, changes, 3*time.Minute)

	return changes, nil
}

// PriceHistoryItem представляет точку истории цен
type PriceHistoryItem struct {
	ProductID  int       `json:"productId"`
	Date       time.Time `json:"date"`
	Price      int       `json:"price"`
	Discount   int       `json:"discount"`
	FinalPrice int       `json:"finalPrice"`
}

// GetPriceHistory - оптимизированная версия
func (s *Service) GetPriceHistory(ctx context.Context, productID int, days int) ([]PriceHistoryItem, error) {
	// Короткий таймаут
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	cacheKey := fmt.Sprintf("price_history_%d_%d", productID, days)

	// Проверяем кэш
	if cachedData, found := s.cache.Get(cacheKey); found {
		return cachedData.([]PriceHistoryItem), nil
	}

	// Оптимизированный запрос - группируем по дням для уменьшения количества точек
	endDate := time.Now()
	startDate := endDate.AddDate(0, 0, -days)

	query := `
        WITH daily_prices AS (
            SELECT DISTINCT ON (DATE_TRUNC('day', recorded_at))
                product_id, price, discount, final_price, recorded_at
            FROM prices
            WHERE product_id = $1
            AND recorded_at BETWEEN $2 AND $3
            ORDER BY DATE_TRUNC('day', recorded_at), recorded_at DESC
        )
        SELECT 
            product_id, 
            recorded_at as date,
            price,
            discount,
            final_price
        FROM daily_prices
        ORDER BY date
    `

	startTime := time.Now()

	var prices []struct {
		ProductID  int       `db:"product_id"`
		Date       time.Time `db:"date"`
		Price      int       `db:"price"`
		Discount   int       `db:"discount"`
		FinalPrice int       `db:"final_price"`
	}

	err := s.db.SelectContext(ctx, &prices, query, productID, startDate, endDate)
	queryTime := time.Since(startTime)

	if err != nil {
		log.Printf("Ошибка запроса истории цен (время: %v): %v", queryTime, err)
		return nil, fmt.Errorf("ошибка получения истории цен: %w", err)
	}

	log.Printf("Запрос истории цен для product_id=%d выполнен за %v", productID, queryTime)

	history := make([]PriceHistoryItem, len(prices))
	for i, price := range prices {
		history[i] = PriceHistoryItem{
			ProductID:  price.ProductID,
			Date:       price.Date,
			Price:      price.Price,
			Discount:   price.Discount,
			FinalPrice: price.FinalPrice,
		}
	}

	// Если история пустая, добавляем фиктивную точку для отображения на графике
	if len(history) == 0 {
		// Получаем последнюю известную цену
		var lastPrice struct {
			Price      int `db:"price"`
			Discount   int `db:"discount"`
			FinalPrice int `db:"final_price"`
		}

		lastPriceQuery := `
            SELECT price, discount, final_price
            FROM prices
            WHERE product_id = $1
            ORDER BY recorded_at DESC
            LIMIT 1
        `

		err := s.db.GetContext(ctx, &lastPrice, lastPriceQuery, productID)
		if err == nil {
			// Создаем две точки - начало и конец периода
			history = append(history, PriceHistoryItem{
				ProductID:  productID,
				Date:       startDate,
				Price:      lastPrice.Price,
				Discount:   lastPrice.Discount,
				FinalPrice: lastPrice.FinalPrice,
			}, PriceHistoryItem{
				ProductID:  productID,
				Date:       endDate,
				Price:      lastPrice.Price,
				Discount:   lastPrice.Discount,
				FinalPrice: lastPrice.FinalPrice,
			})
		}
	}

	// Кэшируем с небольшим TTL
	s.cache.SetWithTTL(cacheKey, history, 5*time.Minute)

	return history, nil
}

// StockHistoryItem представляет точку истории остатков
type StockHistoryItem struct {
	ProductID   int       `json:"productId"`
	WarehouseID int64     `json:"warehouseId"`
	Date        time.Time `json:"date"`
	Amount      int       `json:"amount"`
}

// GetStockHistory - оптимизированная версия
func (s *Service) GetStockHistory(ctx context.Context, productID int, warehouseID int64, days int) ([]StockHistoryItem, error) {
	// Короткий таймаут
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	cacheKey := fmt.Sprintf("stock_history_%d_%d_%d", productID, warehouseID, days)

	// Проверяем кэш
	if cachedData, found := s.cache.Get(cacheKey); found {
		return cachedData.([]StockHistoryItem), nil
	}

	endDate := time.Now()
	startDate := endDate.AddDate(0, 0, -days)

	// Оптимизированный запрос - группируем по дням
	query := `
        WITH daily_stocks AS (
            SELECT DISTINCT ON (DATE_TRUNC('day', recorded_at))
                product_id, warehouse_id, amount, recorded_at
            FROM stocks
            WHERE product_id = $1
            AND warehouse_id = $2
            AND recorded_at BETWEEN $3 AND $4
            ORDER BY DATE_TRUNC('day', recorded_at), recorded_at DESC
        )
        SELECT 
            product_id, 
            warehouse_id,
            recorded_at as date,
            amount
        FROM daily_stocks
        ORDER BY date
    `

	startTime := time.Now()

	var stocks []struct {
		ProductID   int       `db:"product_id"`
		WarehouseID int64     `db:"warehouse_id"`
		Date        time.Time `db:"date"`
		Amount      int       `db:"amount"`
	}

	err := s.db.SelectContext(ctx, &stocks, query, productID, warehouseID, startDate, endDate)
	queryTime := time.Since(startTime)

	if err != nil {
		log.Printf("Ошибка запроса истории остатков (время: %v): %v", queryTime, err)
		return nil, fmt.Errorf("ошибка получения истории остатков: %w", err)
	}

	log.Printf("Запрос истории остатков для product_id=%d, warehouse_id=%d выполнен за %v",
		productID, warehouseID, queryTime)

	history := make([]StockHistoryItem, len(stocks))
	for i, stock := range stocks {
		history[i] = StockHistoryItem{
			ProductID:   stock.ProductID,
			WarehouseID: stock.WarehouseID,
			Date:        stock.Date,
			Amount:      stock.Amount,
		}
	}

	// Если история пустая, добавляем фиктивную точку для отображения на графике
	if len(history) == 0 {
		// Получаем последний известный остаток
		var lastStock struct {
			Amount int `db:"amount"`
		}

		lastStockQuery := `
            SELECT amount
            FROM stocks
            WHERE product_id = $1 AND warehouse_id = $2
            ORDER BY recorded_at DESC
            LIMIT 1
        `

		err := s.db.GetContext(ctx, &lastStock, lastStockQuery, productID, warehouseID)
		if err == nil {
			// Создаем две точки - начало и конец периода
			history = append(history, StockHistoryItem{
				ProductID:   productID,
				WarehouseID: warehouseID,
				Date:        startDate,
				Amount:      lastStock.Amount,
			}, StockHistoryItem{
				ProductID:   productID,
				WarehouseID: warehouseID,
				Date:        endDate,
				Amount:      lastStock.Amount,
			})
		}
	}

	// Кэшируем с небольшим TTL
	s.cache.SetWithTTL(cacheKey, history, 5*time.Minute)

	return history, nil
}

// PaginatedPriceChanges содержит список изменений цен с информацией для пагинации
type PaginatedPriceChanges struct {
	Items      []PriceChange `json:"items"`
	NextCursor string        `json:"nextCursor,omitempty"`
	HasMore    bool          `json:"hasMore"`
	TotalCount int           `json:"totalCount"`
}

func (s *Service) GetPriceChangesWithCursor(ctx context.Context, limit int, cursor string, forceRefresh bool) (*PaginatedPriceChanges, error) {
	emptyFilter := PriceChangeFilter{}

	return s.GetPriceChangesWithCursorAndFilter(ctx, limit, cursor, forceRefresh, emptyFilter)
}

// GetPriceChangesWithCursorAndFilter возвращает изменения цен с пагинацией и фильтрацией
// ИСПРАВЛЕННАЯ ВЕРСИЯ с корректной привязкой параметров
func (s *Service) GetPriceChangesWithCursorAndFilter(
	ctx context.Context,
	limit int,
	cursor string,
	forceRefresh bool,
	filter PriceChangeFilter,
) (*PaginatedPriceChanges, error) {
	// Короткий таймаут
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	if limit <= 0 {
		limit = 20
	}
	if limit > 500 {
		limit = 500
	}

	// Формируем ключ кэша с учетом фильтров
	var filterKey string
	if filter.MinChangePercent != nil {
		filterKey += fmt.Sprintf("_min%.1f", *filter.MinChangePercent)
	}
	if filter.MaxChangePercent != nil {
		filterKey += fmt.Sprintf("_max%.1f", *filter.MaxChangePercent)
	}
	if filter.MinChangeAmount != nil {
		filterKey += fmt.Sprintf("_amt%d", *filter.MinChangeAmount)
	}
	if filter.Since != nil {
		filterKey += fmt.Sprintf("_s%s", filter.Since.Format("20060102"))
	}
	if filter.OnlyIncreases != nil && *filter.OnlyIncreases {
		filterKey += "_inc"
	}
	if filter.OnlyDecreases != nil && *filter.OnlyDecreases {
		filterKey += "_dec"
	}

	// Проверяем кэш для первой страницы
	cacheKey := fmt.Sprintf("paginated_price_changes_%d%s", limit, filterKey)
	if !forceRefresh && cursor == "" {
		if cachedData, found := s.cache.Get(cacheKey); found {
			return cachedData.(*PaginatedPriceChanges), nil
		}
	}

	// Разбираем курсор
	var cursorOffset int
	if cursor != "" {
		decodedCursor, err := base64.StdEncoding.DecodeString(cursor)
		if err != nil {
			return nil, fmt.Errorf("недействительный курсор: %w", err)
		}
		cursorOffset, err = strconv.Atoi(string(decodedCursor))
		if err != nil {
			return nil, fmt.Errorf("некорректное значение в курсоре: %w", err)
		}
	}

	// Устанавливаем значения фильтров по умолчанию
	sinceDate := time.Now().AddDate(0, -1, 0) // По умолчанию 1 месяц
	if filter.Since != nil {
		sinceDate = *filter.Since
	}

	// Минимальный процент изменения
	var minChangePercent float64 = 5 // По умолчанию 5%
	if filter.MinChangePercent != nil {
		minChangePercent = *filter.MinChangePercent
	}

	// Минимальное абсолютное изменение
	var minChangeAmount int = 0
	if filter.MinChangeAmount != nil {
		minChangeAmount = *filter.MinChangeAmount
	}

	// Максимальный процент изменения (если указан)
	var hasMaxPercent bool
	var maxChangePercent float64
	if filter.MaxChangePercent != nil {
		hasMaxPercent = true
		maxChangePercent = *filter.MaxChangePercent
	}

	// Направление изменения (только повышения/понижения цен)
	var onlyIncreases, onlyDecreases bool
	if filter.OnlyIncreases != nil && *filter.OnlyIncreases {
		onlyIncreases = true
	}
	if filter.OnlyDecreases != nil && *filter.OnlyDecreases {
		onlyDecreases = true
	}

	// Логируем параметры запроса
	log.Printf("Запрос изменений цен: minPercent=%.1f, minAmount=%d, since=%s, onlyInc=%v, onlyDec=%v",
		minChangePercent, minChangeAmount, sinceDate.Format("2006-01-02"), onlyIncreases, onlyDecreases)

	// Базовый запрос - первая часть CTE
	baseQueryStart := `
        WITH latest_price_snapshots AS (
            SELECT 
                product_id,
                size_id,
                final_price,
                snapshot_time,
                ROW_NUMBER() OVER (PARTITION BY product_id, size_id ORDER BY snapshot_time DESC) as rn
            FROM price_snapshots
            WHERE snapshot_time >= $1
        ),
        top_snapshots AS (
            SELECT 
                product_id,
                size_id,
                final_price as new_price,
                snapshot_time
            FROM latest_price_snapshots
            WHERE rn = 1
        ),
        previous_snapshots AS (
            SELECT DISTINCT ON (lps.product_id, lps.size_id)
                lps.product_id,
                lps.size_id,
                prev.final_price as old_price,
                prev.snapshot_time as prev_time
            FROM latest_price_snapshots lps
            JOIN price_snapshots prev ON 
                lps.product_id = prev.product_id AND 
                lps.size_id = prev.size_id
            WHERE 
                lps.rn = 1 AND
                prev.snapshot_time < lps.snapshot_time
            ORDER BY 
                lps.product_id, 
                lps.size_id, 
                prev.snapshot_time DESC
        ),
    `

	// Динамически строим WHERE условие для фильтрации и параметры
	whereConditions := []string{
		"ABS(ts.new_price - ps.old_price) > 0", // Только реальные изменения
	}

	// Очень важно: начинаем с $2, так как $1 уже используется для sinceDate
	nextParamIndex := 2
	args := []interface{}{sinceDate} // Базовые аргументы запроса, всегда содержит sinceDate

	// Добавляем условие минимального процентного изменения
	if minChangePercent > 0 {
		whereConditions = append(whereConditions,
			fmt.Sprintf("ABS(CASE WHEN ps.old_price > 0 THEN ((ts.new_price - ps.old_price)::float / ps.old_price) * 100 ELSE 0 END) >= $%d", nextParamIndex))
		args = append(args, minChangePercent)
		nextParamIndex++
	}

	// Добавляем условие минимального абсолютного изменения
	if minChangeAmount > 0 {
		whereConditions = append(whereConditions,
			fmt.Sprintf("ABS(ts.new_price - ps.old_price) >= $%d", nextParamIndex))
		args = append(args, minChangeAmount)
		nextParamIndex++
	}

	// Добавляем условие максимального процентного изменения
	if hasMaxPercent {
		whereConditions = append(whereConditions,
			fmt.Sprintf("ABS(CASE WHEN ps.old_price > 0 THEN ((ts.new_price - ps.old_price)::float / ps.old_price) * 100 ELSE 0 END) <= $%d", nextParamIndex))
		args = append(args, maxChangePercent)
		nextParamIndex++
	}

	// Только повышения цен
	if onlyIncreases {
		whereConditions = append(whereConditions, "ts.new_price > ps.old_price")
	}

	// Только понижения цен
	if onlyDecreases {
		whereConditions = append(whereConditions, "ts.new_price < ps.old_price")
	}

	// Соединяем условия с AND
	whereClause := strings.Join(whereConditions, " AND ")

	// Завершаем CTE блок
	baseQueryEnd := fmt.Sprintf(`
        all_changes AS (
            SELECT 
                ts.product_id,
                ts.size_id,
                ps.old_price,
                ts.new_price,
                ts.new_price - ps.old_price as change_amount,
                CASE 
                    WHEN ps.old_price > 0 THEN ((ts.new_price - ps.old_price)::float / ps.old_price) * 100 
                    ELSE 0 
                END as change_percent,
                ABS(CASE 
                    WHEN ps.old_price > 0 THEN ((ts.new_price - ps.old_price)::float / ps.old_price) * 100 
                    ELSE 0 
                END) as abs_change_percent,
                ts.snapshot_time as change_date,
                ROW_NUMBER() OVER (ORDER BY 
                    ABS(CASE WHEN ps.old_price > 0 THEN ((ts.new_price - ps.old_price)::float / ps.old_price) * 100 ELSE 0 END) DESC,
                    ts.snapshot_time DESC
                ) as rn
            FROM top_snapshots ts
            JOIN previous_snapshots ps ON ts.product_id = ps.product_id AND ts.size_id = ps.size_id
            WHERE %s
        ),
        significant_changes AS (
            SELECT DISTINCT ON (product_id)
                product_id,
                old_price,
                new_price,
                change_amount,
                change_percent,
                abs_change_percent,
                change_date,
                rn
            FROM all_changes
            ORDER BY product_id, abs_change_percent DESC
        )
    `, whereClause)

	// Объединяем запрос, добавляя параметры пагинации с правильными индексами
	mainQuery := fmt.Sprintf(`
        SELECT 
            sc.product_id,
            p.name as product_name,
            p.vendor_code,
            sc.old_price,
            sc.new_price,
            sc.change_amount,
            sc.change_percent,
            sc.change_date
        FROM significant_changes sc
        JOIN products p ON sc.product_id = p.id
        WHERE sc.rn > $%d
        ORDER BY sc.abs_change_percent DESC, sc.change_date DESC
        LIMIT $%d
    `, nextParamIndex, nextParamIndex+1)

	// Собираем полный запрос
	query := baseQueryStart + baseQueryEnd + mainQuery

	// Добавляем параметры пагинации
	args = append(args, cursorOffset, limit+1)

	// Логируем полный запрос для отладки
	log.Printf("SQL запрос с %d параметрами, условиями: %s", len(args), whereClause)

	// Запускаем запрос с логированием времени
	startTime := time.Now()
	var changes []PriceChange
	err := s.db.SelectContext(ctx, &changes, query, args...)
	queryTime := time.Since(startTime)

	if err != nil {
		log.Printf("Ошибка запроса изменений цен (время: %v): %v", queryTime, err)
		return nil, fmt.Errorf("ошибка получения изменений цен: %w", err)
	}

	log.Printf("Запрос изменений цен выполнен за %v (limit=%d, найдено: %d)",
		queryTime, limit, len(changes))

	// Определяем, есть ли еще записи
	hasMore := false
	if len(changes) > limit {
		hasMore = true
		changes = changes[:limit]
	}

	// Формируем курсор для следующей страницы
	var nextCursor string
	if hasMore && len(changes) > 0 {
		nextOffset := cursorOffset + limit
		nextCursor = base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(nextOffset)))
	}

	// Создаем результат
	result := &PaginatedPriceChanges{
		Items:      changes,
		NextCursor: nextCursor,
		HasMore:    hasMore,
		TotalCount: cursorOffset + len(changes),
	}

	// Кэшируем первую страницу с коротким TTL
	if cursor == "" {
		s.cache.SetWithTTL(cacheKey, result, 3*time.Minute)
	}

	return result, nil
}

// StockChangeFilter содержит параметры фильтрации для изменений остатков
type StockChangeFilter struct {
	WarehouseID      *int64     // Фильтр по конкретному складу (если указан)
	MinChangeAmount  *int       // Минимальное абсолютное изменение количества
	MinChangePercent *float64   // Минимальное изменение в процентах
	Since            *time.Time // Начальная дата периода (если не указана, используется 30 дней)
}

// PaginatedStockChanges содержит список изменений остатков с информацией для пагинации
type PaginatedStockChanges struct {
	Items      []StockChange `json:"items"`
	NextCursor string        `json:"nextCursor,omitempty"`
	HasMore    bool          `json:"hasMore"`
	TotalCount int           `json:"totalCount"` // Приблизительное общее количество записей
}

// GetStockChangesWithCursor returns stock changes with pagination and filtering
// Fixed version without PostgreSQL syntax errors
func (s *Service) GetStockChangesWithCursor(ctx context.Context, limit int, cursor string, filter StockChangeFilter, forceRefresh bool) (*PaginatedStockChanges, error) {
	// Устанавливаем короткий таймаут для ускорения ответа
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	if limit <= 0 {
		limit = 20 // Значение по умолчанию
	}
	if limit > 500 {
		limit = 500
	}

	// Разбираем курсор
	var cursorOffset int
	if cursor != "" {
		decodedCursor, err := base64.StdEncoding.DecodeString(cursor)
		if err != nil {
			return nil, fmt.Errorf("недействительный курсор: %w", err)
		}
		cursorOffset, err = strconv.Atoi(string(decodedCursor))
		if err != nil {
			return nil, fmt.Errorf("некорректное значение в курсоре: %w", err)
		}
	}

	// Устанавливаем значения фильтров ТОЧНО как указаны в запросе
	sinceDate := time.Now().AddDate(0, 0, -7) // По умолчанию смотрим за неделю
	if filter.Since != nil {
		sinceDate = *filter.Since
	}

	// Важно! Используем ИМЕННО те значения, которые переданы в запросе
	// Если параметр не указан (nil), только тогда используем значение по умолчанию
	var minChangePercent float64 = 0
	if filter.MinChangePercent != nil {
		minChangePercent = *filter.MinChangePercent
	}

	var minChangeAmt int = 0
	if filter.MinChangeAmount != nil {
		minChangeAmt = *filter.MinChangeAmount
	}

	// Логируем реально используемые значения
	log.Printf("Фильтрация с параметрами: warehouse=%v, minChangePercent=%.2f, minChangeAmt=%d, sinceDate=%v",
		filter.WarehouseID, minChangePercent, minChangeAmt, sinceDate.Format("2006-01-02"))

	// Исправленный запрос - используем DISTINCT ON вместо LIMIT BY
	query := `
		WITH latest_snapshots AS (
			SELECT 
				product_id, 
				warehouse_id, 
				amount, 
				snapshot_time,
				ROW_NUMBER() OVER (PARTITION BY product_id, warehouse_id ORDER BY snapshot_time DESC) as rn
			FROM stock_snapshots
			WHERE snapshot_time >= $1
			AND ($2::bigint IS NULL OR warehouse_id = $2)
		),
		top_snapshots AS (
			SELECT 
				product_id, 
				warehouse_id, 
				amount as new_amount, 
				snapshot_time as new_time
			FROM latest_snapshots
			WHERE rn = 1
		),
		previous_snapshots AS (
			SELECT DISTINCT ON (ls.product_id, ls.warehouse_id)
				ls.product_id, 
				ls.warehouse_id, 
				prev.amount as old_amount, 
				prev.snapshot_time as old_time
			FROM latest_snapshots ls
			JOIN stock_snapshots prev ON 
				ls.product_id = prev.product_id AND 
				ls.warehouse_id = prev.warehouse_id
			WHERE 
				ls.rn = 1 AND
				prev.snapshot_time < ls.snapshot_time
			ORDER BY 
				ls.product_id, 
				ls.warehouse_id, 
				prev.snapshot_time DESC
		),
		filtered_changes AS (
			SELECT 
				ts.product_id,
				ts.warehouse_id,
				ps.old_amount,
				ts.new_amount,
				ts.new_amount - ps.old_amount AS change_amount,
				CASE 
					WHEN ps.old_amount > 0 THEN ((ts.new_amount - ps.old_amount)::float / ps.old_amount) * 100 
					WHEN ps.old_amount = 0 AND ts.new_amount > 0 THEN 100
					ELSE 0 
				END AS change_percent,
				ABS(CASE 
					WHEN ps.old_amount > 0 THEN ((ts.new_amount - ps.old_amount)::float / ps.old_amount) * 100 
					WHEN ps.old_amount = 0 AND ts.new_amount > 0 THEN 100
					ELSE 0 
				END) AS abs_change_percent,
				ts.new_time AS change_date
			FROM top_snapshots ts
			JOIN previous_snapshots ps ON ts.product_id = ps.product_id AND ts.warehouse_id = ps.warehouse_id
			WHERE 
				-- Явно применяем фильтры минимальных изменений
				(CASE WHEN $4 > 0 THEN 
					ABS(CASE 
						WHEN ps.old_amount > 0 THEN ((ts.new_amount - ps.old_amount)::float / ps.old_amount) * 100 
						WHEN ps.old_amount = 0 AND ts.new_amount > 0 THEN 100
						ELSE 0 
					END) >= $4
				ELSE TRUE END)
				AND
				(CASE WHEN $3 > 0 THEN
					ABS(ts.new_amount - ps.old_amount) >= $3
				ELSE TRUE END)
		)
		SELECT 
			fc.product_id,
			p.name as product_name,
			p.vendor_code,
			fc.warehouse_id,
			w.name as warehouse_name,
			fc.old_amount,
			fc.new_amount,
			fc.change_amount,
			fc.change_percent,
			fc.change_date
		FROM filtered_changes fc
		JOIN products p ON fc.product_id = p.id
		JOIN warehouses w ON fc.warehouse_id = w.id
		ORDER BY fc.abs_change_percent DESC, fc.change_date DESC, fc.product_id, fc.warehouse_id
		LIMIT $6 OFFSET $5
	`

	startTime := time.Now()
	var changes []StockChange

	// Выполняем запрос с явно указанными параметрами
	err := s.db.SelectContext(
		ctx,
		&changes,
		query,
		sinceDate,
		filter.WarehouseID,
		minChangeAmt,     // Явно используем переданное значение
		minChangePercent, // Явно используем переданное значение
		cursorOffset,
		limit+1,
	)

	queryTime := time.Since(startTime)
	if err != nil {
		log.Printf("Ошибка запроса изменений остатков: %v (время запроса: %v)", err, queryTime)
		return nil, fmt.Errorf("ошибка получения изменений остатков: %w", err)
	}

	// Логируем результаты запроса
	log.Printf("Запрос изменений остатков выполнен за %v. Найдено записей: %d/%d при minChangePercent=%.2f",
		queryTime, len(changes), limit, minChangePercent)

	// Определяем, есть ли еще записи
	hasMore := false
	if len(changes) > limit {
		hasMore = true
		changes = changes[:limit]
	}

	// Создаем курсор для следующей страницы
	var nextCursor string
	if hasMore {
		nextOffset := cursorOffset + limit
		nextCursor = base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(nextOffset)))
	}

	// Проверка корректности сортировки - логируем первые несколько результатов
	if len(changes) > 0 {
		log.Printf("Топ изменения: 1) %s: %.2f%%, 2) %s: %.2f%% (сортировка по убыванию)",
			changes[0].ProductName, changes[0].ChangePercent,
			changes[min(len(changes)-1, 1)].ProductName, changes[min(len(changes)-1, 1)].ChangePercent)
	}

	// Создаем результат
	result := &PaginatedStockChanges{
		Items:      changes,
		NextCursor: nextCursor,
		HasMore:    hasMore,
		TotalCount: cursorOffset + len(changes),
	}

	return result, nil
}

// Вспомогательная функция для безопасного получения минимального значения
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
