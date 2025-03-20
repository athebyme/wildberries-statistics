package stats

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"math"
	"sort"
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

// GetOverviewStats возвращает общую статистику
func (s *Service) GetOverviewStats(ctx context.Context, forceRefresh bool) (*OverviewStats, error) {
	cacheKey := "overview_stats"

	// Проверяем кэш, если не требуется принудительное обновление
	if !forceRefresh {
		if cachedStats, found := s.cache.Get(cacheKey); found {
			return cachedStats.(*OverviewStats), nil
		}
	}

	// Создаем канал для результатов асинхронных запросов
	type result struct {
		value interface{}
		err   error
	}

	// Лимит товаров с низким остатком
	lowStockThreshold := 10

	// Запускаем несколько запросов параллельно
	productCountCh := make(chan result, 1)
	warehouseCountCh := make(chan result, 1)
	pricingDataCh := make(chan result, 1)
	stockDataCh := make(chan result, 1)
	lowStockCountCh := make(chan result, 1)

	// Получаем количество продуктов
	go func() {
		count, err := db.GetProductCount(ctx, s.db)
		productCountCh <- result{value: count, err: err}
	}()

	// Получаем количество складов
	go func() {
		warehouses, err := db.GetAllWarehouses(ctx, s.db)
		warehouseCountCh <- result{value: len(warehouses), err: err}
	}()

	// Получаем данные о ценах
	go func() {
		var pricingData struct {
			AvgPrice          float64
			MostExpensiveItem string
			CheapestItem      string
		}

		err := s.db.GetContext(ctx, &pricingData, `
			WITH price_data AS (
				SELECT 
					p.product_id,
					p.final_price,
					pr.name,
					ROW_NUMBER() OVER (PARTITION BY p.product_id ORDER BY p.recorded_at DESC) as rn
				FROM 
					prices p
					JOIN products pr ON p.product_id = pr.id
			)
			SELECT 
				COALESCE(AVG(final_price), 0) as avg_price,
				COALESCE(MAX(name) FILTER (WHERE final_price = max_price.max_fp), '') as most_expensive_item,
				COALESCE(MAX(name) FILTER (WHERE final_price = min_price.min_fp), '') as cheapest_item
			FROM 
				price_data pd,
				(SELECT MIN(final_price) as min_fp FROM price_data WHERE rn = 1) min_price,
				(SELECT MAX(final_price) as max_fp FROM price_data WHERE rn = 1) max_price
			WHERE 
				pd.rn = 1
		`)

		pricingDataCh <- result{value: pricingData, err: err}
	}()

	// Получаем данные об остатках
	go func() {
		var stockData struct {
			TotalStock int
			AvgStock   float64
		}

		err := s.db.GetContext(ctx, &stockData, `
			WITH stock_data AS (
				SELECT 
					product_id,
					amount,
					ROW_NUMBER() OVER (PARTITION BY product_id, warehouse_id ORDER BY recorded_at DESC) as rn
				FROM 
					stocks
			)
			SELECT 
				COALESCE(SUM(amount), 0) as total_stock,
				COALESCE(AVG(amount), 0) as avg_stock
			FROM 
				stock_data
			WHERE 
				rn = 1
		`)

		stockDataCh <- result{value: stockData, err: err}
	}()

	// Получаем количество товаров с низким остатком
	go func() {
		var lowStockCount int

		err := s.db.GetContext(ctx, &lowStockCount, `
			WITH latest_stocks AS (
				SELECT 
					product_id,
					SUM(amount) as total_amount
				FROM (
					SELECT 
						product_id,
						warehouse_id,
						amount,
						ROW_NUMBER() OVER (PARTITION BY product_id, warehouse_id ORDER BY recorded_at DESC) as rn
					FROM 
						stocks
				) s
				WHERE 
					s.rn = 1
				GROUP BY 
					product_id
			)
			SELECT 
				COUNT(*) 
			FROM 
				latest_stocks
			WHERE 
				total_amount < $1
		`, lowStockThreshold)

		lowStockCountCh <- result{value: lowStockCount, err: err}
	}()

	// Получаем результаты всех запросов
	productCountRes := <-productCountCh
	warehouseCountRes := <-warehouseCountCh
	pricingDataRes := <-pricingDataCh
	stockDataRes := <-stockDataCh
	lowStockCountRes := <-lowStockCountCh

	// Проверяем наличие ошибок
	for _, res := range []result{productCountRes, warehouseCountRes, pricingDataRes, stockDataRes, lowStockCountRes} {
		if res.err != nil {
			return nil, fmt.Errorf("ошибка получения статистики: %w", res.err)
		}
	}

	// Извлекаем результаты
	productCount := productCountRes.value.(int)
	warehouseCount := warehouseCountRes.value.(int)

	pricingData := pricingDataRes.value.(struct {
		AvgPrice          float64
		MostExpensiveItem string
		CheapestItem      string
	})

	stockData := stockDataRes.value.(struct {
		TotalStock int
		AvgStock   float64
	})

	lowStockCount := lowStockCountRes.value.(int)

	// Создаем результат
	stats := &OverviewStats{
		TotalProducts:     productCount,
		TotalWarehouses:   warehouseCount,
		AvgPrice:          pricingData.AvgPrice,
		TotalStock:        stockData.TotalStock,
		AvgStock:          stockData.AvgStock,
		LastUpdated:       time.Now(),
		MostExpensiveItem: pricingData.MostExpensiveItem,
		CheapestItem:      pricingData.CheapestItem,
		LowStockItems:     lowStockCount,
		LowStockThreshold: lowStockThreshold,
	}

	// Сохраняем в кэш
	s.cache.Set(cacheKey, stats)

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

// GetTopProducts возвращает список топовых продуктов по изменению цены или остатков
func (s *Service) GetTopProducts(ctx context.Context, limit int, forceRefresh bool) ([]ProductStats, error) {
	cacheKey := fmt.Sprintf("top_products_%d", limit)

	// Проверяем кэш, если не требуется принудительное обновление
	if !forceRefresh {
		if cachedStats, found := s.cache.Get(cacheKey); found {
			return cachedStats.([]ProductStats), nil
		}
	}

	now := time.Now()
	sevenDaysAgo := now.AddDate(0, 0, -7)

	// Получаем все продукты
	products, err := db.GetAllProducts(ctx, s.db)
	if err != nil {
		return nil, fmt.Errorf("ошибка получения списка продуктов: %w", err)
	}

	if len(products) == 0 {
		return []ProductStats{}, nil
	}

	// Формируем список ID продуктов
	productIDs := make([]int, len(products))
	productsMap := make(map[int]models.ProductRecord)
	for i, product := range products {
		productIDs[i] = product.ID
		productsMap[product.ID] = product
	}

	// Получаем последние цены для всех продуктов
	latestPrices, err := db.GetLatestPricesForProducts(ctx, s.db, productIDs)
	if err != nil {
		return nil, fmt.Errorf("ошибка получения последних цен: %w", err)
	}

	// Получаем данные по складам
	warehouses, err := db.GetAllWarehouses(ctx, s.db)
	if err != nil {
		return nil, fmt.Errorf("ошибка получения списка складов: %w", err)
	}

	warehouseIDs := make([]int64, len(warehouses))
	for i, warehouse := range warehouses {
		warehouseIDs[i] = warehouse.ID
	}

	// Получаем последние остатки для всех продуктов на всех складах
	latestStocks, err := db.GetLatestStocksForProducts(ctx, s.db, productIDs, warehouseIDs)
	if err != nil {
		return nil, fmt.Errorf("ошибка получения последних остатков: %w", err)
	}

	// Получаем исторические данные о ценах
	historicalPrices, err := db.GetBatchPricesForProducts(ctx, s.db, productIDs, sevenDaysAgo, now)
	if err != nil {
		return nil, fmt.Errorf("ошибка получения исторических данных о ценах: %w", err)
	}

	// Получаем исторические данные об остатках
	historicalStocks, err := db.GetBatchStocksForProducts(ctx, s.db, productIDs, warehouseIDs, sevenDaysAgo, now)
	if err != nil {
		return nil, fmt.Errorf("ошибка получения исторических данных об остатках: %w", err)
	}

	// Создаем результирующие данные
	var result []ProductStats

	for _, product := range products {
		price, hasPriceData := latestPrices[product.ID]

		// Пропускаем продукты без ценовых данных
		if !hasPriceData {
			continue
		}

		// Рассчитываем общее количество остатков
		totalStock := 0
		stocksForProduct, hasStockData := latestStocks[product.ID]
		if hasStockData {
			for _, stock := range stocksForProduct {
				totalStock += stock.Amount
			}
		}

		// Рассчитываем изменение цены
		priceChange := 0.0
		if prices, ok := historicalPrices[product.ID]; ok && len(prices) > 1 {
			oldestPrice := prices[0].Price
			if oldestPrice > 0 {
				priceChange = float64(price.Price-oldestPrice) / float64(oldestPrice) * 100
			}
		}

		// Рассчитываем изменение остатков
		stockChange := 0.0
		oldestTotalStock := 0

		if stocksByWarehouse, ok := historicalStocks[product.ID]; ok {
			// Собираем самые ранние данные об остатках для каждого склада
			for _, stocks := range stocksByWarehouse {
				if len(stocks) > 0 {
					oldestTotalStock += stocks[0].Amount
				}
			}

			if oldestTotalStock > 0 {
				stockChange = float64(totalStock-oldestTotalStock) / float64(oldestTotalStock) * 100
			}
		}

		// Создаем объект ProductStats
		productStats := ProductStats{
			ID:           product.ID,
			NmID:         product.NmID,
			VendorCode:   product.VendorCode,
			Name:         product.Name,
			CurrentPrice: price.Price,
			PriceChange:  priceChange,
			TotalStock:   totalStock,
			StockChange:  stockChange,
			LastUpdated:  price.RecordedAt.Format("02.01.2006 15:04"),
		}

		result = append(result, productStats)
	}

	// Сортируем по абсолютному значению изменения цены (от большего к меньшему)
	sortProductsByAbsChange(result)

	// Ограничиваем количество результатов
	if len(result) > limit {
		result = result[:limit]
	}

	// Сохраняем в кэш
	s.cache.Set(cacheKey, result)

	return result, nil
}

// PriceChange представляет значительное изменение цены
type PriceChange struct {
	ProductID     int       `json:"productId"`
	ProductName   string    `json:"productName"`
	VendorCode    string    `json:"vendorCode"`
	OldPrice      int       `json:"oldPrice"`
	NewPrice      int       `json:"newPrice"`
	ChangeAmount  int       `json:"changeAmount"`
	ChangePercent float64   `json:"changePercent"`
	Date          time.Time `json:"date"`
}

// GetRecentPriceChanges возвращает недавние значительные изменения цен
func (s *Service) GetRecentPriceChanges(ctx context.Context, limit int, forceRefresh bool) ([]PriceChange, error) {
	cacheKey := fmt.Sprintf("recent_price_changes_%d", limit)

	// Проверяем кэш, если не требуется принудительное обновление
	if !forceRefresh {
		if cachedData, found := s.cache.Get(cacheKey); found {
			return cachedData.([]PriceChange), nil
		}
	}

	// Запрашиваем данные о значительных изменениях цен за последние 30 дней
	// с порогом изменения в 5%
	var changes []PriceChange

	query := `
		WITH price_changes AS (
			SELECT 
				p1.product_id,
				pr.name as product_name,
				pr.vendor_code,
				p1.price as old_price,
				p2.price as new_price,
				p2.price - p1.price as change_amount,
				CASE WHEN p1.price > 0 THEN ((p2.price - p1.price)::float / p1.price) * 100 ELSE 0 END as change_percent,
				p2.recorded_at as change_date,
				ROW_NUMBER() OVER (PARTITION BY p1.product_id ORDER BY p2.recorded_at DESC) as rn
			FROM 
				prices p1
				JOIN prices p2 ON p1.product_id = p2.product_id
				JOIN products pr ON p1.product_id = pr.id
			WHERE 
				p2.recorded_at > p1.recorded_at
				AND p2.recorded_at > NOW() - INTERVAL '30 days'
				AND p1.recorded_at = (
					SELECT MAX(recorded_at) 
					FROM prices p3 
					WHERE p3.product_id = p1.product_id AND p3.recorded_at < p2.recorded_at
				)
				AND ABS(((p2.price - p1.price)::float / NULLIF(p1.price, 0)) * 100) >= 5
		)
		SELECT 
			product_id,
			product_name,
			vendor_code,
			old_price,
			new_price,
			change_amount,
			change_percent,
			change_date
		FROM 
			price_changes
		WHERE 
			rn = 1
		ORDER BY 
			ABS(change_percent) DESC, change_date DESC
		LIMIT $1
	`

	err := s.db.SelectContext(ctx, &changes, query, limit)
	if err != nil {
		return nil, fmt.Errorf("ошибка получения изменений цен: %w", err)
	}

	// Сохраняем в кэш
	s.cache.Set(cacheKey, changes)

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

// GetRecentStockChanges возвращает недавние значительные изменения остатков
func (s *Service) GetRecentStockChanges(ctx context.Context, limit int, forceRefresh bool) ([]StockChange, error) {
	cacheKey := fmt.Sprintf("recent_stock_changes_%d", limit)

	// Проверяем кэш, если не требуется принудительное обновление
	if !forceRefresh {
		if cachedData, found := s.cache.Get(cacheKey); found {
			return cachedData.([]StockChange), nil
		}
	}

	// Запрашиваем данные о значительных изменениях остатков за последние 30 дней
	// с порогом изменения в 20%
	var changes []StockChange

	query := `
		WITH stock_changes AS (
			SELECT 
				s1.product_id,
				pr.name as product_name,
				pr.vendor_code,
				s1.warehouse_id,
				w.name as warehouse_name,
				s1.amount as old_amount,
				s2.amount as new_amount,
				s2.amount - s1.amount as change_amount,
				CASE WHEN s1.amount > 0 THEN ((s2.amount - s1.amount)::float / s1.amount) * 100 ELSE 0 END as change_percent,
				s2.recorded_at as change_date,
				ROW_NUMBER() OVER (PARTITION BY s1.product_id, s1.warehouse_id ORDER BY s2.recorded_at DESC) as rn
			FROM 
				stocks s1
				JOIN stocks s2 ON s1.product_id = s2.product_id AND s1.warehouse_id = s2.warehouse_id
				JOIN products pr ON s1.product_id = pr.id
				JOIN warehouses w ON s1.warehouse_id = w.id
			WHERE 
				s2.recorded_at > s1.recorded_at
				AND s2.recorded_at > NOW() - INTERVAL '30 days'
				AND s1.recorded_at = (
					SELECT MAX(recorded_at) 
					FROM stocks s3 
					WHERE s3.product_id = s1.product_id 
					AND s3.warehouse_id = s1.warehouse_id 
					AND s3.recorded_at < s2.recorded_at
				)
				AND (ABS(((s2.amount - s1.amount)::float / NULLIF(s1.amount, 0)) * 100) >= 20 OR ABS(s2.amount - s1.amount) >= 10)
		)
		SELECT 
			product_id,
			product_name,
			vendor_code,
			warehouse_id,
			warehouse_name,
			old_amount,
			new_amount,
			change_amount,
			change_percent,
			change_date
		FROM 
			stock_changes
		WHERE 
			rn = 1
		ORDER BY 
			ABS(change_amount) DESC, change_date DESC
		LIMIT $1
	`

	err := s.db.SelectContext(ctx, &changes, query, limit)
	if err != nil {
		return nil, fmt.Errorf("ошибка получения изменений остатков: %w", err)
	}

	// Сохраняем в кэш
	s.cache.Set(cacheKey, changes)

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

// GetPriceHistory возвращает историю цен для указанного продукта
func (s *Service) GetPriceHistory(ctx context.Context, productID int, days int) ([]PriceHistoryItem, error) {
	cacheKey := fmt.Sprintf("price_history_%d_%d", productID, days)

	// Проверяем кэш
	if cachedData, found := s.cache.Get(cacheKey); found {
		return cachedData.([]PriceHistoryItem), nil
	}

	endDate := time.Now()
	startDate := endDate.AddDate(0, 0, -days)

	prices, err := db.GetPricesForPeriod(ctx, s.db, productID, startDate, endDate)
	if err != nil {
		return nil, fmt.Errorf("ошибка получения истории цен: %w", err)
	}

	var history []PriceHistoryItem
	for _, price := range prices {
		history = append(history, PriceHistoryItem{
			ProductID:  productID,
			Date:       price.RecordedAt,
			Price:      price.Price,
			Discount:   price.Discount,
			FinalPrice: price.FinalPrice,
		})
	}

	// Сохраняем в кэш
	s.cache.Set(cacheKey, history)

	return history, nil
}

// StockHistoryItem представляет точку истории остатков
type StockHistoryItem struct {
	ProductID   int       `json:"productId"`
	WarehouseID int64     `json:"warehouseId"`
	Date        time.Time `json:"date"`
	Amount      int       `json:"amount"`
}

// GetStockHistory возвращает историю остатков для указанного продукта
func (s *Service) GetStockHistory(ctx context.Context, productID int, warehouseID int64, days int) ([]StockHistoryItem, error) {
	cacheKey := fmt.Sprintf("stock_history_%d_%d_%d", productID, warehouseID, days)

	// Проверяем кэш
	if cachedData, found := s.cache.Get(cacheKey); found {
		return cachedData.([]StockHistoryItem), nil
	}

	endDate := time.Now()
	startDate := endDate.AddDate(0, 0, -days)

	stocks, err := db.GetStocksForPeriod(ctx, s.db, productID, warehouseID, startDate, endDate)
	if err != nil {
		return nil, fmt.Errorf("ошибка получения истории остатков: %w", err)
	}

	var history []StockHistoryItem
	for _, stock := range stocks {
		history = append(history, StockHistoryItem{
			ProductID:   productID,
			WarehouseID: warehouseID,
			Date:        stock.RecordedAt,
			Amount:      stock.Amount,
		})
	}

	// Сохраняем в кэш
	s.cache.Set(cacheKey, history)

	return history, nil
}

// PaginatedPriceChanges содержит список изменений цен с информацией для пагинации
type PaginatedPriceChanges struct {
	Items      []PriceChange `json:"items"`
	NextCursor string        `json:"nextCursor,omitempty"`
	HasMore    bool          `json:"hasMore"`
}

// GetPriceChangesWithCursor возвращает недавние изменения цен с поддержкой пагинации по курсору
func (s *Service) GetPriceChangesWithCursor(ctx context.Context, limit int, cursor string, forceRefresh bool) (*PaginatedPriceChanges, error) {
	if limit <= 0 {
		limit = 20 // Значение по умолчанию
	}

	// Если кэширование включено и курсор пустой (первая страница), пробуем получить из кэша
	if !forceRefresh && cursor == "" {
		cacheKey := fmt.Sprintf("paginated_price_changes_%d", limit)
		if cachedData, found := s.cache.Get(cacheKey); found {
			return cachedData.(*PaginatedPriceChanges), nil
		}
	}

	// Разбираем курсор для получения последних значений
	var lastPercent float64
	var lastDate time.Time

	if cursor != "" {
		decodedCursor, err := base64.StdEncoding.DecodeString(cursor)
		if err != nil {
			return nil, fmt.Errorf("недействительный курсор: %w", err)
		}

		parts := strings.Split(string(decodedCursor), "|")
		if len(parts) != 2 {
			return nil, fmt.Errorf("недопустимый формат курсора")
		}

		// Парсим значения курсора
		var err1, err2 error
		lastPercent, err1 = strconv.ParseFloat(parts[0], 64)
		lastDateStr := parts[1]
		lastDate, err2 = time.Parse(time.RFC3339, lastDateStr)

		if err1 != nil || err2 != nil {
			return nil, fmt.Errorf("недопустимые значения курсора")
		}
	}

	// Формируем базовый запрос
	baseQuery := `
		WITH price_changes AS (
			SELECT 
				p1.product_id,
				pr.name as product_name,
				pr.vendor_code,
				p1.price as old_price,
				p2.price as new_price,
				p2.price - p1.price as change_amount,
				CASE WHEN p1.price > 0 THEN ((p2.price - p1.price)::float / p1.price) * 100 ELSE 0 END as change_percent,
				p2.recorded_at as change_date,
				ROW_NUMBER() OVER (PARTITION BY p1.product_id ORDER BY p2.recorded_at DESC) as rn
			FROM 
				prices p1
				JOIN prices p2 ON p1.product_id = p2.product_id
				JOIN products pr ON p1.product_id = pr.id
			WHERE 
				p2.recorded_at > p1.recorded_at
				AND p2.recorded_at > NOW() - INTERVAL '30 days'
				AND p1.recorded_at = (
					SELECT MAX(recorded_at) 
					FROM prices p3 
					WHERE p3.product_id = p1.product_id AND p3.recorded_at < p2.recorded_at
				)
				AND ABS(((p2.price - p1.price)::float / NULLIF(p1.price, 0)) * 100) >= 5
		)
	`

	// Добавляем условия пагинации
	var query string
	var args []interface{}

	if cursor == "" {
		// Первая страница
		query = baseQuery + `
			SELECT 
				product_id,
				product_name,
				vendor_code,
				old_price,
				new_price,
				change_amount,
				change_percent,
				change_date
			FROM 
				price_changes
			WHERE 
				rn = 1
			ORDER BY 
				ABS(change_percent) DESC, change_date DESC
			LIMIT $1
		`
		args = []interface{}{limit + 1} // +1 чтобы определить, есть ли еще записи
	} else {
		// Последующие страницы
		query = baseQuery + `
			SELECT 
				product_id,
				product_name,
				vendor_code,
				old_price,
				new_price,
				change_amount,
				change_percent,
				change_date
			FROM 
				price_changes
			WHERE 
				rn = 1
				AND (
					ABS(change_percent) < $1
					OR (ABS(change_percent) = $1 AND change_date < $2)
				)
			ORDER BY 
				ABS(change_percent) DESC, change_date DESC
			LIMIT $3
		`
		args = []interface{}{math.Abs(lastPercent), lastDate, limit + 1}
	}

	// Выполняем запрос
	var changes []PriceChange
	err := s.db.SelectContext(ctx, &changes, query, args...)
	if err != nil {
		return nil, fmt.Errorf("ошибка получения изменений цен: %w", err)
	}

	// Определяем, есть ли еще записи
	hasMore := false
	if len(changes) > limit {
		hasMore = true
		changes = changes[:limit] // Убираем дополнительную запись
	}

	// Создаем курсор для следующей страницы, если есть еще записи
	var nextCursor string
	if hasMore && len(changes) > 0 {
		lastItem := changes[len(changes)-1]
		cursorStr := fmt.Sprintf("%.2f|%s", math.Abs(lastItem.ChangePercent), lastItem.Date.Format(time.RFC3339))
		nextCursor = base64.StdEncoding.EncodeToString([]byte(cursorStr))
	}

	// Создаем результат с пагинацией
	result := &PaginatedPriceChanges{
		Items:      changes,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}

	// Кэшируем только первую страницу
	if cursor == "" {
		s.cache.Set(fmt.Sprintf("paginated_price_changes_%d", limit), result)
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

// GetStockChangesWithCursor возвращает недавние изменения остатков с поддержкой пагинации по курсору
func (s *Service) GetStockChangesWithCursor(ctx context.Context, limit int, cursor string, filter StockChangeFilter, forceRefresh bool) (*PaginatedStockChanges, error) {
	// Проверяем и устанавливаем тайм-аут для контекста, если он еще не установлен
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	if limit <= 0 {
		limit = 20 // Значение по умолчанию
	}

	// Формируем ключ кэша на основе параметров фильтрации
	cacheKey := fmt.Sprintf("paginated_stock_changes_%d_%v", limit, filter)

	// Проверяем кэш, если не требуется принудительное обновление и это первая страница
	if !forceRefresh && cursor == "" {
		if cachedData, found := s.cache.Get(cacheKey); found {
			return cachedData.(*PaginatedStockChanges), nil
		}
	}

	// Разбираем курсор для получения последних значений
	var lastChangeAmount int
	var lastDate time.Time

	if cursor != "" {
		decodedCursor, err := base64.StdEncoding.DecodeString(cursor)
		if err != nil {
			return nil, fmt.Errorf("недействительный курсор: %w", err)
		}

		parts := strings.Split(string(decodedCursor), "|")
		if len(parts) != 2 {
			return nil, fmt.Errorf("недопустимый формат курсора")
		}

		// Парсим значения курсора
		var err1, err2 error
		lastChangeAmount, err1 = strconv.Atoi(parts[0])
		lastDateStr := parts[1]
		lastDate, err2 = time.Parse(time.RFC3339, lastDateStr)

		if err1 != nil || err2 != nil {
			return nil, fmt.Errorf("недопустимые значения курсора")
		}
	}

	// Формируем условия фильтрации
	var filterConditions []string
	var filterArgs []interface{}
	argIndex := 1

	// Начальная дата для фильтрации
	sinceDate := time.Now().AddDate(0, 0, -30) // По умолчанию 30 дней
	if filter.Since != nil {
		sinceDate = *filter.Since
	}
	filterConditions = append(filterConditions, fmt.Sprintf("s2.recorded_at > $%d", argIndex))
	filterArgs = append(filterArgs, sinceDate)
	argIndex++

	// Фильтрация по складу
	if filter.WarehouseID != nil {
		filterConditions = append(filterConditions, fmt.Sprintf("s2.warehouse_id = $%d", argIndex))
		filterArgs = append(filterArgs, *filter.WarehouseID)
		argIndex++
	}

	// Фильтрация по минимальному абсолютному изменению
	minChangeAmt := 10 // По умолчанию минимальное изменение 10 единиц
	if filter.MinChangeAmount != nil {
		minChangeAmt = *filter.MinChangeAmount
	}

	// Фильтрация по минимальному процентному изменению
	minChangePercent := 20.0 // По умолчанию минимальное изменение 20%
	if filter.MinChangePercent != nil {
		minChangePercent = *filter.MinChangePercent
	}

	// Объединяем условия фильтрации
	changeFilterCondition := fmt.Sprintf(
		"(ABS(((s2.amount - s1.amount)::float / NULLIF(s1.amount, 0)) * 100) >= $%d OR ABS(s2.amount - s1.amount) >= $%d)",
		argIndex, argIndex+1,
	)
	filterConditions = append(filterConditions, changeFilterCondition)
	filterArgs = append(filterArgs, minChangePercent, minChangeAmt)
	argIndex += 2

	// Объединяем все условия фильтрации
	filterWhereClause := strings.Join(filterConditions, " AND ")

	// Формируем оптимизированный запрос с использованием LATERAL JOIN
	// вместо вложенного подзапроса для лучшей производительности
	baseQuery := fmt.Sprintf(`
		WITH latest_records AS (
			SELECT 
				s2.product_id,
				s2.warehouse_id,
				s2.amount as new_amount,
				s2.recorded_at as new_date,
				s1.amount as old_amount,
				s1.recorded_at as old_date,
				s2.amount - s1.amount as change_amount,
				CASE WHEN s1.amount > 0 THEN ((s2.amount - s1.amount)::float / s1.amount) * 100 ELSE 0 END as change_percent,
				pr.name as product_name,
				pr.vendor_code,
				w.name as warehouse_name
			FROM 
				stocks s2
				JOIN LATERAL (
					SELECT s1.amount, s1.recorded_at, s1.warehouse_id
					FROM stocks s1
					WHERE s1.product_id = s2.product_id
					AND s1.warehouse_id = s2.warehouse_id
					AND s1.recorded_at < s2.recorded_at
					ORDER BY s1.recorded_at DESC
					LIMIT 1
				) s1 ON true
				JOIN products pr ON s2.product_id = pr.id
				JOIN warehouses w ON s2.warehouse_id = w.id
			WHERE 
				s2.recorded_at > s1.recorded_at
				AND %s
		),
		stock_changes AS (
			SELECT 
				product_id,
				product_name,
				vendor_code,
				warehouse_id,
				warehouse_name,
				old_amount,
				new_amount,
				change_amount,
				change_percent,
				new_date as change_date,
				ROW_NUMBER() OVER (PARTITION BY product_id, warehouse_id ORDER BY new_date DESC) as rn
			FROM 
				latest_records
		)
	`, filterWhereClause)

	// Аргументы для запроса с курсором
	var args []interface{}
	args = append(args, filterArgs...)

	// Добавляем условия пагинации
	var query string

	if cursor == "" {
		// Первая страница
		query = baseQuery + `
			SELECT 
				product_id,
				product_name,
				vendor_code,
				warehouse_id,
				warehouse_name,
				old_amount,
				new_amount,
				change_amount,
				change_percent,
				change_date
			FROM 
				stock_changes
			WHERE 
				rn = 1
			ORDER BY 
				ABS(change_amount) DESC, change_date DESC
			LIMIT $` + strconv.Itoa(argIndex)
		args = append(args, limit+1) // +1 чтобы определить, есть ли еще записи
	} else {
		// Последующие страницы
		query = baseQuery + `
			SELECT 
				product_id,
				product_name,
				vendor_code,
				warehouse_id,
				warehouse_name,
				old_amount,
				new_amount,
				change_amount,
				change_percent,
				change_date
			FROM 
				stock_changes
			WHERE 
				rn = 1
				AND (
					ABS(change_amount) < $` + strconv.Itoa(argIndex) + `
					OR (ABS(change_amount) = $` + strconv.Itoa(argIndex) + ` AND change_date < $` + strconv.Itoa(argIndex+1) + `)
				)
			ORDER BY 
				ABS(change_amount) DESC, change_date DESC
			LIMIT $` + strconv.Itoa(argIndex+2)
		args = append(args, math.Abs(float64(lastChangeAmount)), lastDate, limit+1)
	}

	// Запрос для получения приблизительного количества записей
	countQuery := `
		SELECT COUNT(*) FROM (
			SELECT 1
			FROM stocks s2
			JOIN LATERAL (
				SELECT s1.amount, s1.recorded_at
				FROM stocks s1
				WHERE s1.product_id = s2.product_id
				AND s1.warehouse_id = s2.warehouse_id
				AND s1.recorded_at < s2.recorded_at
				ORDER BY s1.recorded_at DESC
				LIMIT 1
			) s1 ON true
			WHERE 
				s2.recorded_at > s1.recorded_at
				AND ` + filterWhereClause + `
			LIMIT 1000
		) as count_estimate
	`

	// Создаем группу вопросов для выполнения запросов параллельно
	var wg sync.WaitGroup
	var changes []StockChange
	var count int
	var queryErr, countErr error

	wg.Add(2)

	// Запускаем основной запрос
	go func() {
		defer wg.Done()
		queryErr = s.db.SelectContext(ctx, &changes, query, args...)
	}()

	// Запускаем запрос счетчика (только для первой страницы)
	go func() {
		defer wg.Done()
		if cursor == "" {
			countErr = s.db.GetContext(ctx, &count, countQuery, filterArgs...)
		} else {
			// Для последующих страниц не делаем запрос счетчика
			count = 0
		}
	}()

	// Ждем завершения обоих запросов
	wg.Wait()

	// Проверяем ошибки
	if queryErr != nil {
		return nil, fmt.Errorf("ошибка получения изменений остатков: %w", queryErr)
	}

	if countErr != nil && cursor == "" {
		log.Printf("Ошибка получения общего количества: %v", countErr)
		// Продолжаем работу, даже если счетчик не получен
	}

	// Определяем, есть ли еще записи
	hasMore := false
	if len(changes) > limit {
		hasMore = true
		changes = changes[:limit] // Убираем дополнительную запись
	}

	// Создаем курсор для следующей страницы, если есть еще записи
	var nextCursor string
	if hasMore && len(changes) > 0 {
		lastItem := changes[len(changes)-1]
		cursorStr := fmt.Sprintf("%d|%s", int(math.Abs(float64(lastItem.ChangeAmount))), lastItem.Date.Format(time.RFC3339))
		nextCursor = base64.StdEncoding.EncodeToString([]byte(cursorStr))
	}

	// Устанавливаем расчетное общее количество
	totalCount := count
	if cursor != "" {
		// Для последующих страниц используем приблизительное значение
		if cachedData, found := s.cache.Get(cacheKey); found {
			totalCount = cachedData.(*PaginatedStockChanges).TotalCount
		}
	}

	// Создаем результат с пагинацией
	result := &PaginatedStockChanges{
		Items:      changes,
		NextCursor: nextCursor,
		HasMore:    hasMore,
		TotalCount: totalCount,
	}

	// Кэшируем только первую страницу
	if cursor == "" {
		s.cache.SetWithTTL(cacheKey, result, 5*time.Minute) // Сокращаем время кэширования для свежих данных
	}

	return result, nil
}

// Вспомогательные функции

// Сортирует продукты по абсолютному изменению цены (от большего к меньшему)
func sortProductsByAbsChange(products []ProductStats) {
	sort.Slice(products, func(i, j int) bool {
		absPriceChangeI := absFloat(products[i].PriceChange)
		absPriceChangeJ := absFloat(products[j].PriceChange)

		// Сначала сортируем по изменению цены
		if absPriceChangeI != absPriceChangeJ {
			return absPriceChangeI > absPriceChangeJ
		}

		// Затем по изменению остатков
		absStockChangeI := absFloat(products[i].StockChange)
		absStockChangeJ := absFloat(products[j].StockChange)

		return absStockChangeI > absStockChangeJ
	})
}

// Возвращает абсолютное значение float64
func absFloat(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
