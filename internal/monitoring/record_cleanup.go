package monitoring

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/lib/pq"
	"log"
	"sort"
	"strings"
	"time"
	"wbmonitoring/monitoring/internal/models"

	"github.com/jmoiron/sqlx"
)

type RecordCleanupService struct {
	db                *sqlx.DB
	cleanupInterval   time.Duration
	retentionInterval time.Duration
	hourlyDataKeeper  *HourlyDataKeeper
	workerPoolSize    int
}

func NewRecordCleanupService(
	db *sqlx.DB,
	cleanupInterval,
	retentionInterval time.Duration,
	hourlyDataKeeper *HourlyDataKeeper,
	workerPoolSize int,
) *RecordCleanupService {
	if workerPoolSize <= 0 {
		workerPoolSize = 5
	}

	return &RecordCleanupService{
		db:                db,
		cleanupInterval:   cleanupInterval,
		retentionInterval: retentionInterval,
		hourlyDataKeeper:  hourlyDataKeeper,
		workerPoolSize:    workerPoolSize,
	}
}

func (s *RecordCleanupService) RunCleanupProcess(ctx context.Context) {
	ticker := time.NewTicker(s.cleanupInterval)
	defer ticker.Stop()

	log.Println("Record cleanup process started")

	log.Printf("Retention interval set to %v", s.retentionInterval)

	if err := s.CleanupRecords(ctx); err != nil {
		log.Printf("Error during initial records cleanup: %v", err)
	}

	for {
		select {
		case <-ticker.C:
			if err := s.CleanupRecords(ctx); err != nil {
				log.Printf("Error during records cleanup: %v", err)
			}
		case <-ctx.Done():
			log.Println("Record cleanup process stopped")
			return
		}
	}
}

func (s *RecordCleanupService) CleanupRecords(ctx context.Context) error {
	log.Println("Starting records cleanup process")

	//  таймаут для избежания зависаний
	ctx, cancel := context.WithTimeout(ctx, 2*time.Hour)
	defer cancel()

	now := time.Now()
	yesterday := now.AddDate(0, 0, -1)
	startOfYesterday := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 0, 0, 0, 0, now.Location())
	endOfYesterday := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 23, 59, 59, 999999999, now.Location())

	//  список продуктов
	products, err := s.getAllProducts(ctx)
	if err != nil {
		return fmt.Errorf("getting products for cleanup: %w", err)
	}

	log.Printf("Found %d products to process for cleanup", len(products))

	// Ограничиваем размер пакета для обработки
	batchSize := 200

	// Группируем товары для пакетной обработки
	var batches [][]models.ProductRecord

	for i := 0; i < len(products); i += batchSize {
		end := i + batchSize
		if end > len(products) {
			end = len(products)
		}

		batches = append(batches, products[i:end])
	}

	log.Printf("Divided products into %d batches for processing", len(batches))

	// Получаем все склады для дальнейшего использования
	warehouses, err := s.getAllWarehouses(ctx)
	if err != nil {
		log.Printf("Error getting warehouses: %v", err)
		// Продолжаем выполнение с пустым списком складов
		warehouses = []models.Warehouse{}
	}

	for i, batch := range batches {
		log.Printf("Processing batch %d of %d (%d products)", i+1, len(batches), len(batch))

		// Обработка пакета с таймаутом
		batchCtx, batchCancel := context.WithTimeout(ctx, 100*time.Minute)

		// Пакетная обработка снапшотов цен
		priceErr := s.processBatchPrices(batchCtx, batch, startOfYesterday, endOfYesterday)
		if priceErr != nil {
			log.Printf("Error processing price snapshots for batch %d: %v", i+1, priceErr)
		}

		// Пакетная обработка снапшотов остатков
		stockErr := s.processBatchStocks(batchCtx, batch, warehouses, startOfYesterday, endOfYesterday)
		if stockErr != nil {
			log.Printf("Error processing stock snapshots for batch %d: %v", i+1, stockErr)
		}

		batchCancel()

		// Пауза между пакетами для снижения нагрузки на БД
		select {
		case <-time.After(1 * time.Second):
			// Продолжаем после паузы
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Вызываем очистку старых данных
	yesterday = now.AddDate(0, 0, -1)
	retentionDate := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(),
		0, 0, 0, 0, now.Location())

	log.Printf("Will delete records older than %s", retentionDate.Format("2006-01-02 15:04:05"))

	var priceCount int
	countQuery := "SELECT COUNT(*) FROM prices WHERE recorded_at < $1"
	if err := s.db.GetContext(ctx, &priceCount, countQuery, retentionDate); err == nil {
		log.Printf("Found %d price records to delete", priceCount)
	}

	var stockCount int
	countQuery = "SELECT COUNT(*) FROM stocks WHERE recorded_at < $1"
	if err := s.db.GetContext(ctx, &stockCount, countQuery, retentionDate); err == nil {
		log.Printf("Found %d stock records to delete", stockCount)
	}

	if err := s.deleteOldRecords(ctx, retentionDate); err != nil {
		return fmt.Errorf("deleting old records: %w", err)
	}

	log.Println("Records cleanup process completed")
	return nil
}

// Пакетная обработка снапшотов цен
func (s *RecordCleanupService) processBatchPrices(ctx context.Context, products []models.ProductRecord, startDate, endDate time.Time) error {
	// Подготовка ID продуктов для пакетного запроса
	productIDs := make([]int, len(products))
	for i, p := range products {
		productIDs[i] = p.ID
	}

	// Получаем все размеры для продуктов
	sizes, err := s.getProductSizesBatch(ctx, productIDs)
	if err != nil {
		return fmt.Errorf("getting product sizes: %w", err)
	}

	// Если нет размеров для обработки
	if len(sizes) == 0 {
		return nil
	}

	// Получаем последние известные цены перед периодом
	lastKnownPrices, err := s.getLastKnownPricesBatch(ctx, productIDs, sizes, startDate)
	if err != nil {
		return fmt.Errorf("getting last known prices: %w", err)
	}

	// Получаем все записи о ценах за период
	periodPrices, err := s.getPriceRecordsForPeriodBatch(ctx, productIDs, sizes, startDate, endDate)
	if err != nil {
		return fmt.Errorf("getting price records for period: %w", err)
	}

	// Создаем часовые снапшоты (используем пакетную вставку)
	if err := s.ensureHourlyPriceSnapshotsBatch(ctx, lastKnownPrices, periodPrices, startDate, endDate); err != nil {
		return fmt.Errorf("ensuring hourly price snapshots: %w", err)
	}

	return nil
}

func (s *RecordCleanupService) processBatchStocks(ctx context.Context, products []models.ProductRecord, warehouses []models.Warehouse, startDate, endDate time.Time) error {
	// Подготовка ID продуктов и складов для пакетного запроса
	productIDs := make([]int, len(products))
	for i, p := range products {
		productIDs[i] = p.ID
	}

	warehouseIDs := make([]int64, len(warehouses))
	for i, w := range warehouses {
		warehouseIDs[i] = w.ID
	}

	// Если нет складов, пропускаем обработку
	if len(warehouseIDs) == 0 {
		return nil
	}

	// Получаем последние известные остатки перед периодом
	lastKnownStocks, err := s.getLastKnownStocksBatch(ctx, productIDs, warehouseIDs, startDate)
	if err != nil {
		return fmt.Errorf("getting last known stocks: %w", err)
	}

	// Получаем все записи об остатках за период
	periodStocks, err := s.getStockRecordsForPeriodBatch(ctx, productIDs, warehouseIDs, startDate, endDate)
	if err != nil {
		return fmt.Errorf("getting stock records for period: %w", err)
	}

	// Создаем часовые снапшоты (используем пакетную вставку)
	if err := s.ensureHourlyStockSnapshotsBatch(ctx, lastKnownStocks, periodStocks, startDate, endDate); err != nil {
		return fmt.Errorf("ensuring hourly stock snapshots: %w", err)
	}

	return nil
}

// Обеспечение часовых снапшотов остатков с пакетной вставкой
func (s *RecordCleanupService) ensureHourlyStockSnapshotsBatch(
	ctx context.Context,
	lastKnownStocks map[int]map[int64]models.StockRecord,
	periodStocks map[int]map[int64][]models.StockRecord,
	startDate,
	endDate time.Time,
) error {
	// Подготовка данных для пакетной вставки снапшотов
	type SnapshotKey struct {
		ProductID   int
		WarehouseID int64
		Hour        time.Time
	}

	// Кеш уже существующих снапшотов
	existingSnapshots := make(map[SnapshotKey]bool)

	// Получаем существующие снапшоты за период
	query := `
        SELECT product_id, warehouse_id, snapshot_time 
        FROM stock_snapshots
        WHERE snapshot_time BETWEEN $1 AND $2
        AND product_id = ANY($3)
    `

	rows, err := s.db.QueryxContext(ctx, query, startDate, endDate, pq.Array(getKeysFromStockMap(lastKnownStocks)))
	if err != nil {
		return fmt.Errorf("querying existing snapshots: %w", err)
	}

	for rows.Next() {
		var productID int
		var warehouseID int64
		var snapshotTime time.Time

		if err := rows.Scan(&productID, &warehouseID, &snapshotTime); err != nil {
			rows.Close()
			return fmt.Errorf("scanning snapshot row: %w", err)
		}

		key := SnapshotKey{
			ProductID:   productID,
			WarehouseID: warehouseID,
			Hour:        snapshotTime,
		}

		existingSnapshots[key] = true
	}

	rows.Close()

	// Подготовка данных для вставки
	var snapshotsToInsert []models.StockSnapshot

	// Проходим по всем комбинациям product_id-warehouse_id
	for productID, warehouseMap := range periodStocks {
		for warehouseID, stocks := range warehouseMap {
			if len(stocks) == 0 {
				continue
			}

			// Добавляем последний известный остаток перед периодом (если есть)
			var allStocks []models.StockRecord

			if lastStock, ok := lastKnownStocks[productID][warehouseID]; ok {
				allStocks = append(allStocks, lastStock)
			}

			allStocks = append(allStocks, stocks...)

			// Если остатков нет, пропускаем
			if len(allStocks) == 0 {
				continue
			}

			// Создаем снапшоты для каждого часа
			current := startDate

			for current.Before(endDate) || current.Equal(endDate) {
				hourStart := time.Date(current.Year(), current.Month(), current.Day(), current.Hour(), 0, 0, 0, current.Location())
				hourEnd := hourStart.Add(time.Hour)

				// Проверяем, существует ли уже снапшот
				key := SnapshotKey{
					ProductID:   productID,
					WarehouseID: warehouseID,
					Hour:        hourStart,
				}

				if existingSnapshots[key] {
					current = hourEnd
					continue
				}

				// Поиск последнего остатка перед концом часа
				var latestStock *models.StockRecord

				for i := len(allStocks) - 1; i >= 0; i-- {
					if !allStocks[i].RecordedAt.After(hourEnd) &&
						(latestStock == nil || allStocks[i].RecordedAt.After(latestStock.RecordedAt)) {
						stockCopy := allStocks[i]
						latestStock = &stockCopy
						break
					}
				}

				if latestStock != nil {
					snapshotsToInsert = append(snapshotsToInsert, models.StockSnapshot{
						ProductID:   productID,
						WarehouseID: warehouseID,
						Amount:      latestStock.Amount,

						SnapshotTime: time.Date(latestStock.RecordedAt.Year(), latestStock.RecordedAt.Month(),
							latestStock.RecordedAt.Day(), latestStock.RecordedAt.Hour(), 0, 0, 0,
							latestStock.RecordedAt.Location()),
					})
				}

				current = hourEnd
			}
		}
	}

	// Пакетная вставка снапшотов (если есть что вставлять)
	if len(snapshotsToInsert) > 0 {
		return s.batchInsertStockSnapshots(ctx, snapshotsToInsert)
	}

	return nil
}

func (s *RecordCleanupService) getProductSizesBatch(ctx context.Context, productIDs []int) (map[int][]int, error) {
	if len(productIDs) == 0 {
		return make(map[int][]int), nil
	}

	query := `
        SELECT DISTINCT product_id, size_id FROM prices 
        WHERE product_id = ANY($1)
    `

	rows, err := s.db.QueryxContext(ctx, query, pq.Array(productIDs))
	if err != nil {
		return nil, fmt.Errorf("querying product sizes: %w", err)
	}
	defer rows.Close()

	result := make(map[int][]int)

	for rows.Next() {
		var productID, sizeID int
		if err := rows.Scan(&productID, &sizeID); err != nil {
			return nil, fmt.Errorf("scanning product size row: %w", err)
		}

		result[productID] = append(result[productID], sizeID)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating product sizes rows: %w", err)
	}

	return result, nil
}

// Получение последних известных цен перед периодом пакетным запросом
func (s *RecordCleanupService) getLastKnownPricesBatch(ctx context.Context, productIDs []int, sizes map[int][]int, date time.Time) (map[int]map[int]models.PriceRecord, error) {
	if len(productIDs) == 0 {
		return make(map[int]map[int]models.PriceRecord), nil
	}

	// Создаем комбинации productID-sizeID для запроса
	type ProductSizePair struct {
		ProductID int
		SizeID    int
	}

	var pairs []ProductSizePair

	for productID, sizeIDs := range sizes {
		for _, sizeID := range sizeIDs {
			pairs = append(pairs, ProductSizePair{ProductID: productID, SizeID: sizeID})
		}
	}

	// Если пар нет, возвращаем пустой результат
	if len(pairs) == 0 {
		return make(map[int]map[int]models.PriceRecord), nil
	}

	// Получаем последние цены перед датой для всех пар одним запросом
	query := `
        WITH ranked_prices AS (
            SELECT 
                p.*,
                ROW_NUMBER() OVER (PARTITION BY product_id, size_id ORDER BY recorded_at DESC) as rn
            FROM prices p
            WHERE (product_id, size_id) IN (SELECT unnest($1::int[]), unnest($2::int[]))
            AND recorded_at < $3
        )
        SELECT * FROM ranked_prices WHERE rn = 1
    `

	// Подготавливаем массивы для запроса
	productIDArr := make([]int, len(pairs))
	sizeIDArr := make([]int, len(pairs))

	for i, pair := range pairs {
		productIDArr[i] = pair.ProductID
		sizeIDArr[i] = pair.SizeID
	}

	rows, err := s.db.QueryxContext(ctx, query, pq.Array(productIDArr), pq.Array(sizeIDArr), date)
	if err != nil {
		return nil, fmt.Errorf("querying last known prices: %w", err)
	}
	defer rows.Close()

	result := make(map[int]map[int]models.PriceRecord)

	for rows.Next() {
		var price models.PriceRecord
		var _ int

		// Создаем временную структуру для сканирования
		dest := map[string]interface{}{}

		if err := rows.MapScan(dest); err != nil {
			return nil, fmt.Errorf("scanning price row: %w", err)
		}

		// Заполняем структуру PriceRecord
		if productID, ok := dest["product_id"].(int64); ok {
			price.ProductID = int(productID)
		}

		if sizeID, ok := dest["size_id"].(int64); ok {
			price.SizeID = int(sizeID)
		}

		if priceVal, ok := dest["price"].(int64); ok {
			price.Price = int(priceVal)
		}

		if discount, ok := dest["discount"].(int64); ok {
			price.Discount = int(discount)
		}

		if clubDiscount, ok := dest["club_discount"].(int64); ok {
			price.ClubDiscount = int(clubDiscount)
		}

		if finalPrice, ok := dest["final_price"].(int64); ok {
			price.FinalPrice = int(finalPrice)
		}

		if clubFinalPrice, ok := dest["club_final_price"].(int64); ok {
			price.ClubFinalPrice = int(clubFinalPrice)
		}

		if currency, ok := dest["currency_iso_code"].(string); ok {
			price.CurrencyIsoCode = currency
		}

		if techSize, ok := dest["tech_size_name"].(string); ok {
			price.TechSizeName = techSize
		}

		if editable, ok := dest["editable_size_price"].(bool); ok {
			price.EditableSizePrice = editable
		}

		if recordedAt, ok := dest["recorded_at"].(time.Time); ok {
			price.RecordedAt = recordedAt
		}

		// Добавляем в результат
		if _, ok := result[price.ProductID]; !ok {
			result[price.ProductID] = make(map[int]models.PriceRecord)
		}

		result[price.ProductID][price.SizeID] = price
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating price rows: %w", err)
	}

	return result, nil
}

// Получение записей о ценах за период пакетным запросом
func (s *RecordCleanupService) getPriceRecordsForPeriodBatch(
	ctx context.Context,
	productIDs []int,
	sizes map[int][]int,
	startTime,
	endTime time.Time,
) (map[int]map[int][]models.PriceRecord, error) {
	if len(productIDs) == 0 {
		return make(map[int]map[int][]models.PriceRecord), nil
	}

	// Получаем все записи о ценах за период одним запросом
	query := `
        SELECT * FROM prices
        WHERE product_id = ANY($1)
        AND recorded_at BETWEEN $2 AND $3
        ORDER BY product_id, size_id, recorded_at ASC
    `

	rows, err := s.db.QueryxContext(ctx, query, pq.Array(productIDs), startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("querying price records: %w", err)
	}
	defer rows.Close()

	result := make(map[int]map[int][]models.PriceRecord)

	for rows.Next() {
		var price models.PriceRecord

		// Создаем временную структуру для сканирования
		dest := map[string]interface{}{}

		if err := rows.MapScan(dest); err != nil {
			return nil, fmt.Errorf("scanning price row: %w", err)
		}

		// Заполняем структуру PriceRecord
		if productID, ok := dest["product_id"].(int64); ok {
			price.ProductID = int(productID)
		}

		if sizeID, ok := dest["size_id"].(int64); ok {
			price.SizeID = int(sizeID)
		}

		if priceVal, ok := dest["price"].(int64); ok {
			price.Price = int(priceVal)
		}

		if discount, ok := dest["discount"].(int64); ok {
			price.Discount = int(discount)
		}

		if clubDiscount, ok := dest["club_discount"].(int64); ok {
			price.ClubDiscount = int(clubDiscount)
		}

		if finalPrice, ok := dest["final_price"].(int64); ok {
			price.FinalPrice = int(finalPrice)
		}

		if clubFinalPrice, ok := dest["club_final_price"].(int64); ok {
			price.ClubFinalPrice = int(clubFinalPrice)
		}

		if currency, ok := dest["currency_iso_code"].(string); ok {
			price.CurrencyIsoCode = currency
		}

		if techSize, ok := dest["tech_size_name"].(string); ok {
			price.TechSizeName = techSize
		}

		if editable, ok := dest["editable_size_price"].(bool); ok {
			price.EditableSizePrice = editable
		}

		if recordedAt, ok := dest["recorded_at"].(time.Time); ok {
			price.RecordedAt = recordedAt
		}

		// Добавляем в результат
		if _, ok := result[price.ProductID]; !ok {
			result[price.ProductID] = make(map[int][]models.PriceRecord)
		}

		result[price.ProductID][price.SizeID] = append(result[price.ProductID][price.SizeID], price)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating price rows: %w", err)
	}

	return result, nil
}

// Обеспечение часовых снапшотов цен с пакетной вставкой
func (s *RecordCleanupService) ensureHourlyPriceSnapshotsBatch(
	ctx context.Context,
	lastKnownPrices map[int]map[int]models.PriceRecord,
	periodPrices map[int]map[int][]models.PriceRecord,
	startDate,
	endDate time.Time,
) error {
	// Подготовка данных для пакетной вставки снапшотов
	type SnapshotKey struct {
		ProductID int
		SizeID    int
		Hour      time.Time
	}

	// Кеш уже существующих снапшотов
	existingSnapshots := make(map[SnapshotKey]bool)

	// Получаем существующие снапшоты за период
	query := `
        SELECT product_id, size_id, snapshot_time 
        FROM price_snapshots
        WHERE snapshot_time BETWEEN $1 AND $2
        AND product_id = ANY($3)
    `

	rows, err := s.db.QueryxContext(ctx, query, startDate, endDate, pq.Array(getKeysFromPriceMap(lastKnownPrices)))
	if err != nil {
		return fmt.Errorf("querying existing snapshots: %w", err)
	}

	for rows.Next() {
		var productID, sizeID int
		var snapshotTime time.Time

		if err := rows.Scan(&productID, &sizeID, &snapshotTime); err != nil {
			rows.Close()
			return fmt.Errorf("scanning snapshot row: %w", err)
		}

		key := SnapshotKey{
			ProductID: productID,
			SizeID:    sizeID,
			Hour:      snapshotTime,
		}

		existingSnapshots[key] = true
	}

	rows.Close()

	// Подготовка данных для вставки
	var snapshotsToInsert []models.PriceSnapshot

	// Проходим по всем комбинациям product_id-size_id
	for productID, sizeMap := range periodPrices {
		for sizeID, prices := range sizeMap {
			if len(prices) == 0 {
				continue
			}

			// Добавляем последнюю известную цену перед периодом (если есть)
			var allPrices []models.PriceRecord

			if lastPrice, ok := lastKnownPrices[productID][sizeID]; ok {
				allPrices = append(allPrices, lastPrice)
			}

			allPrices = append(allPrices, prices...)

			// Если цен нет, пропускаем
			if len(allPrices) == 0 {
				continue
			}

			// Создаем снапшоты для каждого часа
			current := startDate

			for current.Before(endDate) || current.Equal(endDate) {
				hourStart := time.Date(current.Year(), current.Month(), current.Day(), current.Hour(), 0, 0, 0, current.Location())
				hourEnd := hourStart.Add(time.Hour)

				// Проверяем, существует ли уже снапшот
				key := SnapshotKey{
					ProductID: productID,
					SizeID:    sizeID,
					Hour:      hourStart,
				}

				if existingSnapshots[key] {
					current = hourEnd
					continue
				}

				// Поиск последней цены перед концом часа
				var latestPrice *models.PriceRecord

				for i := len(allPrices) - 1; i >= 0; i-- {
					if !allPrices[i].RecordedAt.After(hourEnd) &&
						(latestPrice == nil || allPrices[i].RecordedAt.After(latestPrice.RecordedAt)) {
						priceCopy := allPrices[i]
						latestPrice = &priceCopy
						break
					}
				}

				if latestPrice != nil {
					snapshotsToInsert = append(snapshotsToInsert, models.PriceSnapshot{
						ProductID:         productID,
						SizeID:            sizeID,
						Price:             latestPrice.Price,
						Discount:          latestPrice.Discount,
						ClubDiscount:      latestPrice.ClubDiscount,
						FinalPrice:        latestPrice.FinalPrice,
						ClubFinalPrice:    latestPrice.ClubFinalPrice,
						CurrencyIsoCode:   latestPrice.CurrencyIsoCode,
						TechSizeName:      latestPrice.TechSizeName,
						EditableSizePrice: latestPrice.EditableSizePrice,

						SnapshotTime: time.Date(latestPrice.RecordedAt.Year(), latestPrice.RecordedAt.Month(),
							latestPrice.RecordedAt.Day(), latestPrice.RecordedAt.Hour(), 0, 0, 0,
							latestPrice.RecordedAt.Location()),
					})
				}

				current = hourEnd
			}
		}
	}

	// Пакетная вставка снапшотов (если есть что вставлять)
	if len(snapshotsToInsert) > 0 {
		return s.batchInsertPriceSnapshots(ctx, snapshotsToInsert)
	}

	return nil
}

// Пакетная вставка снапшотов цен
func (s *RecordCleanupService) batchInsertPriceSnapshots(ctx context.Context, snapshots []models.PriceSnapshot) error {
	if len(snapshots) == 0 {
		return nil
	}

	const maxRetries = 5
	const smallBatchSize = 200 // Значительно меньший размер пакета

	// 1. Сортируем снапшоты по идентификаторам для обеспечения постоянного порядка доступа
	sort.Slice(snapshots, func(i, j int) bool {
		if snapshots[i].ProductID != snapshots[j].ProductID {
			return snapshots[i].ProductID < snapshots[j].ProductID
		}
		if snapshots[i].SizeID != snapshots[j].SizeID {
			return snapshots[i].SizeID < snapshots[j].SizeID
		}
		return snapshots[i].SnapshotTime.Before(snapshots[j].SnapshotTime)
	})

	// 2. Обрабатываем данные очень маленькими пакетами для уменьшения времени транзакций
	var lastError error
	for i := 0; i < len(snapshots); i += smallBatchSize {
		end := i + smallBatchSize
		if end > len(snapshots) {
			end = len(snapshots)
		}

		batch := snapshots[i:end]

		// 3. Механизм повторных попыток с экспоненциальной задержкой
		for attempt := 0; attempt < maxRetries; attempt++ {
			err := s.insertPriceSnapshotBatchSafely(ctx, batch)
			if err == nil {
				// Успешно - переходим к следующему пакету
				lastError = nil
				break
			}

			// Проверяем, был ли это deadlock или другая временная ошибка
			if strings.Contains(err.Error(), "deadlock detected") ||
				strings.Contains(err.Error(), "could not serialize access") {
				// Увеличиваем задержку с каждой попыткой (экспоненциальная задержка)
				delay := time.Duration(50*(1<<attempt)) * time.Millisecond
				log.Printf("Deadlock on price snapshot batch %d/%d, attempt %d, retrying after %v",
					i/smallBatchSize+1, (len(snapshots)-1)/smallBatchSize+1, attempt+1, delay)
				time.Sleep(delay)
				lastError = err
				continue
			}

			// Другая ошибка - немедленно возвращаем
			return fmt.Errorf("inserting price snapshot batch %d: %w", i/smallBatchSize+1, err)
		}

		// Если все попытки не удались
		if lastError != nil {
			return fmt.Errorf("failed to insert price snapshot batch %d after %d attempts: %w",
				i/smallBatchSize+1, maxRetries, lastError)
		}
	}

	log.Printf("Successfully processed %d price snapshots in %d small batches",
		len(snapshots), (len(snapshots)-1)/smallBatchSize+1)
	return nil
}

// Безопасная вставка одного мини-пакета снапшотов цен
func (s *RecordCleanupService) insertPriceSnapshotBatchSafely(ctx context.Context, batch []models.PriceSnapshot) error {
	// Используем более низкий уровень изоляции
	tx, err := s.db.BeginTxx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
		ReadOnly:  false,
	})
	if err != nil {
		return fmt.Errorf("starting transaction: %w", err)
	}
	defer tx.Rollback()

	// Прямая вставка в основную таблицу - без временных таблиц для уменьшения нагрузки
	stmt, err := tx.PrepareContext(ctx, `
    INSERT INTO price_snapshots (
        product_id, size_id, price, discount, club_discount, 
        final_price, club_final_price, currency_iso_code, 
        tech_size_name, editable_size_price, snapshot_time
    ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
    )
    ON CONFLICT (product_id, size_id, snapshot_time) DO UPDATE SET
        price = EXCLUDED.price,
        discount = EXCLUDED.discount,
        club_discount = EXCLUDED.club_discount,
        final_price = EXCLUDED.final_price,
        club_final_price = EXCLUDED.club_final_price,
        currency_iso_code = EXCLUDED.currency_iso_code,
        tech_size_name = EXCLUDED.tech_size_name,
        editable_size_price = EXCLUDED.editable_size_price
`)
	if err != nil {
		return fmt.Errorf("preparing statement: %w", err)
	}
	defer stmt.Close()

	// Вставляем по одной записи
	for _, snapshot := range batch {
		_, err := stmt.ExecContext(ctx,
			snapshot.ProductID, snapshot.SizeID, snapshot.Price,
			snapshot.Discount, snapshot.ClubDiscount, snapshot.FinalPrice,
			snapshot.ClubFinalPrice, snapshot.CurrencyIsoCode,
			snapshot.TechSizeName, snapshot.EditableSizePrice, snapshot.SnapshotTime)
		if err != nil {
			return fmt.Errorf("executing insert: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	return nil
}

// Получение списка складов для продуктов одним запросом
func (s *RecordCleanupService) getAllWarehouses(ctx context.Context) ([]models.Warehouse, error) {
	var warehouses []models.Warehouse

	query := "SELECT id, name FROM warehouses"

	err := s.db.SelectContext(ctx, &warehouses, query)
	if err != nil {
		return nil, fmt.Errorf("selecting warehouses: %w", err)
	}

	return warehouses, nil
}

// Получение последних известных остатков перед периодом пакетным запросом
func (s *RecordCleanupService) getLastKnownStocksBatch(
	ctx context.Context,
	productIDs []int,
	warehouseIDs []int64,
	date time.Time,
) (map[int]map[int64]models.StockRecord, error) {
	if len(productIDs) == 0 || len(warehouseIDs) == 0 {
		return make(map[int]map[int64]models.StockRecord), nil
	}

	query := `
        WITH ranked_stocks AS (
            SELECT 
                s.*,
                ROW_NUMBER() OVER (PARTITION BY product_id, warehouse_id ORDER BY recorded_at DESC) as rn
            FROM stocks s
            WHERE product_id = ANY($1)
            AND warehouse_id = ANY($2)
            AND recorded_at < $3
        )
        SELECT * FROM ranked_stocks WHERE rn = 1
    `

	rows, err := s.db.QueryxContext(ctx, query, pq.Array(productIDs), pq.Array(warehouseIDs), date)
	if err != nil {
		return nil, fmt.Errorf("querying last known stocks: %w", err)
	}
	defer rows.Close()

	result := make(map[int]map[int64]models.StockRecord)

	for rows.Next() {
		var stock models.StockRecord
		var _ int

		// Создаем временную структуру для сканирования
		dest := map[string]interface{}{}

		if err := rows.MapScan(dest); err != nil {
			return nil, fmt.Errorf("scanning stock row: %w", err)
		}

		// Заполняем структуру StockRecord
		if productID, ok := dest["product_id"].(int64); ok {
			stock.ProductID = int(productID)
		}

		if warehouseID, ok := dest["warehouse_id"].(int64); ok {
			stock.WarehouseID = warehouseID
		}

		if amount, ok := dest["amount"].(int64); ok {
			stock.Amount = int(amount)
		}

		if recordedAt, ok := dest["recorded_at"].(time.Time); ok {
			stock.RecordedAt = recordedAt
		}

		// Добавляем в результат
		if _, ok := result[stock.ProductID]; !ok {
			result[stock.ProductID] = make(map[int64]models.StockRecord)
		}

		result[stock.ProductID][stock.WarehouseID] = stock
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating stock rows: %w", err)
	}

	return result, nil
}

// Получение записей об остатках за период пакетным запросом
func (s *RecordCleanupService) getStockRecordsForPeriodBatch(
	ctx context.Context,
	productIDs []int,
	warehouseIDs []int64,
	startTime,
	endTime time.Time,
) (map[int]map[int64][]models.StockRecord, error) {
	if len(productIDs) == 0 || len(warehouseIDs) == 0 {
		return make(map[int]map[int64][]models.StockRecord), nil
	}

	query := `
        SELECT * FROM stocks
        WHERE product_id = ANY($1)
        AND warehouse_id = ANY($2)
        AND recorded_at BETWEEN $3 AND $4
        ORDER BY product_id, warehouse_id, recorded_at ASC
    `

	rows, err := s.db.QueryxContext(ctx, query, pq.Array(productIDs), pq.Array(warehouseIDs), startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("querying stock records: %w", err)
	}
	defer rows.Close()

	result := make(map[int]map[int64][]models.StockRecord)

	for rows.Next() {
		var stock models.StockRecord

		// Создаем временную структуру для сканирования
		dest := map[string]interface{}{}

		if err := rows.MapScan(dest); err != nil {
			return nil, fmt.Errorf("scanning stock row: %w", err)
		}

		// Заполняем структуру StockRecord
		if productID, ok := dest["product_id"].(int64); ok {
			stock.ProductID = int(productID)
		}

		if warehouseID, ok := dest["warehouse_id"].(int64); ok {
			stock.WarehouseID = warehouseID
		}

		if amount, ok := dest["amount"].(int64); ok {
			stock.Amount = int(amount)
		}

		if recordedAt, ok := dest["recorded_at"].(time.Time); ok {
			stock.RecordedAt = recordedAt
		}

		// Добавляем в результат
		if _, ok := result[stock.ProductID]; !ok {
			result[stock.ProductID] = make(map[int64][]models.StockRecord)
		}

		result[stock.ProductID][stock.WarehouseID] = append(result[stock.ProductID][stock.WarehouseID], stock)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating stock rows: %w", err)
	}

	return result, nil
}

// Пакетная вставка снапшотов остатков
func (s *RecordCleanupService) batchInsertStockSnapshots(ctx context.Context, snapshots []models.StockSnapshot) error {
	if len(snapshots) == 0 {
		return nil
	}

	const maxRetries = 5
	const smallBatchSize = 200 // Значительно меньший размер пакета

	// 1. Сортируем снапшоты по идентификаторам для обеспечения постоянного порядка доступа
	sort.Slice(snapshots, func(i, j int) bool {
		if snapshots[i].ProductID != snapshots[j].ProductID {
			return snapshots[i].ProductID < snapshots[j].ProductID
		}
		if snapshots[i].WarehouseID != snapshots[j].WarehouseID {
			return snapshots[i].WarehouseID < snapshots[j].WarehouseID
		}
		return snapshots[i].SnapshotTime.Before(snapshots[j].SnapshotTime)
	})

	// 2. Обрабатываем данные очень маленькими пакетами для уменьшения времени транзакций
	var lastError error
	for i := 0; i < len(snapshots); i += smallBatchSize {
		end := i + smallBatchSize
		if end > len(snapshots) {
			end = len(snapshots)
		}

		batch := snapshots[i:end]

		// 3. Механизм повторных попыток с экспоненциальной задержкой
		for attempt := 0; attempt < maxRetries; attempt++ {
			err := s.insertStockSnapshotBatchSafely(ctx, batch)
			if err == nil {
				// Успешно - переходим к следующему пакету
				lastError = nil
				break
			}

			// Проверяем, был ли это deadlock или другая временная ошибка
			if strings.Contains(err.Error(), "deadlock detected") ||
				strings.Contains(err.Error(), "could not serialize access") {
				// Увеличиваем задержку с каждой попыткой (экспоненциальная задержка)
				delay := time.Duration(50*(1<<attempt)) * time.Millisecond
				log.Printf("Deadlock on stock snapshot batch %d/%d, attempt %d, retrying after %v",
					i/smallBatchSize+1, (len(snapshots)-1)/smallBatchSize+1, attempt+1, delay)
				time.Sleep(delay)
				lastError = err
				continue
			}

			// Другая ошибка - немедленно возвращаем
			return fmt.Errorf("inserting stock snapshot batch %d: %w", i/smallBatchSize+1, err)
		}

		// Если все попытки не удались
		if lastError != nil {
			return fmt.Errorf("failed to insert stock snapshot batch %d after %d attempts: %w",
				i/smallBatchSize+1, maxRetries, lastError)
		}
	}

	log.Printf("Successfully processed %d stock snapshots in %d small batches",
		len(snapshots), (len(snapshots)-1)/smallBatchSize+1)
	return nil
}

// Безопасная вставка одного мини-пакета снапшотов
func (s *RecordCleanupService) insertStockSnapshotBatchSafely(ctx context.Context, batch []models.StockSnapshot) error {
	// Используем более низкий уровень изоляции
	tx, err := s.db.BeginTxx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
		ReadOnly:  false,
	})
	if err != nil {
		return fmt.Errorf("starting transaction: %w", err)
	}
	defer tx.Rollback()

	// Прямая вставка в основную таблицу - без временных таблиц для уменьшения нагрузки
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO stock_snapshots (product_id, warehouse_id, amount, snapshot_time)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (product_id, warehouse_id, snapshot_time) DO UPDATE SET
			amount = EXCLUDED.amount
	`)
	if err != nil {
		return fmt.Errorf("preparing statement: %w", err)
	}
	defer stmt.Close()

	// Вставляем по одной записи
	for _, snapshot := range batch {
		_, err := stmt.ExecContext(ctx,
			snapshot.ProductID, snapshot.WarehouseID, snapshot.Amount, snapshot.SnapshotTime)
		if err != nil {
			return fmt.Errorf("executing insert: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	return nil
}

func (s *RecordCleanupService) processProductPrices(ctx context.Context, productID int, startDate, endDate time.Time) error {

	sizes, err := s.getProductSizes(ctx, productID)
	if err != nil {
		return fmt.Errorf("getting product sizes: %w", err)
	}

	if len(sizes) == 0 {
		return nil
	}

	for _, sizeID := range sizes {

		lastKnownPrice, err := s.getLastKnownPriceBefore(ctx, productID, sizeID, startDate)
		if err != nil {
			log.Printf("Error getting last known price for product %d, size %d: %v",
				productID, sizeID, err)
			continue
		}

		prices, err := s.getPriceRecordsForPeriod(ctx, productID, sizeID, startDate, endDate)
		if err != nil {
			log.Printf("Error getting price records for product %d, size %d: %v",
				productID, sizeID, err)
			continue
		}

		if err := s.ensureHourlyPriceSnapshots(ctx, productID, sizeID, lastKnownPrice, prices, startDate, endDate); err != nil {
			log.Printf("Error ensuring hourly price snapshots for product %d, size %d: %v",
				productID, sizeID, err)
		}
	}

	return nil
}

func (s *RecordCleanupService) ensureHourlyPriceSnapshots(
	ctx context.Context,
	productID int,
	sizeID int,
	lastKnownPrice *models.PriceRecord,
	prices []models.PriceRecord,
	startDate, endDate time.Time,
) error {
	if len(prices) == 0 && lastKnownPrice == nil {
		return nil
	}

	var allPrices []models.PriceRecord
	if lastKnownPrice != nil {

		allPrices = append(allPrices, *lastKnownPrice)
	}
	allPrices = append(allPrices, prices...)

	if len(allPrices) == 0 {
		return nil
	}

	priceByTimestamp := make(map[time.Time]models.PriceRecord)
	for _, price := range allPrices {
		priceByTimestamp[price.RecordedAt] = price
	}

	current := startDate
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("starting transaction: %w", err)
	}
	defer tx.Rollback()

	for current.Before(endDate) || current.Equal(endDate) {
		hourStart := time.Date(current.Year(), current.Month(), current.Day(), current.Hour(), 0, 0, 0, current.Location())
		hourEnd := hourStart.Add(time.Hour)

		var latestPrice *models.PriceRecord
		for _, price := range allPrices {
			if !price.RecordedAt.After(hourEnd) && (latestPrice == nil || price.RecordedAt.After(latestPrice.RecordedAt)) {
				priceCopy := price
				latestPrice = &priceCopy
			}
		}

		if latestPrice != nil {

			var snapshotExists int
			err := tx.QueryRowxContext(ctx, `
				SELECT COUNT(*) FROM price_snapshots 
				WHERE product_id = $1 AND size_id = $2 AND snapshot_time = $3
			`, productID, sizeID, hourStart).Scan(&snapshotExists)

			if err != nil {
				log.Printf("Error checking price snapshot for product %d, size %d, hour %s: %v",
					productID, sizeID, hourStart.Format("2006-01-02 15:04"), err)
				continue
			}

			if snapshotExists == 0 {

				snapshotPrice := &models.PriceSnapshot{
					ProductID:         productID,
					SizeID:            sizeID,
					Price:             latestPrice.Price,
					Discount:          latestPrice.Discount,
					ClubDiscount:      latestPrice.ClubDiscount,
					FinalPrice:        latestPrice.FinalPrice,
					ClubFinalPrice:    latestPrice.ClubFinalPrice,
					CurrencyIsoCode:   latestPrice.CurrencyIsoCode,
					TechSizeName:      latestPrice.TechSizeName,
					EditableSizePrice: latestPrice.EditableSizePrice,

					SnapshotTime: time.Date(latestPrice.RecordedAt.Year(), latestPrice.RecordedAt.Month(),
						latestPrice.RecordedAt.Day(), latestPrice.RecordedAt.Hour(), 0, 0, 0,
						latestPrice.RecordedAt.Location()),
				}

				_, err = tx.ExecContext(ctx, `
					INSERT INTO price_snapshots (
						product_id, size_id, price, discount, club_discount, 
						final_price, club_final_price, currency_iso_code, 
						tech_size_name, editable_size_price, snapshot_time
					) VALUES (
						$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
					)
					ON CONFLICT (product_id, size_id, snapshot_time) DO UPDATE SET
						price = EXCLUDED.price,
						discount = EXCLUDED.discount,
						club_discount = EXCLUDED.club_discount,
						final_price = EXCLUDED.final_price,
						club_final_price = EXCLUDED.club_final_price,
						currency_iso_code = EXCLUDED.currency_iso_code,
						tech_size_name = EXCLUDED.tech_size_name,
						editable_size_price = EXCLUDED.editable_size_price
				`, snapshotPrice.ProductID, snapshotPrice.SizeID, snapshotPrice.Price,
					snapshotPrice.Discount, snapshotPrice.ClubDiscount, snapshotPrice.FinalPrice,
					snapshotPrice.ClubFinalPrice, snapshotPrice.CurrencyIsoCode,
					snapshotPrice.TechSizeName, snapshotPrice.EditableSizePrice, snapshotPrice.SnapshotTime)

				if err != nil {
					log.Printf("Error inserting hourly price snapshot for product %d, size %d, hour %s: %v",
						productID, sizeID, hourStart.Format("2006-01-02 15:04"), err)
				}
			}

			current = hourEnd
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	return nil
}

func (s *RecordCleanupService) processProductStocks(ctx context.Context, productID int, startDate, endDate time.Time) error {

	warehouses, err := s.getProductWarehouses(ctx, productID)
	if err != nil {
		return fmt.Errorf("getting product warehouses: %w", err)
	}

	if len(warehouses) == 0 {
		return nil
	}

	for _, warehouseID := range warehouses {

		lastKnownStock, err := s.getLastKnownStockBefore(ctx, productID, warehouseID, startDate)
		if err != nil {
			log.Printf("Error getting last known stock for product %d, warehouse %d: %v",
				productID, warehouseID, err)
			continue
		}

		stocks, err := s.getStockRecordsForPeriod(ctx, productID, warehouseID, startDate, endDate)
		if err != nil {
			log.Printf("Error getting stock records for product %d, warehouse %d: %v",
				productID, warehouseID, err)
			continue
		}

		if err := s.ensureHourlyStockSnapshots(ctx, productID, warehouseID, lastKnownStock, stocks, startDate, endDate); err != nil {
			log.Printf("Error ensuring hourly stock snapshots for product %d, warehouse %d: %v",
				productID, warehouseID, err)
		}
	}

	return nil
}

func (s *RecordCleanupService) ensureHourlyStockSnapshots(
	ctx context.Context,
	productID int,
	warehouseID int64,
	lastKnownStock *models.StockRecord,
	stocks []models.StockRecord,
	startDate, endDate time.Time,
) error {
	if len(stocks) == 0 && lastKnownStock == nil {
		return nil
	}

	var allStocks []models.StockRecord
	if lastKnownStock != nil {

		allStocks = append(allStocks, *lastKnownStock)
	}
	allStocks = append(allStocks, stocks...)

	if len(allStocks) == 0 {
		return nil
	}

	current := startDate
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("starting transaction: %w", err)
	}
	defer tx.Rollback()

	for current.Before(endDate) || current.Equal(endDate) {
		hourStart := time.Date(current.Year(), current.Month(), current.Day(), current.Hour(), 0, 0, 0, current.Location())
		hourEnd := hourStart.Add(time.Hour)

		var latestStock *models.StockRecord
		for _, stock := range allStocks {
			if !stock.RecordedAt.After(hourEnd) && (latestStock == nil || stock.RecordedAt.After(latestStock.RecordedAt)) {
				stockCopy := stock
				latestStock = &stockCopy
			}
		}

		if latestStock != nil {

			var snapshotExists int
			err := tx.QueryRowxContext(ctx, `
				SELECT COUNT(*) FROM stock_snapshots 
				WHERE product_id = $1 AND warehouse_id = $2 AND snapshot_time = $3
			`, productID, warehouseID, hourStart).Scan(&snapshotExists)

			if err != nil {
				log.Printf("Error checking stock snapshot for product %d, warehouse %d, hour %s: %v",
					productID, warehouseID, hourStart.Format("2006-01-02 15:04"), err)
				continue
			}

			if snapshotExists == 0 {

				snapshotStock := &models.StockSnapshot{
					ProductID:   productID,
					WarehouseID: warehouseID,
					Amount:      latestStock.Amount,

					SnapshotTime: time.Date(latestStock.RecordedAt.Year(), latestStock.RecordedAt.Month(),
						latestStock.RecordedAt.Day(), latestStock.RecordedAt.Hour(), 0, 0, 0,
						latestStock.RecordedAt.Location()),
				}

				_, err = tx.ExecContext(ctx, `
				INSERT INTO stock_snapshots (
					product_id, warehouse_id, amount, snapshot_time
				) VALUES (
					$1, $2, $3, $4
				)
				ON CONFLICT (product_id, warehouse_id, snapshot_time) DO UPDATE SET
					amount = EXCLUDED.amount
			`, snapshotStock.ProductID, snapshotStock.WarehouseID, snapshotStock.Amount, snapshotStock.SnapshotTime)

				if err != nil {
					log.Printf("Error inserting hourly stock snapshot for product %d, warehouse %d, hour %s: %v",
						productID, warehouseID, hourStart.Format("2006-01-02 15:04"), err)
				}
			}

			current = hourEnd
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	return nil
}

func (s *RecordCleanupService) getAllProducts(ctx context.Context) ([]models.ProductRecord, error) {
	var products []models.ProductRecord
	query := "SELECT id, nm_id, vendor_code, barcode, name, created_at FROM products"

	err := s.db.SelectContext(ctx, &products, query)
	if err != nil {
		return nil, fmt.Errorf("selecting products: %w", err)
	}

	return products, nil
}

func (s *RecordCleanupService) getProductSizes(ctx context.Context, productID int) ([]int, error) {
	var sizes []int
	query := "SELECT DISTINCT size_id FROM prices WHERE product_id = $1"

	err := s.db.SelectContext(ctx, &sizes, query, productID)
	if err != nil {
		return nil, fmt.Errorf("selecting product sizes: %w", err)
	}

	return sizes, nil
}

func (s *RecordCleanupService) getProductWarehouses(ctx context.Context, productID int) ([]int64, error) {
	var warehouses []int64
	query := "SELECT DISTINCT warehouse_id FROM stocks WHERE product_id = $1"

	err := s.db.SelectContext(ctx, &warehouses, query, productID)
	if err != nil {
		return nil, fmt.Errorf("selecting product warehouses: %w", err)
	}

	return warehouses, nil
}

func (s *RecordCleanupService) getLastKnownPriceBefore(ctx context.Context, productID int, sizeID int, date time.Time) (*models.PriceRecord, error) {
	var record models.PriceRecord
	query := `
        SELECT * FROM prices
        WHERE product_id = $1 AND size_id = $2 AND recorded_at < $3
        ORDER BY recorded_at DESC
        LIMIT 1
    `

	err := s.db.GetContext(ctx, &record, query, productID, sizeID, date)
	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			return nil, nil
		}
		return nil, fmt.Errorf("selecting last known price: %w", err)
	}

	return &record, nil
}

func (s *RecordCleanupService) getLastKnownStockBefore(ctx context.Context, productID int, warehouseID int64, date time.Time) (*models.StockRecord, error) {
	var record models.StockRecord
	query := `
        SELECT * FROM stocks
        WHERE product_id = $1 AND warehouse_id = $2 AND recorded_at < $3
        ORDER BY recorded_at DESC
        LIMIT 1
    `

	err := s.db.GetContext(ctx, &record, query, productID, warehouseID, date)
	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			return nil, nil
		}
		return nil, fmt.Errorf("selecting last known stock: %w", err)
	}

	return &record, nil
}

func (s *RecordCleanupService) getPriceRecordsForPeriod(ctx context.Context, productID int, sizeID int, startTime, endTime time.Time) ([]models.PriceRecord, error) {
	var records []models.PriceRecord
	query := `
        SELECT * FROM prices
        WHERE product_id = $1 AND size_id = $2 AND recorded_at >= $3 AND recorded_at <= $4
        ORDER BY recorded_at ASC
    `

	err := s.db.SelectContext(ctx, &records, query, productID, sizeID, startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("selecting price records: %w", err)
	}

	return records, nil
}

func (s *RecordCleanupService) getStockRecordsForPeriod(ctx context.Context, productID int, warehouseID int64, startTime, endTime time.Time) ([]models.StockRecord, error) {
	var records []models.StockRecord
	query := `
        SELECT * FROM stocks
        WHERE product_id = $1 AND warehouse_id = $2 AND recorded_at >= $3 AND recorded_at <= $4
        ORDER BY recorded_at ASC
    `

	err := s.db.SelectContext(ctx, &records, query, productID, warehouseID, startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("selecting stock records: %w", err)
	}

	return records, nil
}

type DeletionStats struct {
	PricesDeleted    int64
	StocksDeleted    int64
	BatchesProcessed int64
	StartTime        time.Time
	LastBatchTime    time.Time
}

// Оптимизированная версия удаления старых записей
func (s *RecordCleanupService) deleteOldRecords(ctx context.Context, retentionDate time.Time) error {
	log.Printf("Starting deletion of records older than %s", retentionDate.Format("2006-01-02"))

	deleteCtx, cancel := context.WithTimeout(ctx, 6*time.Hour)
	defer cancel()

	priceStats, err := s.deleteOldPriceRecords(deleteCtx, retentionDate)
	if err != nil {
		return fmt.Errorf("error deleting old price records: %w", err)
	}

	stockStats, err := s.deleteOldStockRecords(deleteCtx, retentionDate)
	if err != nil {
		return fmt.Errorf("error deleting old stock records: %w", err)
	}

	log.Printf("Record cleanup completed. Deleted %d price records in %d batches, %d stock records in %d batches",
		priceStats.PricesDeleted, priceStats.BatchesProcessed,
		stockStats.StocksDeleted, stockStats.BatchesProcessed)

	return nil
}

func (s *RecordCleanupService) deleteOldPriceRecords(ctx context.Context, retentionDate time.Time) (*DeletionStats, error) {
	stats := &DeletionStats{
		StartTime: time.Now(),
	}

	batchSize := 50000

	for {
		select {
		case <-ctx.Done():
			return stats, ctx.Err()
		default:
		}

		batchCtx, cancel := context.WithTimeout(ctx, 50*time.Minute)

		result, err := s.db.ExecContext(batchCtx, `
			WITH old_price_ids AS (
				SELECT id FROM prices 
				WHERE recorded_at < $1
				LIMIT $2
			)
			DELETE FROM prices WHERE id IN (SELECT id FROM old_price_ids)
		`, retentionDate, batchSize)

		cancel()

		if err != nil {
			return stats, fmt.Errorf("batch deleting old price records: %w", err)
		}

		rowsAffected, _ := result.RowsAffected()
		stats.PricesDeleted += rowsAffected
		stats.BatchesProcessed++
		stats.LastBatchTime = time.Now()

		log.Printf("Deleted batch of %d old price records (total: %d)",
			rowsAffected, stats.PricesDeleted)

		if rowsAffected == 0 {
			break
		}

		select {
		case <-time.After(500 * time.Millisecond):
		case <-ctx.Done():
			return stats, ctx.Err()
		}
	}

	duration := time.Since(stats.StartTime)
	log.Printf("Price records deletion completed. Total deleted: %d in %s",
		stats.PricesDeleted, duration)

	return stats, nil
}

func (s *RecordCleanupService) deleteOldStockRecords(ctx context.Context, retentionDate time.Time) (*DeletionStats, error) {
	stats := &DeletionStats{
		StartTime: time.Now(),
	}

	batchSize := 50000

	for {
		select {
		case <-ctx.Done():
			return stats, ctx.Err()
		default:
		}

		batchCtx, cancel := context.WithTimeout(ctx, 50*time.Minute)

		result, err := s.db.ExecContext(batchCtx, `
			WITH old_stock_ids AS (
				SELECT id FROM stocks 
				WHERE recorded_at < $1
				LIMIT $2
			)
			DELETE FROM stocks WHERE id IN (SELECT id FROM old_stock_ids)
		`, retentionDate, batchSize)

		cancel()

		if err != nil {
			return stats, fmt.Errorf("batch deleting old stock records: %w", err)
		}

		rowsAffected, _ := result.RowsAffected()
		stats.StocksDeleted += rowsAffected
		stats.BatchesProcessed++
		stats.LastBatchTime = time.Now()

		log.Printf("Deleted batch of %d old stock records (total: %d)",
			rowsAffected, stats.StocksDeleted)

		if rowsAffected == 0 {
			break
		}

		select {
		case <-time.After(500 * time.Millisecond):
		case <-ctx.Done():
			return stats, ctx.Err()
		}
	}

	duration := time.Since(stats.StartTime)
	log.Printf("Stock records deletion completed. Total deleted: %d in %s",
		stats.StocksDeleted, duration)

	return stats, nil
}
