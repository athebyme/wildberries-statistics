package monitoring

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/lib/pq"
	"log"
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

	// Добавляем таймаут для избежания зависаний
	ctx, cancel := context.WithTimeout(ctx, 2*time.Hour)
	defer cancel()

	now := time.Now()
	yesterday := now.AddDate(0, 0, -1)
	startOfYesterday := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 0, 0, 0, 0, now.Location())
	endOfYesterday := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 23, 59, 59, 999999999, now.Location())

	// Получаем список продуктов
	products, err := s.getAllProducts(ctx)
	if err != nil {
		return fmt.Errorf("getting products for cleanup: %w", err)
	}

	log.Printf("Found %d products to process for cleanup", len(products))

	// Ограничиваем размер пакета для обработки
	batchSize := 50

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
		batchCtx, batchCancel := context.WithTimeout(ctx, 10*time.Minute)

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
	retentionDate := now.Add(-s.retentionInterval)
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
						ProductID:    productID,
						WarehouseID:  warehouseID,
						Amount:       latestStock.Amount,
						SnapshotTime: hourStart,
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
						SnapshotTime:      hourStart,
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
	// Используем транзакцию для пакетной вставки
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("starting transaction: %w", err)
	}
	defer tx.Rollback()

	// Подготавливаем запрос для пакетной вставки
	stmt, err := tx.PrepareContext(ctx, `
        INSERT INTO price_snapshots (
            product_id, size_id, price, discount, club_discount, 
            final_price, club_final_price, currency_iso_code, 
            tech_size_name, editable_size_price, snapshot_time
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
        )
        ON CONFLICT (product_id, size_id, snapshot_time) DO NOTHING
    `)
	if err != nil {
		return fmt.Errorf("preparing statement: %w", err)
	}
	defer stmt.Close()

	// Вставляем снапшоты пакетами по 1000 штук
	batchSize := 1000

	for i := 0; i < len(snapshots); i += batchSize {
		end := i + batchSize
		if end > len(snapshots) {
			end = len(snapshots)
		}

		batch := snapshots[i:end]

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
	}

	// Фиксируем транзакцию
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	log.Printf("Inserted %d price snapshots", len(snapshots))

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
	// Используем транзакцию для пакетной вставки
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("starting transaction: %w", err)
	}
	defer tx.Rollback()

	// Подготавливаем запрос для пакетной вставки
	stmt, err := tx.PrepareContext(ctx, `
        INSERT INTO stock_snapshots (
            product_id, warehouse_id, amount, snapshot_time
        ) VALUES (
            $1, $2, $3, $4
        )
        ON CONFLICT (product_id, warehouse_id, snapshot_time) DO NOTHING
    `)
	if err != nil {
		return fmt.Errorf("preparing statement: %w", err)
	}
	defer stmt.Close()

	// Вставляем снапшоты пакетами по 1000 штук
	batchSize := 1000

	for i := 0; i < len(snapshots); i += batchSize {
		end := i + batchSize
		if end > len(snapshots) {
			end = len(snapshots)
		}

		batch := snapshots[i:end]

		for _, snapshot := range batch {
			_, err := stmt.ExecContext(ctx,
				snapshot.ProductID, snapshot.WarehouseID, snapshot.Amount, snapshot.SnapshotTime)

			if err != nil {
				return fmt.Errorf("executing insert: %w", err)
			}
		}
	}

	// Фиксируем транзакцию
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	log.Printf("Inserted %d stock snapshots", len(snapshots))

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
					SnapshotTime:      hourStart,
				}

				_, err = tx.ExecContext(ctx, `
					INSERT INTO price_snapshots (
						product_id, size_id, price, discount, club_discount, 
						final_price, club_final_price, currency_iso_code, 
						tech_size_name, editable_size_price, snapshot_time
					) VALUES (
						$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
					)
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
					ProductID:    productID,
					WarehouseID:  warehouseID,
					Amount:       latestStock.Amount,
					SnapshotTime: hourStart,
				}

				_, err = tx.ExecContext(ctx, `
					INSERT INTO stock_snapshots (
						product_id, warehouse_id, amount, snapshot_time
					) VALUES (
						$1, $2, $3, $4
					)
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

// Оптимизированная версия удаления старых записей
func (s *RecordCleanupService) deleteOldRecords(ctx context.Context, retentionDate time.Time) error {
	// Определяем окно безопасности - не трогаем данные новее 1 часа
	safetyWindow := time.Now().Add(-1 * time.Hour)
	if safetyWindow.Before(retentionDate) {
		safetyWindow = retentionDate
	}

	log.Printf("Cleaning up records older than %s with safety window at %s",
		retentionDate.Format("2006-01-02 15:04:05"),
		safetyWindow.Format("2006-01-02 15:04:05"))

	// Устанавливаем порог для значимых изменений
	// Порог 0 означает "сохранять все изменения"
	const (
		defaultPriceThreshold = 0.0 // 0% - сохранять все изменения цен
		defaultStockThreshold = 0.0 // 0% - сохранять все изменения остатков
	)

	// Запрос для интеллектуального удаления записей о ценах с учетом окна безопасности
	priceQuery := `
        WITH 
        -- Первая и последняя запись за день
        daily_boundaries AS (
            SELECT id 
            FROM (
                SELECT 
                    id,
                    product_id,
                    size_id,
                    recorded_at,
                    ROW_NUMBER() OVER (PARTITION BY product_id, size_id, DATE_TRUNC('day', recorded_at) ORDER BY recorded_at ASC) as first_row,
                    ROW_NUMBER() OVER (PARTITION BY product_id, size_id, DATE_TRUNC('day', recorded_at) ORDER BY recorded_at DESC) as last_row
                FROM prices
                WHERE recorded_at < $1 
                AND recorded_at < $3 -- Используем окно безопасности
            ) t
            WHERE first_row = 1 OR last_row = 1
        ),
        -- Записи со значительными изменениями цены или все изменения при пороге 0
        significant_changes AS (
            SELECT p.id
            FROM prices p
            JOIN (
                SELECT 
                    product_id, 
                    size_id, 
                    recorded_at,
                    price,
                    LAG(price) OVER (PARTITION BY product_id, size_id ORDER BY recorded_at) as prev_price
                FROM prices
                WHERE recorded_at < $1
                AND recorded_at < $3 -- Используем окно безопасности
            ) prev ON p.product_id = prev.product_id AND p.size_id = prev.size_id AND p.recorded_at = prev.recorded_at
            WHERE 
                prev.prev_price IS NOT NULL AND
                (
                    $2 <= 0 OR -- Если порог 0 или отрицательный, сохраняем все изменения
                    (prev.price > 0 AND ABS((p.price - prev.prev_price)::float / prev.prev_price * 100) >= $2)
                )
        ),
        -- Одна запись для каждого часа
        hourly_records AS (
            SELECT id
            FROM (
                SELECT 
                    id,
                    product_id,
                    size_id,
                    recorded_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY 
                            product_id, 
                            size_id, 
                            DATE_TRUNC('hour', recorded_at) 
                        ORDER BY 
                            ABS(EXTRACT(EPOCH FROM (recorded_at - DATE_TRUNC('hour', recorded_at))))
                    ) as rn
                FROM prices
                WHERE recorded_at < $1
                AND recorded_at < $3 -- Используем окно безопасности
            ) t
            WHERE rn = 1
        ),
        -- Все записи, которые нужно сохранить
        records_to_keep AS (
            SELECT id FROM daily_boundaries
            UNION
            SELECT id FROM significant_changes
            UNION
            SELECT id FROM hourly_records
        ),
        -- Записи, которые можно удалить
        records_to_delete AS (
            SELECT id 
            FROM prices
            WHERE recorded_at < $1
            AND recorded_at < $3 -- Используем окно безопасности
            AND id NOT IN (SELECT id FROM records_to_keep)
            LIMIT 50000
        )
        -- Удаляем только отобранные записи
        DELETE FROM prices 
        WHERE id IN (SELECT id FROM records_to_delete)
    `

	// Аналогичный запрос для очистки остатков (stocks), также с окном безопасности
	stockQuery := `
        WITH 
        -- Первая и последняя запись за день
        daily_boundaries AS (
            SELECT id 
            FROM (
                SELECT 
                    id,
                    product_id,
                    warehouse_id,
                    recorded_at,
                    ROW_NUMBER() OVER (PARTITION BY product_id, warehouse_id, DATE_TRUNC('day', recorded_at) ORDER BY recorded_at ASC) as first_row,
                    ROW_NUMBER() OVER (PARTITION BY product_id, warehouse_id, DATE_TRUNC('day', recorded_at) ORDER BY recorded_at DESC) as last_row
                FROM stocks
                WHERE recorded_at < $1
                AND recorded_at < $3 -- Используем окно безопасности
            ) t
            WHERE first_row = 1 OR last_row = 1
        ),
        -- Записи со значительными изменениями остатков или все изменения при пороге 0
        significant_changes AS (
            SELECT s.id
            FROM stocks s
            JOIN (
                SELECT 
                    product_id, 
                    warehouse_id, 
                    recorded_at,
                    amount,
                    LAG(amount) OVER (PARTITION BY product_id, warehouse_id ORDER BY recorded_at) as prev_amount
                FROM stocks
                WHERE recorded_at < $1
                AND recorded_at < $3 -- Используем окно безопасности
            ) prev ON s.product_id = prev.product_id AND s.warehouse_id = prev.warehouse_id AND s.recorded_at = prev.recorded_at
            WHERE 
                prev.prev_amount IS NOT NULL AND
                (
                    $2 <= 0 OR -- Если порог 0 или отрицательный, сохраняем все изменения
                    (prev.prev_amount > 0 AND ABS((s.amount - prev.prev_amount)::float / prev.prev_amount * 100) >= $2)
                )
        ),
        -- Одна запись для каждого часа
        hourly_records AS (
            SELECT id
            FROM (
                SELECT 
                    id,
                    product_id,
                    warehouse_id,
                    recorded_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY 
                            product_id, 
                            warehouse_id, 
                            DATE_TRUNC('hour', recorded_at) 
                        ORDER BY 
                            ABS(EXTRACT(EPOCH FROM (recorded_at - DATE_TRUNC('hour', recorded_at))))
                    ) as rn
                FROM stocks
                WHERE recorded_at < $1
                AND recorded_at < $3 -- Используем окно безопасности
            ) t
            WHERE rn = 1
        ),
        -- Все записи, которые нужно сохранить
        records_to_keep AS (
            SELECT id FROM daily_boundaries
            UNION
            SELECT id FROM significant_changes
            UNION
            SELECT id FROM hourly_records
        ),
        -- Записи, которые можно удалить
        records_to_delete AS (
            SELECT id 
            FROM stocks
            WHERE recorded_at < $1
            AND recorded_at < $3 -- Используем окно безопасности
            AND id NOT IN (SELECT id FROM records_to_keep)
            LIMIT 50000
        )
        -- Удаляем только отобранные записи
        DELETE FROM stocks 
        WHERE id IN (SELECT id FROM records_to_delete)
    `

	// Выполняем запрос для цен с изоляцией READ COMMITTED
	tx, err := s.db.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("starting transaction for prices: %w", err)
	}

	priceResult, err := tx.ExecContext(ctx, priceQuery, retentionDate, defaultPriceThreshold, safetyWindow)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("deleting old price records: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing price deletion transaction: %w", err)
	}

	priceRows, _ := priceResult.RowsAffected()
	log.Printf("Deleted %d old price records while preserving important data (safety window: %s)",
		priceRows, safetyWindow.Format("2006-01-02 15:04:05"))

	// Если были удалены записи, продолжаем удалять пакетами
	if priceRows > 0 {
		go s.continueIntelligentDeleteOldPrices(context.Background(), retentionDate, defaultPriceThreshold, safetyWindow)
	}

	// Выполняем запрос для остатков с изоляцией READ COMMITTED
	stockTx, err := s.db.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("starting transaction for stocks: %w", err)
	}

	stockResult, err := stockTx.ExecContext(ctx, stockQuery, retentionDate, defaultStockThreshold, safetyWindow)
	if err != nil {
		stockTx.Rollback()
		return fmt.Errorf("deleting old stock records: %w", err)
	}

	if err := stockTx.Commit(); err != nil {
		return fmt.Errorf("committing stock deletion transaction: %w", err)
	}

	stockRows, _ := stockResult.RowsAffected()
	log.Printf("Deleted %d old stock records while preserving important data (safety window: %s)",
		stockRows, safetyWindow.Format("2006-01-02 15:04:05"))

	// Если были удалены записи, продолжаем удалять пакетами
	if stockRows > 0 {
		go s.continueIntelligentDeleteOldStocks(context.Background(), retentionDate, defaultStockThreshold, safetyWindow)
	}

	log.Printf("Started intelligent background deletion of old records older than %s with safety window at %s",
		retentionDate.Format("2006-01-02"),
		safetyWindow.Format("2006-01-02 15:04:05"))
	return nil
}

// Продолжение интеллектуального удаления старых цен в фоне
func (s *RecordCleanupService) continueIntelligentDeleteOldPrices(ctx context.Context, retentionDate time.Time, priceThreshold float64, safetyWindow time.Time) {
	// Создаем новый контекст с таймаутом
	ctx, cancel := context.WithTimeout(ctx, 1*time.Hour)
	defer cancel()

	for {
		// Проверяем контекст перед выполнением операции
		select {
		case <-ctx.Done():
			log.Printf("Background price deletion stopped: %v", ctx.Err())
			return
		default:
			// продолжаем
		}

		// Обновляем окно безопасности, если прошло достаточно времени
		currentSafetyWindow := time.Now().Add(-1 * time.Hour)
		if currentSafetyWindow.Before(safetyWindow) {
			currentSafetyWindow = safetyWindow
		}

		// Тот же запрос, что и в основном методе, но с обновленным окном безопасности
		query := `
            WITH 
            daily_boundaries AS (
                SELECT id 
                FROM (
                    SELECT 
                        id,
                        product_id,
                        size_id,
                        recorded_at,
                        ROW_NUMBER() OVER (PARTITION BY product_id, size_id, DATE_TRUNC('day', recorded_at) ORDER BY recorded_at ASC) as first_row,
                        ROW_NUMBER() OVER (PARTITION BY product_id, size_id, DATE_TRUNC('day', recorded_at) ORDER BY recorded_at DESC) as last_row
                    FROM prices
                    WHERE recorded_at < $1
                    AND recorded_at < $3
                ) t
                WHERE first_row = 1 OR last_row = 1
            ),
            significant_changes AS (
                SELECT p.id
                FROM prices p
                JOIN (
                    SELECT 
                        product_id, 
                        size_id, 
                        recorded_at,
                        price,
                        LAG(price) OVER (PARTITION BY product_id, size_id ORDER BY recorded_at) as prev_price
                    FROM prices
                    WHERE recorded_at < $1
                    AND recorded_at < $3
                ) prev ON p.product_id = prev.product_id AND p.size_id = prev.size_id AND p.recorded_at = prev.recorded_at
                WHERE 
                    prev.prev_price IS NOT NULL AND
                    (
                        $2 <= 0 OR
                        (prev.price > 0 AND ABS((p.price - prev.prev_price)::float / prev.prev_price * 100) >= $2)
                    )
            ),
            hourly_records AS (
                SELECT id
                FROM (
                    SELECT 
                        id,
                        product_id,
                        size_id,
                        recorded_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY 
                                product_id, 
                                size_id, 
                                DATE_TRUNC('hour', recorded_at) 
                            ORDER BY 
                                ABS(EXTRACT(EPOCH FROM (recorded_at - DATE_TRUNC('hour', recorded_at))))
                        ) as rn
                    FROM prices
                    WHERE recorded_at < $1
                    AND recorded_at < $3
                ) t
                WHERE rn = 1
            ),
            records_to_keep AS (
                SELECT id FROM daily_boundaries
                UNION
                SELECT id FROM significant_changes
                UNION
                SELECT id FROM hourly_records
            ),
            records_to_delete AS (
                SELECT id 
                FROM prices
                WHERE recorded_at < $1
                AND recorded_at < $3
                AND id NOT IN (SELECT id FROM records_to_keep)
                LIMIT 50000
            )
            DELETE FROM prices 
            WHERE id IN (SELECT id FROM records_to_delete)
        `

		// Используем транзакцию с уровнем изоляции READ COMMITTED
		tx, err := s.db.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
		if err != nil {
			log.Printf("Error starting transaction for background price deletion: %v", err)
			return
		}

		result, err := tx.ExecContext(ctx, query, retentionDate, priceThreshold, currentSafetyWindow)
		if err != nil {
			tx.Rollback()
			log.Printf("Error during intelligent background price deletion: %v", err)
			return
		}

		if err := tx.Commit(); err != nil {
			log.Printf("Error committing background price deletion: %v", err)
			return
		}

		rows, _ := result.RowsAffected()
		if rows == 0 {
			log.Printf("Intelligent background price deletion completed")
			return
		}

		log.Printf("Deleted additional %d old price records while preserving important data (safety window: %s)",
			rows, currentSafetyWindow.Format("2006-01-02 15:04:05"))

		// Пауза между пакетами для снижения нагрузки на БД
		select {
		case <-time.After(1 * time.Second):
			// продолжаем после паузы
		case <-ctx.Done():
			log.Printf("Background price deletion stopped: %v", ctx.Err())
			return
		}
	}
}

// Продолжение интеллектуального удаления старых остатков в фоне
func (s *RecordCleanupService) continueIntelligentDeleteOldStocks(ctx context.Context, retentionDate time.Time, stockThreshold float64, safetyWindow time.Time) {
	// Создаем новый контекст с таймаутом
	ctx, cancel := context.WithTimeout(ctx, 1*time.Hour)
	defer cancel()

	for {
		// Проверяем контекст перед выполнением операции
		select {
		case <-ctx.Done():
			log.Printf("Background stock deletion stopped: %v", ctx.Err())
			return
		default:
			// продолжаем
		}

		// Обновляем окно безопасности, если прошло достаточно времени
		currentSafetyWindow := time.Now().Add(-1 * time.Hour)
		if currentSafetyWindow.Before(safetyWindow) {
			currentSafetyWindow = safetyWindow
		}

		// Тот же запрос, что и в основном методе, но с обновленным окном безопасности
		query := `
            WITH 
            daily_boundaries AS (
                SELECT id 
                FROM (
                    SELECT 
                        id,
                        product_id,
                        warehouse_id,
                        recorded_at,
                        ROW_NUMBER() OVER (PARTITION BY product_id, warehouse_id, DATE_TRUNC('day', recorded_at) ORDER BY recorded_at ASC) as first_row,
                        ROW_NUMBER() OVER (PARTITION BY product_id, warehouse_id, DATE_TRUNC('day', recorded_at) ORDER BY recorded_at DESC) as last_row
                    FROM stocks
                    WHERE recorded_at < $1
                    AND recorded_at < $3
                ) t
                WHERE first_row = 1 OR last_row = 1
            ),
            significant_changes AS (
                SELECT s.id
                FROM stocks s
                JOIN (
                    SELECT 
                        product_id, 
                        warehouse_id, 
                        recorded_at,
                        amount,
                        LAG(amount) OVER (PARTITION BY product_id, warehouse_id ORDER BY recorded_at) as prev_amount
                    FROM stocks
                    WHERE recorded_at < $1
                    AND recorded_at < $3
                ) prev ON s.product_id = prev.product_id AND s.warehouse_id = prev.warehouse_id AND s.recorded_at = prev.recorded_at
                WHERE 
                    prev.prev_amount IS NOT NULL AND
                    (
                        $2 <= 0 OR
                        (prev.prev_amount > 0 AND ABS((s.amount - prev.prev_amount)::float / prev.prev_amount * 100) >= $2)
                    )
            ),
            hourly_records AS (
                SELECT id
                FROM (
                    SELECT 
                        id,
                        product_id,
                        warehouse_id,
                        recorded_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY 
                                product_id, 
                                warehouse_id, 
                                DATE_TRUNC('hour', recorded_at) 
                            ORDER BY 
                                ABS(EXTRACT(EPOCH FROM (recorded_at - DATE_TRUNC('hour', recorded_at))))
                        ) as rn
                    FROM stocks
                    WHERE recorded_at < $1
                    AND recorded_at < $3
                ) t
                WHERE rn = 1
            ),
            records_to_keep AS (
                SELECT id FROM daily_boundaries
                UNION
                SELECT id FROM significant_changes
                UNION
                SELECT id FROM hourly_records
            ),
            records_to_delete AS (
                SELECT id 
                FROM stocks
                WHERE recorded_at < $1
                AND recorded_at < $3
                AND id NOT IN (SELECT id FROM records_to_keep)
                LIMIT 50000
            )
            DELETE FROM stocks 
            WHERE id IN (SELECT id FROM records_to_delete)
        `

		// Используем транзакцию с уровнем изоляции READ COMMITTED
		tx, err := s.db.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
		if err != nil {
			log.Printf("Error starting transaction for background stock deletion: %v", err)
			return
		}

		result, err := tx.ExecContext(ctx, query, retentionDate, stockThreshold, currentSafetyWindow)
		if err != nil {
			tx.Rollback()
			log.Printf("Error during intelligent background stock deletion: %v", err)
			return
		}

		if err := tx.Commit(); err != nil {
			log.Printf("Error committing background stock deletion: %v", err)
			return
		}

		rows, _ := result.RowsAffected()
		if rows == 0 {
			log.Printf("Intelligent background stock deletion completed")
			return
		}

		log.Printf("Deleted additional %d old stock records while preserving important data (safety window: %s)",
			rows, currentSafetyWindow.Format("2006-01-02 15:04:05"))

		// Пауза между пакетами для снижения нагрузки на БД
		select {
		case <-time.After(1 * time.Second):
			// продолжаем после паузы
		case <-ctx.Done():
			log.Printf("Background stock deletion stopped: %v", ctx.Err())
			return
		}
	}
}

// Продолжение удаления старых цен в фоне
func (s *RecordCleanupService) continueDeleteOldPrices(ctx context.Context, retentionDate time.Time) {
	// Создаем новый контекст с таймаутом
	ctx, cancel := context.WithTimeout(ctx, 1*time.Hour)
	defer cancel()

	for {
		// Проверяем контекст перед выполнением операции
		select {
		case <-ctx.Done():
			log.Printf("Background price deletion stopped: %v", ctx.Err())
			return
		default:
			// продолжаем
		}

		result, err := s.db.ExecContext(ctx, `
            WITH old_price_ids AS (
                SELECT id FROM prices 
                WHERE recorded_at < $1
                LIMIT 50000
            )
            DELETE FROM prices WHERE id IN (SELECT id FROM old_price_ids)
        `, retentionDate)

		if err != nil {
			log.Printf("Error during background price deletion: %v", err)
			return
		}

		rows, _ := result.RowsAffected()
		if rows == 0 {
			log.Printf("Background price deletion completed")
			return
		}

		log.Printf("Deleted additional %d old price records", rows)

		// Пауза между пакетами для снижения нагрузки на БД
		select {
		case <-time.After(1 * time.Second):
			// продолжаем после паузы
		case <-ctx.Done():
			log.Printf("Background price deletion stopped: %v", ctx.Err())
			return
		}
	}
}

// Продолжение удаления старых остатков в фоне
func (s *RecordCleanupService) continueDeleteOldStocks(ctx context.Context, retentionDate time.Time) {
	// Создаем новый контекст с таймаутом
	ctx, cancel := context.WithTimeout(ctx, 1*time.Hour)
	defer cancel()

	for {
		// Проверяем контекст перед выполнением операции
		select {
		case <-ctx.Done():
			log.Printf("Background stock deletion stopped: %v", ctx.Err())
			return
		default:
			// продолжаем
		}

		result, err := s.db.ExecContext(ctx, `
            WITH old_stock_ids AS (
                SELECT id FROM stocks 
                WHERE recorded_at < $1
                LIMIT 50000
            )
            DELETE FROM stocks WHERE id IN (SELECT id FROM old_stock_ids)
        `, retentionDate)

		if err != nil {
			log.Printf("Error during background stock deletion: %v", err)
			return
		}

		rows, _ := result.RowsAffected()
		if rows == 0 {
			log.Printf("Background stock deletion completed")
			return
		}

		log.Printf("Deleted additional %d old stock records", rows)

		// Пауза между пакетами для снижения нагрузки на БД
		select {
		case <-time.After(1 * time.Second):
			// продолжаем после паузы
		case <-ctx.Done():
			log.Printf("Background stock deletion stopped: %v", ctx.Err())
			return
		}
	}
}
