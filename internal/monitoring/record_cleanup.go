package monitoring

import (
	"context"
	"fmt"
	"log"
	"time"
	"wbmonitoring/monitoring/internal/models"

	"github.com/jmoiron/sqlx"
)

// RecordCleanupService представляет сервис для очистки и систематизации записей о ценах и остатках
type RecordCleanupService struct {
	db                *sqlx.DB
	cleanupInterval   time.Duration
	retentionInterval time.Duration
	hourlyDataKeeper  *HourlyDataKeeper
}

// NewRecordCleanupService создает новый сервис очистки записей
func NewRecordCleanupService(db *sqlx.DB, cleanupInterval, retentionInterval time.Duration) *RecordCleanupService {
	return &RecordCleanupService{
		db:                db,
		cleanupInterval:   cleanupInterval,
		retentionInterval: retentionInterval,
		//hourlyDataKeeper:  NewHourlyDataKeeper(db),
	}
}

// RunCleanupProcess запускает процесс очистки записей
func (s *RecordCleanupService) RunCleanupProcess(ctx context.Context) {
	ticker := time.NewTicker(s.cleanupInterval)
	defer ticker.Stop()

	log.Println("Record cleanup process started")

	// При запуске сразу запускаем очистку
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

// CleanupRecords выполняет очистку и систематизацию записей
func (s *RecordCleanupService) CleanupRecords(ctx context.Context) error {
	log.Println("Starting records cleanup process")

	// Получаем время начала и конца предыдущего дня
	now := time.Now()
	yesterday := now.AddDate(0, 0, -1)
	startOfYesterday := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 0, 0, 0, 0, now.Location())
	endOfYesterday := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 23, 59, 59, 999999999, now.Location())

	// Получаем все товары
	products, err := s.getAllProducts(ctx)
	if err != nil {
		return fmt.Errorf("getting products for cleanup: %w", err)
	}

	log.Printf("Found %d products to process", len(products))

	// Для каждого товара очищаем и систематизируем записи
	for _, product := range products {
		// Обрабатываем цены
		if err := s.processProductPrices(ctx, product.ID, startOfYesterday, endOfYesterday); err != nil {
			log.Printf("Error processing prices for product %d: %v", product.ID, err)
			continue
		}

		// Обрабатываем остатки
		if err := s.processProductStocks(ctx, product.ID, startOfYesterday, endOfYesterday); err != nil {
			log.Printf("Error processing stocks for product %d: %v", product.ID, err)
			continue
		}
	}

	// Удаляем записи старше периода хранения
	retentionDate := now.Add(-s.retentionInterval)
	if err := s.deleteOldRecords(ctx, retentionDate); err != nil {
		return fmt.Errorf("deleting old records: %w", err)
	}

	log.Println("Records cleanup process completed")
	return nil
}

// processProductPrices обрабатывает записи о ценах товара
func (s *RecordCleanupService) processProductPrices(ctx context.Context, productID int, startDate, endDate time.Time) error {
	// Получаем все размеры товара
	sizes, err := s.getProductSizes(ctx, productID)
	if err != nil {
		return fmt.Errorf("getting product sizes: %w", err)
	}

	if len(sizes) == 0 {
		return nil // Нет размеров для обработки
	}

	log.Printf("Processing prices for product %d with %d sizes", productID, len(sizes))

	// Обрабатываем записи для каждого размера отдельно
	for _, sizeID := range sizes {
		// Получаем последнюю известную цену до начала обрабатываемого периода
		lastKnownPrice, err := s.getLastKnownPriceBefore(ctx, productID, sizeID, startDate)
		if err != nil {
			log.Printf("Error getting last known price for product %d, size %d: %v",
				productID, sizeID, err)
			continue
		}

		// Обрабатываем каждый час дня
		for hour := 0; hour < 24; hour++ {
			hourStart := time.Date(startDate.Year(), startDate.Month(), startDate.Day(), hour, 0, 0, 0, startDate.Location())
			hourEnd := hourStart.Add(time.Hour)

			// Получаем все записи о ценах за час
			priceRecords, err := s.getPriceRecordsForHour(ctx, productID, sizeID, hourStart, hourEnd)
			if err != nil {
				log.Printf("Error getting price records for hour %d, product %d, size %d: %v",
					hour, productID, sizeID, err)
				continue
			}

			// Если нет записей за этот час, но есть предыдущая известная цена, создаем запись
			if len(priceRecords) == 0 {
				if lastKnownPrice != nil {
					// Создаем копию последней известной цены с новым временем
					hourlyRecord := *lastKnownPrice
					hourlyRecord.RecordedAt = hourStart
					hourlyRecord.ID = 0 // Сбрасываем ID, так как это новая запись

					// Добавляем запись в базу данных
					newID, err := s.insertPriceRecord(ctx, &hourlyRecord)
					if err != nil {
						log.Printf("Error inserting hourly price record for product %d, size %d, hour %d: %v",
							productID, sizeID, hour, err)
					} else {
						hourlyRecord.ID = newID
						priceRecords = append(priceRecords, hourlyRecord)
					}
				}
			} else if len(priceRecords) > 0 {
				// Если есть записи за этот час, обновляем последнюю известную цену
				lastKnownPrice = &priceRecords[len(priceRecords)-1]
			}

			// Сохраняем почасовой снимок и все изменения
			if len(priceRecords) > 0 {
				// Проверяем, есть ли уже почасовой снимок
				exists, err := s.hourlySnapshotExists(ctx, "hourly_prices", productID, sizeID, hourStart)
				if err != nil {
					log.Printf("Error checking hourly price snapshot for product %d, size %d, hour %d: %v",
						productID, sizeID, hour, err)
				} else if !exists {
					// Сохраняем почасовой снимок, только если его еще нет
					//err = s.hourlyDataKeeper.SaveHourlyPriceData(ctx, productID, sizeID, hourStart, priceRecords)
					if err != nil {
						log.Printf("Error saving hourly price data for hour %d, product %d, size %d: %v",
							hour, productID, sizeID, err)
					}
				}
			}
		}
	}

	return nil
}

// processProductStocks обрабатывает записи о складских остатках товара
func (s *RecordCleanupService) processProductStocks(ctx context.Context, productID int, startDate, endDate time.Time) error {
	// Получаем все склады, на которых есть товар
	warehouses, err := s.getProductWarehouses(ctx, productID)
	if err != nil {
		return fmt.Errorf("getting product warehouses: %w", err)
	}

	if len(warehouses) == 0 {
		return nil // Нет складов для обработки
	}

	log.Printf("Processing stocks for product %d with %d warehouses", productID, len(warehouses))

	// Обрабатываем записи для каждого склада отдельно
	for _, warehouseID := range warehouses {
		// Получаем последний известный остаток до начала обрабатываемого периода
		lastKnownStock, err := s.getLastKnownStockBefore(ctx, productID, warehouseID, startDate)
		if err != nil {
			log.Printf("Error getting last known stock for product %d, warehouse %d: %v",
				productID, warehouseID, err)
			continue
		}

		// Обрабатываем каждый час дня
		for hour := 0; hour < 24; hour++ {
			hourStart := time.Date(startDate.Year(), startDate.Month(), startDate.Day(), hour, 0, 0, 0, startDate.Location())
			hourEnd := hourStart.Add(time.Hour)

			// Получаем все записи об остатках за час
			stockRecords, err := s.getStockRecordsForHour(ctx, productID, warehouseID, hourStart, hourEnd)
			if err != nil {
				log.Printf("Error getting stock records for hour %d, product %d, warehouse %d: %v",
					hour, productID, warehouseID, err)
				continue
			}

			// Если нет записей за этот час, но есть предыдущий известный остаток, создаем запись
			if len(stockRecords) == 0 {
				if lastKnownStock != nil {
					// Создаем копию последнего известного остатка с новым временем
					hourlyRecord := *lastKnownStock
					hourlyRecord.RecordedAt = hourStart
					hourlyRecord.ID = 0 // Сбрасываем ID, так как это новая запись

					// Добавляем запись в базу данных
					newID, err := s.insertStockRecord(ctx, &hourlyRecord)
					if err != nil {
						log.Printf("Error inserting hourly stock record for product %d, warehouse %d, hour %d: %v",
							productID, warehouseID, hour, err)
					} else {
						hourlyRecord.ID = newID
						stockRecords = append(stockRecords, hourlyRecord)
					}
				}
			} else if len(stockRecords) > 0 {
				// Если есть записи за этот час, обновляем последний известный остаток
				lastKnownStock = &stockRecords[len(stockRecords)-1]
			}

			// Сохраняем почасовой снимок и все изменения
			if len(stockRecords) > 0 {
				// Проверяем, есть ли уже почасовой снимок
				exists, err := s.hourlySnapshotExists(ctx, "hourly_stocks", productID, int(warehouseID), hourStart)
				if err != nil {
					log.Printf("Error checking hourly stock snapshot for product %d, warehouse %d, hour %d: %v",
						productID, warehouseID, hour, err)
				} else if !exists {
					// Сохраняем почасовой снимок, только если его еще нет
					//err = s.hourlyDataKeeper.saveStockSnapshot(ctx, productID, warehouseID, hourStart, stockRecords)
					if err != nil {
						log.Printf("Error saving hourly stock data for hour %d, product %d, warehouse %d: %v",
							hour, productID, warehouseID, err)
					}
				}
			}
		}
	}

	return nil
}

// hourlySnapshotExists проверяет, существует ли уже почасовой снимок
func (s *RecordCleanupService) hourlySnapshotExists(ctx context.Context, tableName string, productID, itemID int, hour time.Time) (bool, error) {
	var count int
	var query string
	var args []interface{}

	if tableName == "hourly_prices" {
		query = `SELECT COUNT(*) FROM hourly_prices WHERE product_id = $1 AND size_id = $2 AND hour = $3`
		args = []interface{}{productID, itemID, hour}
	} else if tableName == "hourly_stocks" {
		query = `SELECT COUNT(*) FROM hourly_stocks WHERE product_id = $1 AND warehouse_id = $2 AND hour = $3`
		args = []interface{}{productID, itemID, hour}
	} else {
		return false, fmt.Errorf("unknown table name: %s", tableName)
	}

	err := s.db.GetContext(ctx, &count, query, args...)
	if err != nil {
		return false, fmt.Errorf("checking hourly snapshot existence: %w", err)
	}

	return count > 0, nil
}

// insertPriceRecord вставляет запись о цене и возвращает её ID
func (s *RecordCleanupService) insertPriceRecord(ctx context.Context, record *models.PriceRecord) (int, error) {
	query := `
        INSERT INTO prices (
            product_id, size_id, price, discount, club_discount, 
            final_price, club_final_price, currency_iso_code, 
            tech_size_name, editable_size_price, recorded_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
        ) RETURNING id
    `

	var id int
	err := s.db.QueryRowContext(
		ctx,
		query,
		record.ProductID, record.SizeID, record.Price, record.Discount, record.ClubDiscount,
		record.FinalPrice, record.ClubFinalPrice, record.CurrencyIsoCode,
		record.TechSizeName, record.EditableSizePrice, record.RecordedAt,
	).Scan(&id)

	if err != nil {
		return 0, fmt.Errorf("inserting price record: %w", err)
	}

	return id, nil
}

// insertStockRecord вставляет запись о складском остатке и возвращает её ID
func (s *RecordCleanupService) insertStockRecord(ctx context.Context, record *models.StockRecord) (int, error) {
	query := `
        INSERT INTO stocks (
            product_id, warehouse_id, amount, recorded_at
        ) VALUES (
            $1, $2, $3, $4
        ) RETURNING id
    `

	var id int
	err := s.db.QueryRowContext(
		ctx,
		query,
		record.ProductID, record.WarehouseID, record.Amount, record.RecordedAt,
	).Scan(&id)

	if err != nil {
		return 0, fmt.Errorf("inserting stock record: %w", err)
	}

	return id, nil
}

// getLastKnownPriceBefore возвращает последнюю известную цену до указанной даты
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
			return nil, nil // Нет предыдущих записей
		}
		return nil, fmt.Errorf("selecting last known price: %w", err)
	}

	return &record, nil
}

// getLastKnownStockBefore возвращает последний известный остаток до указанной даты
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
			return nil, nil // Нет предыдущих записей
		}
		return nil, fmt.Errorf("selecting last known stock: %w", err)
	}

	return &record, nil
}

// deleteOldRecords удаляет записи старше указанной даты
func (s *RecordCleanupService) deleteOldRecords(ctx context.Context, retentionDate time.Time) error {
	// Удаляем старые записи о ценах
	priceQuery := "DELETE FROM prices WHERE recorded_at < $1"
	_, err := s.db.ExecContext(ctx, priceQuery, retentionDate)
	if err != nil {
		return fmt.Errorf("deleting old price records: %w", err)
	}

	// Удаляем старые записи о складских остатках
	stockQuery := "DELETE FROM stocks WHERE recorded_at < $1"
	_, err = s.db.ExecContext(ctx, stockQuery, retentionDate)
	if err != nil {
		return fmt.Errorf("deleting old stock records: %w", err)
	}

	log.Printf("Deleted records older than %s", retentionDate.Format("2006-01-02"))
	return nil
}

// getAllProducts возвращает все товары из базы данных
func (s *RecordCleanupService) getAllProducts(ctx context.Context) ([]models.ProductRecord, error) {
	var products []models.ProductRecord
	query := "SELECT id, nm_id, vendor_code, barcode, name, created_at FROM products"

	err := s.db.SelectContext(ctx, &products, query)
	if err != nil {
		return nil, fmt.Errorf("selecting products: %w", err)
	}

	return products, nil
}

// getProductSizes возвращает все размеры товара
func (s *RecordCleanupService) getProductSizes(ctx context.Context, productID int) ([]int, error) {
	var sizes []int
	query := "SELECT DISTINCT size_id FROM prices WHERE product_id = $1"

	err := s.db.SelectContext(ctx, &sizes, query, productID)
	if err != nil {
		return nil, fmt.Errorf("selecting product sizes: %w", err)
	}

	return sizes, nil
}

// getProductWarehouses возвращает все склады, на которых есть товар
func (s *RecordCleanupService) getProductWarehouses(ctx context.Context, productID int) ([]int64, error) {
	var warehouses []int64
	query := "SELECT DISTINCT warehouse_id FROM stocks WHERE product_id = $1"

	err := s.db.SelectContext(ctx, &warehouses, query, productID)
	if err != nil {
		return nil, fmt.Errorf("selecting product warehouses: %w", err)
	}

	return warehouses, nil
}

// getPriceRecordsForHour возвращает все записи о ценах товара за указанный час
func (s *RecordCleanupService) getPriceRecordsForHour(
	ctx context.Context,
	productID int,
	sizeID int,
	startTime time.Time,
	endTime time.Time) ([]models.PriceRecord, error) {

	var records []models.PriceRecord
	query := `
        SELECT * FROM prices
        WHERE product_id = $1 AND size_id = $2 AND recorded_at >= $3 AND recorded_at < $4
        ORDER BY recorded_at ASC
    `

	err := s.db.SelectContext(ctx, &records, query, productID, sizeID, startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("selecting price records: %w", err)
	}

	return records, nil
}

// getStockRecordsForHour возвращает все записи о складских остатках товара за указанный час
func (s *RecordCleanupService) getStockRecordsForHour(
	ctx context.Context,
	productID int,
	warehouseID int64,
	startTime time.Time,
	endTime time.Time) ([]models.StockRecord, error) {

	var records []models.StockRecord
	query := `
        SELECT * FROM stocks
        WHERE product_id = $1 AND warehouse_id = $2 AND recorded_at >= $3 AND recorded_at < $4
        ORDER BY recorded_at ASC
    `

	err := s.db.SelectContext(ctx, &records, query, productID, warehouseID, startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("selecting stock records: %w", err)
	}

	return records, nil
}
