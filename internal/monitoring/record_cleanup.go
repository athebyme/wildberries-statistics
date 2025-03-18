package monitoring

import (
	"context"
	"fmt"
	"log"
	"sync"
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

	now := time.Now()
	yesterday := now.AddDate(0, 0, -1)
	startOfYesterday := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 0, 0, 0, 0, now.Location())
	endOfYesterday := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 23, 59, 59, 999999999, now.Location())

	products, err := s.getAllProducts(ctx)
	if err != nil {
		return fmt.Errorf("getting products for cleanup: %w", err)
	}

	log.Printf("Found %d products to process for cleanup", len(products))

	batchSize := 20
	var wg sync.WaitGroup
	sem := make(chan struct{}, s.workerPoolSize)

	for i := 0; i < len(products); i += batchSize {
		end := i + batchSize
		if end > len(products) {
			end = len(products)
		}

		batch := products[i:end]
		wg.Add(1)

		go func(productBatch []models.ProductRecord) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			for _, product := range productBatch {

				if err := s.processProductPrices(ctx, product.ID, startOfYesterday, endOfYesterday); err != nil {
					log.Printf("Error processing prices for product %d: %v", product.ID, err)
				}

				if err := s.processProductStocks(ctx, product.ID, startOfYesterday, endOfYesterday); err != nil {
					log.Printf("Error processing stocks for product %d: %v", product.ID, err)
				}
			}
		}(batch)

		if end < len(products) {
			time.Sleep(50 * time.Millisecond)
		}
	}

	wg.Wait()

	retentionDate := now.Add(-s.retentionInterval)
	if err := s.deleteOldRecords(ctx, retentionDate); err != nil {
		return fmt.Errorf("deleting old records: %w", err)
	}

	log.Println("Records cleanup process completed")
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

func (s *RecordCleanupService) deleteOldRecords(ctx context.Context, retentionDate time.Time) error {

	priceQuery := "DELETE FROM prices WHERE recorded_at < $1"
	_, err := s.db.ExecContext(ctx, priceQuery, retentionDate)
	if err != nil {
		return fmt.Errorf("deleting old price records: %w", err)
	}

	stockQuery := "DELETE FROM stocks WHERE recorded_at < $1"
	_, err = s.db.ExecContext(ctx, stockQuery, retentionDate)
	if err != nil {
		return fmt.Errorf("deleting old stock records: %w", err)
	}

	log.Printf("Deleted records older than %s", retentionDate.Format("2006-01-02"))
	return nil
}
