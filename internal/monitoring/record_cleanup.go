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

// RecordCleanupService represents a service for cleaning up and systematizing records about prices and stocks
type RecordCleanupService struct {
	db                *sqlx.DB
	cleanupInterval   time.Duration
	retentionInterval time.Duration
	hourlyDataKeeper  *HourlyDataKeeper
	workerPoolSize    int
}

// NewRecordCleanupService creates a new record cleanup service
func NewRecordCleanupService(
	db *sqlx.DB,
	cleanupInterval,
	retentionInterval time.Duration,
	hourlyDataKeeper *HourlyDataKeeper,
	workerPoolSize int,
) *RecordCleanupService {
	if workerPoolSize <= 0 {
		workerPoolSize = 5 // Default worker pool size
	}

	return &RecordCleanupService{
		db:                db,
		cleanupInterval:   cleanupInterval,
		retentionInterval: retentionInterval,
		hourlyDataKeeper:  hourlyDataKeeper,
		workerPoolSize:    workerPoolSize,
	}
}

// RunCleanupProcess starts the record cleanup process
func (s *RecordCleanupService) RunCleanupProcess(ctx context.Context) {
	ticker := time.NewTicker(s.cleanupInterval)
	defer ticker.Stop()

	log.Println("Record cleanup process started")

	// Run cleanup right away when starting
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

// CleanupRecords performs cleanup and systematization of records
func (s *RecordCleanupService) CleanupRecords(ctx context.Context) error {
	log.Println("Starting records cleanup process")

	// Get the time of the previous day
	now := time.Now()
	yesterday := now.AddDate(0, 0, -1)
	startOfYesterday := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 0, 0, 0, 0, now.Location())
	endOfYesterday := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 23, 59, 59, 999999999, now.Location())

	// Get all products
	products, err := s.getAllProducts(ctx)
	if err != nil {
		return fmt.Errorf("getting products for cleanup: %w", err)
	}

	log.Printf("Found %d products to process for cleanup", len(products))

	// Process products in batches to avoid overwhelming the database
	batchSize := 20
	var wg sync.WaitGroup
	sem := make(chan struct{}, s.workerPoolSize) // Semaphore to limit concurrent goroutines

	for i := 0; i < len(products); i += batchSize {
		end := i + batchSize
		if end > len(products) {
			end = len(products)
		}

		batch := products[i:end]
		wg.Add(1)

		go func(productBatch []models.ProductRecord) {
			defer wg.Done()
			sem <- struct{}{}        // Acquire semaphore
			defer func() { <-sem }() // Release semaphore

			for _, product := range productBatch {
				// Process product prices
				if err := s.processProductPrices(ctx, product.ID, startOfYesterday, endOfYesterday); err != nil {
					log.Printf("Error processing prices for product %d: %v", product.ID, err)
				}

				// Process product stocks
				if err := s.processProductStocks(ctx, product.ID, startOfYesterday, endOfYesterday); err != nil {
					log.Printf("Error processing stocks for product %d: %v", product.ID, err)
				}
			}
		}(batch)

		// Add a small delay between batches to reduce database load
		if end < len(products) {
			time.Sleep(50 * time.Millisecond)
		}
	}

	// Wait for all batches to complete
	wg.Wait()

	// Delete old records based on retention policy
	retentionDate := now.Add(-s.retentionInterval)
	if err := s.deleteOldRecords(ctx, retentionDate); err != nil {
		return fmt.Errorf("deleting old records: %w", err)
	}

	log.Println("Records cleanup process completed")
	return nil
}

// processProductPrices processes price records for a product
func (s *RecordCleanupService) processProductPrices(ctx context.Context, productID int, startDate, endDate time.Time) error {
	// Get all sizes for the product
	sizes, err := s.getProductSizes(ctx, productID)
	if err != nil {
		return fmt.Errorf("getting product sizes: %w", err)
	}

	if len(sizes) == 0 {
		return nil // No sizes to process
	}

	// Process each size
	for _, sizeID := range sizes {
		// Get the last known price before the start date
		lastKnownPrice, err := s.getLastKnownPriceBefore(ctx, productID, sizeID, startDate)
		if err != nil {
			log.Printf("Error getting last known price for product %d, size %d: %v",
				productID, sizeID, err)
			continue
		}

		// Get all price records for the period
		prices, err := s.getPriceRecordsForPeriod(ctx, productID, sizeID, startDate, endDate)
		if err != nil {
			log.Printf("Error getting price records for product %d, size %d: %v",
				productID, sizeID, err)
			continue
		}

		// Ensure we have hourly snapshots
		if err := s.ensureHourlyPriceSnapshots(ctx, productID, sizeID, lastKnownPrice, prices, startDate, endDate); err != nil {
			log.Printf("Error ensuring hourly price snapshots for product %d, size %d: %v",
				productID, sizeID, err)
		}
	}

	return nil
}

// ensureHourlyPriceSnapshots ensures that there are price snapshots for each hour in the period
func (s *RecordCleanupService) ensureHourlyPriceSnapshots(
	ctx context.Context,
	productID int,
	sizeID int,
	lastKnownPrice *models.PriceRecord,
	prices []models.PriceRecord,
	startDate, endDate time.Time,
) error {
	if len(prices) == 0 && lastKnownPrice == nil {
		return nil // No data to process
	}

	// Combine existing prices with last known price
	var allPrices []models.PriceRecord
	if lastKnownPrice != nil {
		// Add the last known price as the first entry
		allPrices = append(allPrices, *lastKnownPrice)
	}
	allPrices = append(allPrices, prices...)

	if len(allPrices) == 0 {
		return nil // No data after combining
	}

	// Map of timestamps to prices for quick lookup
	priceByTimestamp := make(map[time.Time]models.PriceRecord)
	for _, price := range allPrices {
		priceByTimestamp[price.RecordedAt] = price
	}

	// Generate hourly snapshots for the period
	current := startDate
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("starting transaction: %w", err)
	}
	defer tx.Rollback() // Will be ignored if committed

	// Create a duration-based ticker to roll through each hour in the period
	for current.Before(endDate) || current.Equal(endDate) {
		hourStart := time.Date(current.Year(), current.Month(), current.Day(), current.Hour(), 0, 0, 0, current.Location())
		hourEnd := hourStart.Add(time.Hour)

		// Find the latest price record before or at this hour
		var latestPrice *models.PriceRecord
		for _, price := range allPrices {
			if !price.RecordedAt.After(hourEnd) && (latestPrice == nil || price.RecordedAt.After(latestPrice.RecordedAt)) {
				priceCopy := price // Create a copy to avoid issues with loop variable
				latestPrice = &priceCopy
			}
		}

		if latestPrice != nil {
			// Check if a snapshot already exists for this hour
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
				// Create a snapshot with the latest price data
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

			// Move to the next hour
			current = hourEnd
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	return nil
}

// processProductStocks processes stock records for a product
func (s *RecordCleanupService) processProductStocks(ctx context.Context, productID int, startDate, endDate time.Time) error {
	// Get all warehouses for the product
	warehouses, err := s.getProductWarehouses(ctx, productID)
	if err != nil {
		return fmt.Errorf("getting product warehouses: %w", err)
	}

	if len(warehouses) == 0 {
		return nil // No warehouses to process
	}

	// Process each warehouse
	for _, warehouseID := range warehouses {
		// Get the last known stock before the start date
		lastKnownStock, err := s.getLastKnownStockBefore(ctx, productID, warehouseID, startDate)
		if err != nil {
			log.Printf("Error getting last known stock for product %d, warehouse %d: %v",
				productID, warehouseID, err)
			continue
		}

		// Get all stock records for the period
		stocks, err := s.getStockRecordsForPeriod(ctx, productID, warehouseID, startDate, endDate)
		if err != nil {
			log.Printf("Error getting stock records for product %d, warehouse %d: %v",
				productID, warehouseID, err)
			continue
		}

		// Ensure we have hourly snapshots
		if err := s.ensureHourlyStockSnapshots(ctx, productID, warehouseID, lastKnownStock, stocks, startDate, endDate); err != nil {
			log.Printf("Error ensuring hourly stock snapshots for product %d, warehouse %d: %v",
				productID, warehouseID, err)
		}
	}

	return nil
}

// ensureHourlyStockSnapshots ensures that there are stock snapshots for each hour in the period
func (s *RecordCleanupService) ensureHourlyStockSnapshots(
	ctx context.Context,
	productID int,
	warehouseID int64,
	lastKnownStock *models.StockRecord,
	stocks []models.StockRecord,
	startDate, endDate time.Time,
) error {
	if len(stocks) == 0 && lastKnownStock == nil {
		return nil // No data to process
	}

	// Combine existing stocks with last known stock
	var allStocks []models.StockRecord
	if lastKnownStock != nil {
		// Add the last known stock as the first entry
		allStocks = append(allStocks, *lastKnownStock)
	}
	allStocks = append(allStocks, stocks...)

	if len(allStocks) == 0 {
		return nil // No data after combining
	}

	// Generate hourly snapshots for the period
	current := startDate
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("starting transaction: %w", err)
	}
	defer tx.Rollback() // Will be ignored if committed

	// Create a duration-based ticker to roll through each hour in the period
	for current.Before(endDate) || current.Equal(endDate) {
		hourStart := time.Date(current.Year(), current.Month(), current.Day(), current.Hour(), 0, 0, 0, current.Location())
		hourEnd := hourStart.Add(time.Hour)

		// Find the latest stock record before or at this hour
		var latestStock *models.StockRecord
		for _, stock := range allStocks {
			if !stock.RecordedAt.After(hourEnd) && (latestStock == nil || stock.RecordedAt.After(latestStock.RecordedAt)) {
				stockCopy := stock // Create a copy to avoid issues with loop variable
				latestStock = &stockCopy
			}
		}

		if latestStock != nil {
			// Check if a snapshot already exists for this hour
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
				// Create a snapshot with the latest stock data
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

			// Move to the next hour
			current = hourEnd
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	return nil
}

// Missing methods from the second file that are needed in the first file

// getAllProducts returns all products from the database
func (s *RecordCleanupService) getAllProducts(ctx context.Context) ([]models.ProductRecord, error) {
	var products []models.ProductRecord
	query := "SELECT id, nm_id, vendor_code, barcode, name, created_at FROM products"

	err := s.db.SelectContext(ctx, &products, query)
	if err != nil {
		return nil, fmt.Errorf("selecting products: %w", err)
	}

	return products, nil
}

// getProductSizes returns all sizes for a product
func (s *RecordCleanupService) getProductSizes(ctx context.Context, productID int) ([]int, error) {
	var sizes []int
	query := "SELECT DISTINCT size_id FROM prices WHERE product_id = $1"

	err := s.db.SelectContext(ctx, &sizes, query, productID)
	if err != nil {
		return nil, fmt.Errorf("selecting product sizes: %w", err)
	}

	return sizes, nil
}

// getProductWarehouses returns all warehouses that have the product
func (s *RecordCleanupService) getProductWarehouses(ctx context.Context, productID int) ([]int64, error) {
	var warehouses []int64
	query := "SELECT DISTINCT warehouse_id FROM stocks WHERE product_id = $1"

	err := s.db.SelectContext(ctx, &warehouses, query, productID)
	if err != nil {
		return nil, fmt.Errorf("selecting product warehouses: %w", err)
	}

	return warehouses, nil
}

// getLastKnownPriceBefore returns the last known price before the specified date
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
			return nil, nil // No previous records
		}
		return nil, fmt.Errorf("selecting last known price: %w", err)
	}

	return &record, nil
}

// getLastKnownStockBefore returns the last known stock before the specified date
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
			return nil, nil // No previous records
		}
		return nil, fmt.Errorf("selecting last known stock: %w", err)
	}

	return &record, nil
}

// getPriceRecordsForPeriod returns all price records for a period
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

// getStockRecordsForPeriod returns all stock records for a period
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

// deleteOldRecords deletes records older than the specified date
func (s *RecordCleanupService) deleteOldRecords(ctx context.Context, retentionDate time.Time) error {
	// Delete old price records
	priceQuery := "DELETE FROM prices WHERE recorded_at < $1"
	_, err := s.db.ExecContext(ctx, priceQuery, retentionDate)
	if err != nil {
		return fmt.Errorf("deleting old price records: %w", err)
	}

	// Delete old stock records
	stockQuery := "DELETE FROM stocks WHERE recorded_at < $1"
	_, err = s.db.ExecContext(ctx, stockQuery, retentionDate)
	if err != nil {
		return fmt.Errorf("deleting old stock records: %w", err)
	}

	log.Printf("Deleted records older than %s", retentionDate.Format("2006-01-02"))
	return nil
}
