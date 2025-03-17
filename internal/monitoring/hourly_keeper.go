package monitoring

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
	"wbmonitoring/monitoring/internal/db"
	"wbmonitoring/monitoring/internal/models"

	"github.com/jmoiron/sqlx"
)

// HourlyDataKeeper is responsible for taking and storing hourly snapshots of prices and stocks
type HourlyDataKeeper struct {
	db                *sqlx.DB
	snapshotInterval  time.Duration
	retentionPeriod   time.Duration
	lastSnapshotTaken time.Time
	workerPoolSize    int
}

// NewHourlyDataKeeper creates a new HourlyDataKeeper service
func NewHourlyDataKeeper(
	database *sqlx.DB,
	snapshotInterval time.Duration,
	retentionPeriod time.Duration,
	workerPoolSize int,
) *HourlyDataKeeper {
	if workerPoolSize <= 0 {
		workerPoolSize = 5 // Default worker pool size
	}

	return &HourlyDataKeeper{
		db:               database,
		snapshotInterval: snapshotInterval,
		retentionPeriod:  retentionPeriod,
		workerPoolSize:   workerPoolSize,
	}
}

// RunHourlySnapshots starts the hourly snapshot process
func (h *HourlyDataKeeper) RunHourlySnapshots(ctx context.Context) error {
	// Calculate time until next full hour
	now := time.Now()
	nextHour := time.Date(now.Year(), now.Month(), now.Day(), now.Hour()+1, 0, 0, 0, now.Location())
	initialDelay := nextHour.Sub(now)

	log.Printf("Hourly snapshots scheduled to start at %s (in %s)", nextHour.Format("15:04:05"), initialDelay)

	// Wait until the next full hour to start
	timer := time.NewTimer(initialDelay)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			// Take snapshots
			if err := h.TakeSnapshots(ctx); err != nil {
				log.Printf("Error taking hourly snapshots: %v", err)
			}

			// Clean up old snapshots
			if err := h.CleanupOldSnapshots(ctx); err != nil {
				log.Printf("Error cleaning up old snapshots: %v", err)
			}

			// Reset timer for next snapshot (typically 1 hour)
			timer.Reset(h.snapshotInterval)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// TakeSnapshots takes hourly snapshots of prices and stocks
func (h *HourlyDataKeeper) TakeSnapshots(ctx context.Context) error {
	log.Println("Taking hourly data snapshots")

	// Record the snapshot time
	snapshotTime := time.Now()
	h.lastSnapshotTaken = snapshotTime

	// Get all products and warehouses concurrently
	var wg sync.WaitGroup
	var productsMu, warehousesMu sync.Mutex
	var products []models.ProductRecord
	var warehouses []models.Warehouse
	var productsErr, warehousesErr error

	wg.Add(2)

	// Fetch products asynchronously
	go func() {
		defer wg.Done()
		prods, err := db.GetAllProducts(ctx, h.db)
		productsMu.Lock()
		products = prods
		productsErr = err
		productsMu.Unlock()
	}()

	// Fetch warehouses asynchronously
	go func() {
		defer wg.Done()
		whs, err := db.GetAllWarehouses(ctx, h.db)
		warehousesMu.Lock()
		warehouses = whs
		warehousesErr = err
		warehousesMu.Unlock()
	}()

	wg.Wait()

	if productsErr != nil {
		return fmt.Errorf("failed to fetch products: %w", productsErr)
	}

	if warehousesErr != nil {
		return fmt.Errorf("failed to fetch warehouses: %w", warehousesErr)
	}

	log.Printf("Taking snapshots for %d products across %d warehouses", len(products), len(warehouses))

	// Take price snapshots in parallel
	if err := h.takePriceSnapshots(ctx, products, snapshotTime); err != nil {
		log.Printf("Error taking price snapshots: %v", err)
	}

	// Take stock snapshots in parallel
	if err := h.takeStockSnapshots(ctx, products, warehouses, snapshotTime); err != nil {
		log.Printf("Error taking stock snapshots: %v", err)
	}

	log.Printf("Hourly snapshots completed at %s", snapshotTime.Format("2006-01-02 15:04:05"))
	return nil
}

// takePriceSnapshots takes snapshots of the current prices for all products in parallel
func (h *HourlyDataKeeper) takePriceSnapshots(ctx context.Context, products []models.ProductRecord, snapshotTime time.Time) error {
	if len(products) == 0 {
		return nil
	}

	log.Printf("Taking price snapshots for %d products", len(products))

	// Create worker pool for parallelization
	productCh := make(chan models.ProductRecord, len(products))
	errCh := make(chan error, len(products))
	var wg sync.WaitGroup

	// Limit concurrency to avoid overwhelming the database
	workers := h.workerPoolSize
	if workers > len(products) {
		workers = len(products)
	}

	// Start worker goroutines
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for product := range productCh {
				if err := h.processPriceSnapshot(ctx, product, snapshotTime); err != nil {
					errCh <- fmt.Errorf("error processing price snapshot for product %d: %w", product.ID, err)
				}
			}
		}()
	}

	// Send products to workers
	for _, product := range products {
		productCh <- product
	}
	close(productCh)

	// Wait for all workers to finish
	wg.Wait()
	close(errCh)

	// Collect errors
	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return fmt.Errorf("encountered %d errors while taking price snapshots", len(errs))
	}

	return nil
}

// processPriceSnapshot processes a price snapshot for a single product
func (h *HourlyDataKeeper) processPriceSnapshot(ctx context.Context, product models.ProductRecord, snapshotTime time.Time) error {
	// Get latest price record for this product
	price, err := db.GetLastPrice(ctx, h.db, product.ID)
	if err != nil {
		return fmt.Errorf("getting last price for product %d: %w", product.ID, err)
	}

	// Skip if no price data available
	if price == nil {
		return nil
	}

	// Create snapshot record
	priceSnapshot := &models.PriceSnapshot{
		ProductID:         price.ProductID,
		SizeID:            price.SizeID,
		Price:             price.Price,
		Discount:          price.Discount,
		ClubDiscount:      price.ClubDiscount,
		FinalPrice:        price.FinalPrice,
		ClubFinalPrice:    price.ClubFinalPrice,
		CurrencyIsoCode:   price.CurrencyIsoCode,
		TechSizeName:      price.TechSizeName,
		EditableSizePrice: price.EditableSizePrice,
		SnapshotTime:      snapshotTime,
	}

	// Save snapshot
	if err := h.savePriceSnapshot(ctx, priceSnapshot); err != nil {
		return fmt.Errorf("saving price snapshot for product %d: %w", product.ID, err)
	}

	return nil
}

// takeStockSnapshots takes snapshots of the current stock for all products and warehouses
func (h *HourlyDataKeeper) takeStockSnapshots(ctx context.Context, products []models.ProductRecord, warehouses []models.Warehouse, snapshotTime time.Time) error {
	if len(products) == 0 || len(warehouses) == 0 {
		return nil
	}

	log.Printf("Taking stock snapshots for %d products across %d warehouses", len(products), len(warehouses))

	// Create channels for work and errors
	type workItem struct {
		Product   models.ProductRecord
		Warehouse models.Warehouse
	}

	// Calculate total number of combinations
	total := len(products) * len(warehouses)
	workCh := make(chan workItem, total)
	errCh := make(chan error, total)
	var wg sync.WaitGroup

	// Limit concurrency to avoid overwhelming the database
	workers := h.workerPoolSize
	if workers > 20 {
		workers = 20 // Cap at reasonable amount
	}

	// Start worker goroutines
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for work := range workCh {
				if err := h.processStockSnapshot(ctx, work.Product, work.Warehouse, snapshotTime); err != nil {
					errCh <- fmt.Errorf("error processing stock snapshot for product %d, warehouse %d: %w",
						work.Product.ID, work.Warehouse.ID, err)
				}
			}
		}()
	}

	// Send work to workers
	for _, product := range products {
		for _, warehouse := range warehouses {
			workCh <- workItem{Product: product, Warehouse: warehouse}
		}
	}
	close(workCh)

	// Wait for all workers to finish
	wg.Wait()
	close(errCh)

	// Collect errors
	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return fmt.Errorf("encountered %d errors while taking stock snapshots", len(errs))
	}

	return nil
}

// processStockSnapshot processes a stock snapshot for a single product-warehouse combination
func (h *HourlyDataKeeper) processStockSnapshot(ctx context.Context, product models.ProductRecord, warehouse models.Warehouse, snapshotTime time.Time) error {
	// Get latest stock record for this product and warehouse
	stock, err := db.GetLastStock(ctx, h.db, product.ID, warehouse.ID)
	if err != nil {
		return fmt.Errorf("getting last stock for product %d, warehouse %d: %w", product.ID, warehouse.ID, err)
	}

	// Skip if no stock data available
	if stock == nil {
		return nil
	}

	// Create snapshot record
	stockSnapshot := &models.StockSnapshot{
		ProductID:    stock.ProductID,
		WarehouseID:  stock.WarehouseID,
		Amount:       stock.Amount,
		SnapshotTime: snapshotTime,
	}

	// Save snapshot
	if err := h.saveStockSnapshot(ctx, stockSnapshot); err != nil {
		return fmt.Errorf("saving stock snapshot for product %d, warehouse %d: %w", product.ID, warehouse.ID, err)
	}

	return nil
}

// savePriceSnapshot saves a price snapshot record to the database with transaction
func (h *HourlyDataKeeper) savePriceSnapshot(ctx context.Context, snapshot *models.PriceSnapshot) error {
	tx, err := h.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("starting transaction: %w", err)
	}
	defer tx.Rollback() // Will be ignored if tx.Commit() is called

	// Check if snapshot already exists for this hour
	var count int
	err = tx.QueryRowxContext(ctx, `
		SELECT COUNT(*) FROM price_snapshots 
		WHERE product_id = $1 AND size_id = $2 
		AND snapshot_time = $3
	`, snapshot.ProductID, snapshot.SizeID, snapshot.SnapshotTime).Scan(&count)

	if err != nil {
		return fmt.Errorf("checking existing price snapshot: %w", err)
	}

	if count > 0 {
		// Snapshot already exists, update it if necessary
		_, err = tx.ExecContext(ctx, `
			UPDATE price_snapshots SET
				price = $1,
				discount = $2,
				club_discount = $3, 
				final_price = $4,
				club_final_price = $5,
				currency_iso_code = $6, 
				tech_size_name = $7,
				editable_size_price = $8
			WHERE product_id = $9 AND size_id = $10 AND snapshot_time = $11
		`, snapshot.Price, snapshot.Discount, snapshot.ClubDiscount,
			snapshot.FinalPrice, snapshot.ClubFinalPrice, snapshot.CurrencyIsoCode,
			snapshot.TechSizeName, snapshot.EditableSizePrice,
			snapshot.ProductID, snapshot.SizeID, snapshot.SnapshotTime)
	} else {
		// Insert new snapshot
		_, err = tx.ExecContext(ctx, `
			INSERT INTO price_snapshots (
				product_id, size_id, price, discount, club_discount, 
				final_price, club_final_price, currency_iso_code, 
				tech_size_name, editable_size_price, snapshot_time
			) VALUES (
				$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
			)
		`, snapshot.ProductID, snapshot.SizeID, snapshot.Price,
			snapshot.Discount, snapshot.ClubDiscount, snapshot.FinalPrice,
			snapshot.ClubFinalPrice, snapshot.CurrencyIsoCode,
			snapshot.TechSizeName, snapshot.EditableSizePrice, snapshot.SnapshotTime)
	}

	if err != nil {
		return fmt.Errorf("saving price snapshot: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	return nil
}

// saveStockSnapshot saves a stock snapshot record to the database with transaction
func (h *HourlyDataKeeper) saveStockSnapshot(ctx context.Context, snapshot *models.StockSnapshot) error {
	tx, err := h.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("starting transaction: %w", err)
	}
	defer tx.Rollback() // Will be ignored if tx.Commit() is called

	// Check if snapshot already exists for this hour
	var count int
	err = tx.QueryRowxContext(ctx, `
		SELECT COUNT(*) FROM stock_snapshots 
		WHERE product_id = $1 AND warehouse_id = $2 
		AND snapshot_time = $3
	`, snapshot.ProductID, snapshot.WarehouseID, snapshot.SnapshotTime).Scan(&count)

	if err != nil {
		return fmt.Errorf("checking existing stock snapshot: %w", err)
	}

	if count > 0 {
		// Snapshot already exists, update it if necessary
		_, err = tx.ExecContext(ctx, `
			UPDATE stock_snapshots SET
				amount = $1
			WHERE product_id = $2 AND warehouse_id = $3 AND snapshot_time = $4
		`, snapshot.Amount, snapshot.ProductID, snapshot.WarehouseID, snapshot.SnapshotTime)
	} else {
		// Insert new snapshot
		_, err = tx.ExecContext(ctx, `
			INSERT INTO stock_snapshots (
				product_id, warehouse_id, amount, snapshot_time
			) VALUES (
				$1, $2, $3, $4
			)
		`, snapshot.ProductID, snapshot.WarehouseID, snapshot.Amount, snapshot.SnapshotTime)
	}

	if err != nil {
		return fmt.Errorf("saving stock snapshot: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	return nil
}

// CleanupOldSnapshots removes snapshot data older than the retention period
func (h *HourlyDataKeeper) CleanupOldSnapshots(ctx context.Context) error {
	cutoffTime := time.Now().Add(-h.retentionPeriod)
	log.Printf("Cleaning up snapshots older than %s", cutoffTime.Format("2006-01-02 15:04:05"))

	tx, err := h.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("starting transaction: %w", err)
	}
	defer tx.Rollback() // Will be ignored if tx.Commit() is called

	// Remove old price snapshots
	result, err := tx.ExecContext(ctx, `
		DELETE FROM price_snapshots WHERE snapshot_time < $1
	`, cutoffTime)
	if err != nil {
		return fmt.Errorf("deleting old price snapshots: %w", err)
	}

	priceRows, _ := result.RowsAffected()

	// Remove old stock snapshots
	result, err = tx.ExecContext(ctx, `
		DELETE FROM stock_snapshots WHERE snapshot_time < $1
	`, cutoffTime)
	if err != nil {
		return fmt.Errorf("deleting old stock snapshots: %w", err)
	}

	stockRows, _ := result.RowsAffected()

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	log.Printf("Deleted %d price snapshots and %d stock snapshots older than %s",
		priceRows, stockRows, cutoffTime.Format("2006-01-02"))

	return nil
}

// GetPriceSnapshotsForPeriod gets all price snapshots for a product within a time period
func (h *HourlyDataKeeper) GetPriceSnapshotsForPeriod(
	ctx context.Context,
	productID int,
	startTime, endTime time.Time,
) ([]models.PriceSnapshot, error) {
	var snapshots []models.PriceSnapshot

	err := h.db.SelectContext(ctx, &snapshots, `
		SELECT 
			id, product_id, size_id, price, discount, club_discount, 
			final_price, club_final_price, currency_iso_code, 
			tech_size_name, editable_size_price, snapshot_time
		FROM price_snapshots
		WHERE product_id = $1 AND snapshot_time BETWEEN $2 AND $3
		ORDER BY snapshot_time ASC
	`, productID, startTime, endTime)

	if err != nil {
		return nil, fmt.Errorf("fetching price snapshots: %w", err)
	}

	return snapshots, nil
}

// GetStockSnapshotsForPeriod gets all stock snapshots for a product within a time period
func (h *HourlyDataKeeper) GetStockSnapshotsForPeriod(
	ctx context.Context,
	productID int,
	warehouseID int64,
	startTime, endTime time.Time,
) ([]models.StockSnapshot, error) {
	var snapshots []models.StockSnapshot

	err := h.db.SelectContext(ctx, &snapshots, `
		SELECT 
			id, product_id, warehouse_id, amount, snapshot_time
		FROM stock_snapshots
		WHERE product_id = $1 AND warehouse_id = $2 AND snapshot_time BETWEEN $3 AND $4
		ORDER BY snapshot_time ASC
	`, productID, warehouseID, startTime, endTime)

	if err != nil {
		return nil, fmt.Errorf("fetching stock snapshots: %w", err)
	}

	return snapshots, nil
}

// GetLastSnapshotTime returns the time of the last snapshot taken
func (h *HourlyDataKeeper) GetLastSnapshotTime() time.Time {
	return h.lastSnapshotTaken
}
