package monitoring

import (
	"context"
	"fmt"
	"log"
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
}

// NewHourlyDataKeeper creates a new HourlyDataKeeper service
func NewHourlyDataKeeper(
	database *sqlx.DB,
	snapshotInterval time.Duration,
	retentionPeriod time.Duration,
) *HourlyDataKeeper {
	return &HourlyDataKeeper{
		db:               database,
		snapshotInterval: snapshotInterval,
		retentionPeriod:  retentionPeriod,
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

			// Reset timer for next snapshot
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

	// Take price snapshots
	if err := h.takePriceSnapshots(ctx, snapshotTime); err != nil {
		return fmt.Errorf("taking price snapshots: %w", err)
	}

	// Take stock snapshots
	if err := h.takeStockSnapshots(ctx, snapshotTime); err != nil {
		return fmt.Errorf("taking stock snapshots: %w", err)
	}

	log.Printf("Hourly snapshots completed at %s", snapshotTime.Format("2006-01-02 15:04:05"))
	return nil
}

// takePriceSnapshots takes snapshots of the current prices for all products
func (h *HourlyDataKeeper) takePriceSnapshots(ctx context.Context, snapshotTime time.Time) error {
	// Get all products
	var products []models.ProductRecord
	if err := h.db.SelectContext(ctx, &products, `SELECT id, nm_id, vendor_code FROM products`); err != nil {
		return fmt.Errorf("fetching products for price snapshot: %w", err)
	}

	log.Printf("Taking price snapshots for %d products", len(products))

	// For each product, get the latest price and save as snapshot
	for _, product := range products {
		// Get latest price record for this product
		price, err := db.GetLastPrice(ctx, h.db, product.ID)
		if err != nil {
			log.Printf("Error getting last price for product %d: %v", product.ID, err)
			continue
		}

		// Skip if no price data available
		if price == nil {
			continue
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
			log.Printf("Error saving price snapshot for product %d: %v", product.ID, err)
		}
	}

	return nil
}

// takeStockSnapshots takes snapshots of the current stock for all products across all warehouses
func (h *HourlyDataKeeper) takeStockSnapshots(ctx context.Context, snapshotTime time.Time) error {
	// Get all products
	var products []models.ProductRecord
	if err := h.db.SelectContext(ctx, &products, `SELECT id, nm_id, vendor_code FROM products`); err != nil {
		return fmt.Errorf("fetching products for stock snapshot: %w", err)
	}

	// Get all warehouses
	var warehouses []models.Warehouse
	if err := h.db.SelectContext(ctx, &warehouses, `SELECT id, name FROM warehouses`); err != nil {
		return fmt.Errorf("fetching warehouses for stock snapshot: %w", err)
	}

	log.Printf("Taking stock snapshots for %d products across %d warehouses", len(products), len(warehouses))

	// For each product and warehouse combination, get the latest stock and save as snapshot
	for _, product := range products {
		for _, warehouse := range warehouses {
			// Get latest stock record for this product and warehouse
			stock, err := db.GetLastStock(ctx, h.db, product.ID, warehouse.ID)
			if err != nil {
				log.Printf("Error getting last stock for product %d, warehouse %d: %v",
					product.ID, warehouse.ID, err)
				continue
			}

			// Skip if no stock data available
			if stock == nil {
				continue
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
				log.Printf("Error saving stock snapshot for product %d, warehouse %d: %v",
					product.ID, warehouse.ID, err)
			}
		}
	}

	return nil
}

// savePriceSnapshot saves a price snapshot record to the database
func (h *HourlyDataKeeper) savePriceSnapshot(ctx context.Context, snapshot *models.PriceSnapshot) error {
	_, err := h.db.ExecContext(ctx, `
		INSERT INTO price_snapshots (
			product_id, size_id, price, discount, club_discount, 
			final_price, club_final_price, currency_iso_code, 
			tech_size_name, editable_size_price, snapshot_time
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
		)
	`,
		snapshot.ProductID, snapshot.SizeID, snapshot.Price,
		snapshot.Discount, snapshot.ClubDiscount, snapshot.FinalPrice,
		snapshot.ClubFinalPrice, snapshot.CurrencyIsoCode,
		snapshot.TechSizeName, snapshot.EditableSizePrice, snapshot.SnapshotTime,
	)

	if err != nil {
		return fmt.Errorf("inserting price snapshot: %w", err)
	}

	return nil
}

// saveStockSnapshot saves a stock snapshot record to the database
func (h *HourlyDataKeeper) saveStockSnapshot(ctx context.Context, snapshot *models.StockSnapshot) error {
	_, err := h.db.ExecContext(ctx, `
		INSERT INTO stock_snapshots (
			product_id, warehouse_id, amount, snapshot_time
		) VALUES (
			$1, $2, $3, $4
		)
	`,
		snapshot.ProductID, snapshot.WarehouseID, snapshot.Amount, snapshot.SnapshotTime,
	)

	if err != nil {
		return fmt.Errorf("inserting stock snapshot: %w", err)
	}

	return nil
}

// CleanupOldSnapshots removes snapshot data older than the retention period
func (h *HourlyDataKeeper) CleanupOldSnapshots(ctx context.Context) error {
	cutoffTime := time.Now().Add(-h.retentionPeriod)
	log.Printf("Cleaning up snapshots older than %s", cutoffTime.Format("2006-01-02 15:04:05"))

	// Remove old price snapshots
	if _, err := h.db.ExecContext(ctx, `
		DELETE FROM price_snapshots WHERE snapshot_time < $1
	`, cutoffTime); err != nil {
		return fmt.Errorf("deleting old price snapshots: %w", err)
	}

	// Remove old stock snapshots
	if _, err := h.db.ExecContext(ctx, `
		DELETE FROM stock_snapshots WHERE snapshot_time < $1
	`, cutoffTime); err != nil {
		return fmt.Errorf("deleting old stock snapshots: %w", err)
	}

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
			product_id, size_id, price, discount, club_discount, 
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
			product_id, warehouse_id, amount, snapshot_time
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
