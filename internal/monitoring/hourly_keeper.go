package monitoring

import (
	"context"
	"fmt"
	"time"
	models "wbmonitoring/monitoring/internal/models"

	"github.com/jmoiron/sqlx"
)

// HourlyDataKeeper is responsible for aggregating and storing hourly data snapshots
type HourlyDataKeeper struct {
	db *sqlx.DB
}

// NewHourlyDataKeeper creates a new instance of HourlyDataKeeper
func NewHourlyDataKeeper(db *sqlx.DB) *HourlyDataKeeper {
	return &HourlyDataKeeper{
		db: db,
	}
}

// SaveHourlyPriceData saves hourly snapshots of price data
func (k *HourlyDataKeeper) SaveHourlyPriceData(ctx context.Context, productID int, sizeID int,
	date time.Time, priceRecords []models.PriceRecord) error {

	// Skip if no records
	if len(priceRecords) == 0 {
		return nil
	}

	// Find the most representative price record for the hour
	// (usually the last one in the hour, or one with most significant changes)
	var representativeRecord models.PriceRecord
	if len(priceRecords) > 0 {
		representativeRecord = priceRecords[len(priceRecords)-1] // Take the last record
	}

	// Create hourly price snapshot
	query := `
        INSERT INTO hourly_price_data 
        (product_id, size_id, price, discount, club_discount, final_price, 
         club_final_price, currency_iso_code, tech_size_name, hour_timestamp) 
        VALUES 
        ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (product_id, size_id, hour_timestamp) 
        DO UPDATE SET 
            price = EXCLUDED.price,
            discount = EXCLUDED.discount,
            club_discount = EXCLUDED.club_discount,
            final_price = EXCLUDED.final_price,
            club_final_price = EXCLUDED.club_final_price
    `

	hourTimestamp := time.Date(date.Year(), date.Month(), date.Day(), date.Hour(), 0, 0, 0, date.Location())

	_, err := k.db.ExecContext(ctx, query,
		productID,
		sizeID,
		representativeRecord.Price,
		representativeRecord.Discount,
		representativeRecord.ClubDiscount,
		representativeRecord.FinalPrice,
		representativeRecord.ClubFinalPrice,
		representativeRecord.CurrencyIsoCode,
		representativeRecord.TechSizeName,
		hourTimestamp,
	)

	if err != nil {
		return fmt.Errorf("saving hourly price data: %w", err)
	}

	return nil
}

// SaveHourlyStockData saves hourly snapshots of stock data
func (k *HourlyDataKeeper) SaveHourlyStockData(ctx context.Context, productID int,
	warehouseID int64, date time.Time, stockRecords []models.StockRecord) error {

	// Skip if no records
	if len(stockRecords) == 0 {
		return nil
	}

	// Find the most representative stock record for the hour
	var representativeRecord models.StockRecord
	if len(stockRecords) > 0 {
		representativeRecord = stockRecords[len(stockRecords)-1] // Take the last record
	}

	// Create hourly stock snapshot
	query := `
        INSERT INTO hourly_stock_data 
        (product_id, warehouse_id, amount, hour_timestamp) 
        VALUES 
        ($1, $2, $3, $4)
        ON CONFLICT (product_id, warehouse_id, hour_timestamp) 
        DO UPDATE SET 
            amount = EXCLUDED.amount
    `

	hourTimestamp := time.Date(date.Year(), date.Month(), date.Day(), date.Hour(), 0, 0, 0, date.Location())

	_, err := k.db.ExecContext(ctx, query,
		productID,
		warehouseID,
		representativeRecord.Amount,
		hourTimestamp,
	)

	if err != nil {
		return fmt.Errorf("saving hourly stock data: %w", err)
	}

	return nil
}

// DeleteOldHourlyData removes hourly data older than the retention period
func (k *HourlyDataKeeper) DeleteOldHourlyData(ctx context.Context, retentionDate time.Time) error {
	// Delete old price data
	priceQuery := "DELETE FROM hourly_price_data WHERE hour_timestamp < $1"
	_, err := k.db.ExecContext(ctx, priceQuery, retentionDate)
	if err != nil {
		return fmt.Errorf("deleting old hourly price data: %w", err)
	}

	// Delete old stock data
	stockQuery := "DELETE FROM hourly_stock_data WHERE hour_timestamp < $1"
	_, err = k.db.ExecContext(ctx, stockQuery, retentionDate)
	if err != nil {
		return fmt.Errorf("deleting old hourly stock data: %w", err)
	}

	return nil
}
