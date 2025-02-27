package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"wbmonitoring/monitoring/internal/models"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

// SQL-скрипт для создания схемы БД
const schemaSQL = `
-- Modify the existing schema
CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    nm_id INTEGER NOT NULL UNIQUE,
    vendor_code VARCHAR(50) NOT NULL,
    barcode VARCHAR(50) NOT NULL,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_products_nm_id ON products(nm_id);
CREATE INDEX IF NOT EXISTS idx_products_barcode ON products(barcode);

-- Create a new table for monitoring status
CREATE TABLE IF NOT EXISTS monitoring_status (
    id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL REFERENCES products(id),
    last_price_check TIMESTAMP WITH TIME ZONE,
    last_stock_check TIMESTAMP WITH TIME ZONE,
    CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES products(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_monitoring_status_product_id ON monitoring_status(product_id);

-- Keep existing tables
CREATE TABLE IF NOT EXISTS prices (
    id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL REFERENCES products(id),
    size_id BIGINT,
    price INTEGER NOT NULL,
    discount INTEGER NOT NULL,
    club_discount INTEGER NOT NULL,
    final_price INTEGER NOT NULL,
    club_final_price INTEGER,
    currency_iso_code VARCHAR(10),
    tech_size_name VARCHAR(20),
    editable_size_price BOOLEAN,
    recorded_at TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES products(id)
);

CREATE INDEX IF NOT EXISTS idx_prices_product_id ON prices(product_id);
CREATE INDEX IF NOT EXISTS idx_prices_recorded_at ON prices(recorded_at);

CREATE TABLE IF NOT EXISTS stocks (
    id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL REFERENCES products(id),
    warehouse_id BIGINT NOT NULL,
    amount INTEGER NOT NULL,
    recorded_at TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES products(id)
);

CREATE INDEX IF NOT EXISTS idx_stocks_product_id ON stocks(product_id);
CREATE INDEX IF NOT EXISTS idx_stocks_warehouse_id ON stocks(warehouse_id);
CREATE INDEX IF NOT EXISTS idx_stocks_recorded_at ON stocks(recorded_at);
`

// InitDB initializes the database schema.
func InitDB(db *sqlx.DB) error {
	_, err := db.Exec(schemaSQL)
	if err != nil {
		return fmt.Errorf("initializing database schema: %w", err)
	}
	return nil
}

// GetLastPrice retrieves the last price record from the database.
func GetLastPrice(ctx context.Context, db *sqlx.DB, productID int) (*models.PriceRecord, error) {
	var price models.PriceRecord
	err := db.GetContext(ctx, &price,
		"SELECT * FROM prices WHERE product_id = $1 ORDER BY recorded_at DESC LIMIT 1",
		productID)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("fetching last price from DB: %w", err)
	}

	return &price, nil
}

// GetLastStock retrieves the last stock record from the database.
func GetLastStock(ctx context.Context, db *sqlx.DB, productID int, warehouseID int64) (*models.StockRecord, error) {
	var stock models.StockRecord
	err := db.GetContext(ctx, &stock,
		"SELECT * FROM stocks WHERE product_id = $1 AND warehouse_id = $2 ORDER BY recorded_at DESC LIMIT 1",
		productID, warehouseID)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("fetching last stock from DB: %w", err)
	}

	return &stock, nil
}

// SavePrice saves a price record to the database.
func SavePrice(ctx context.Context, db *sqlx.DB, price *models.PriceRecord) error {
	query := `
		INSERT INTO prices (product_id, size_id, price, discount, club_discount, final_price, club_final_price, currency_iso_code, tech_size_name, editable_size_price, recorded_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		RETURNING id
	`

	row := db.QueryRowContext(ctx, query,
		price.ProductID, price.SizeID, price.Price, price.Discount, price.ClubDiscount,
		price.FinalPrice, price.ClubFinalPrice, price.CurrencyIsoCode, price.TechSizeName, price.EditableSizePrice, price.RecordedAt)

	return row.Scan(&price.ProductID)
}

// SaveStock saves a stock record to the database.
func SaveStock(ctx context.Context, db *sqlx.DB, stock *models.StockRecord) error {
	query := `
		INSERT INTO stocks (product_id, warehouse_id, amount, recorded_at)
		VALUES ($1, $2, $3, $4)
		RETURNING id
	`

	row := db.QueryRowContext(ctx, query,
		stock.ProductID, stock.WarehouseID, stock.Amount, stock.RecordedAt)

	return row.Scan(&stock.ProductID)
}

// GetProductCount retrieves the count of products in the database.
func GetProductCount(ctx context.Context, db *sqlx.DB) (int, error) {
	var count int
	err := db.GetContext(ctx, &count, "SELECT COUNT(*) FROM products")
	if err != nil {
		return 0, fmt.Errorf("getting product count from DB: %w", err)
	}
	return count, nil
}

// GetAllProducts retrieves all products from the database.
func GetAllProducts(ctx context.Context, db *sqlx.DB) ([]models.ProductRecord, error) {
	var products []models.ProductRecord
	query := `SELECT id, nm_id, vendor_code, barcode, name, created_at FROM products`

	if err := db.SelectContext(ctx, &products, query); err != nil {
		return nil, fmt.Errorf("fetching products from DB: %w", err)
	}

	return products, nil
}

// UpdatePriceCheckStatus updates the last price check status in the database.
func UpdatePriceCheckStatus(ctx context.Context, db *sqlx.DB, productID int) error {
	query := `
        INSERT INTO monitoring_status (product_id, last_price_check)
        VALUES ($1, NOW())
        ON CONFLICT (product_id)
        DO UPDATE SET last_price_check = NOW()
    `

	_, err := db.ExecContext(ctx, query, productID)
	if err != nil {
		return fmt.Errorf("updating price check status: %w", err)
	}

	return nil
}

// UpdateStockCheckStatus updates the last stock check status in the database.
func UpdateStockCheckStatus(ctx context.Context, db *sqlx.DB, productID int) error {
	query := `
        INSERT INTO monitoring_status (product_id, last_stock_check)
        VALUES ($1, NOW())
        ON CONFLICT (product_id)
        DO UPDATE SET last_stock_check = NOW()
    `

	_, err := db.ExecContext(ctx, query, productID)
	if err != nil {
		return fmt.Errorf("updating stock check status: %w", err)
	}

	return nil
}
