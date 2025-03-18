package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/lib/pq"
	"golang.org/x/time/rate"
	"net/http"
	"time"
	"wbmonitoring/monitoring/internal/api"
	"wbmonitoring/monitoring/internal/app_errors"
	"wbmonitoring/monitoring/internal/models"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

const schemaSQL = `
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

CREATE TABLE IF NOT EXISTS monitoring_status (
    id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL REFERENCES products(id),
    last_price_check TIMESTAMP WITH TIME ZONE,
    last_stock_check TIMESTAMP WITH TIME ZONE,
    CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES products(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_monitoring_status_product_id ON monitoring_status(product_id);

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

CREATE TABLE IF NOT EXISTS hourly_price_data (
    id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL,
    size_id INTEGER NOT NULL,
    price INTEGER NOT NULL,
    discount INTEGER NOT NULL,
    club_discount INTEGER NOT NULL,
    final_price INTEGER NOT NULL,
    club_final_price INTEGER NOT NULL,
    currency_iso_code VARCHAR(8) NOT NULL,
    tech_size_name VARCHAR(20) NOT NULL,
    hour_timestamp TIMESTAMP NOT NULL,
    UNIQUE(product_id, size_id, hour_timestamp)
);

CREATE TABLE IF NOT EXISTS hourly_stock_data (
    id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL,
    warehouse_id BIGINT NOT NULL,
    amount INTEGER NOT NULL,
    hour_timestamp TIMESTAMP NOT NULL,
    UNIQUE(product_id, warehouse_id, hour_timestamp)
);

CREATE INDEX IF NOT EXISTS idx_hourly_price_time ON hourly_price_data(hour_timestamp);
CREATE INDEX IF NOT EXISTS idx_hourly_stock_time ON hourly_stock_data(hour_timestamp);


CREATE TABLE IF NOT EXISTS warehouses(
    id SERIAL PRIMARY KEY,
    name TEXT,
    office_id BIGINT,
    warehouse_id BIGINT,
    cargo_type INTEGER,
    delivery_type INTEGER
);

CREATE INDEX IF NOT EXISTS idx_warehouses_name ON warehouses(name);
CREATE INDEX IF NOT EXISTS idx_warehouses_wid ON warehouses(warehouse_id);
CREATE INDEX IF NOT EXISTS idx_warehouses_id ON warehouses(id);


CREATE TABLE IF NOT EXISTS price_snapshots (
    id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL,
    size_id INTEGER NOT NULL,
    price INTEGER NOT NULL,
    discount INTEGER NOT NULL,
    club_discount INTEGER NOT NULL,
    final_price INTEGER NOT NULL,
    club_final_price INTEGER NOT NULL,
    currency_iso_code TEXT NOT NULL,
    tech_size_name TEXT NOT NULL,
    editable_size_price BOOLEAN NOT NULL,
    snapshot_time TIMESTAMP NOT NULL,
    FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
);


CREATE INDEX IF NOT EXISTS idx_price_snapshots_product_time ON price_snapshots(product_id, snapshot_time);


CREATE TABLE IF NOT EXISTS stock_snapshots (
    id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL,
    warehouse_id BIGINT NOT NULL,
    amount INTEGER NOT NULL,
    snapshot_time TIMESTAMP NOT NULL,
    FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE,
    FOREIGN KEY (warehouse_id) REFERENCES warehouses(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_stock_snapshots_product_warehouse_time 
    ON stock_snapshots(product_id, warehouse_id, snapshot_time);

CREATE INDEX IF NOT EXISTS idx_prices_product_date ON prices(product_id, recorded_at);

CREATE INDEX IF NOT EXISTS idx_prices_recorded_at_desc ON prices(recorded_at DESC);

CREATE INDEX IF NOT EXISTS idx_stocks_product_warehouse_date ON stocks(product_id, warehouse_id, recorded_at);

CREATE INDEX IF NOT EXISTS idx_stocks_recorded_at_desc ON stocks(recorded_at DESC);

CREATE INDEX IF NOT EXISTS idx_price_snapshots_time ON price_snapshots(snapshot_time);

CREATE INDEX IF NOT EXISTS idx_stock_snapshots_time ON stock_snapshots(snapshot_time);

CREATE INDEX IF NOT EXISTS idx_hourly_price_product_time ON hourly_price_data(product_id, hour_timestamp);

CREATE INDEX IF NOT EXISTS idx_hourly_stock_product_warehouse_time ON hourly_stock_data(product_id, warehouse_id, hour_timestamp);

ANALYZE prices;
ANALYZE stocks;
ANALYZE price_snapshots;
ANALYZE stock_snapshots;
ANALYZE hourly_price_data;
ANALYZE hourly_stock_data;
`

func InitDB(db *sqlx.DB) error {
	_, err := db.Exec(schemaSQL)
	if err != nil {
		return fmt.Errorf("initializing database schema: %w", err)
	}
	return nil
}

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

func GetProductCount(ctx context.Context, db *sqlx.DB) (int, error) {
	var count int
	err := db.GetContext(ctx, &count, "SELECT COUNT(*) FROM products")
	if err != nil {
		return 0, fmt.Errorf("getting product count from DB: %w", err)
	}
	return count, nil
}

func GetAllProducts(ctx context.Context, db *sqlx.DB) ([]models.ProductRecord, error) {
	var products []models.ProductRecord
	query := `SELECT id, nm_id, vendor_code, barcode, name, created_at FROM products`

	if err := db.SelectContext(ctx, &products, query); err != nil {
		return nil, fmt.Errorf("fetching products from DB: %w", err)
	}

	return products, nil
}

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

func UpdateWarehousesFromAPI(ctx context.Context, db *sqlx.DB, client *http.Client, apiKey string, limiter *rate.Limiter) error {
	warehouses, err := api.GetWarehouses(ctx, client, apiKey, limiter)
	if err != nil {
		return fmt.Errorf("failed to get warehouses from API: %w", err)
	}

	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback()

	for _, warehouse := range warehouses {
		existingWarehouse, err := GetWarehouseByID(ctx, tx, int(warehouse.ID))
		if err != nil && !errors.Is(err, app_errors.ErrNotFound) {
			return fmt.Errorf("failed to check if warehouse exists: %w", err)
		}

		if existingWarehouse == nil {

			_, err = tx.NamedExecContext(ctx, `
				INSERT INTO warehouses (id, name)
				VALUES (:id, :name)
			`, warehouse)
			if err != nil {
				return fmt.Errorf("failed to insert new warehouse: %w", err)
			}
		} else {

			_, err = tx.NamedExecContext(ctx, `
				UPDATE warehouses
				SET name = :name
				WHERE id = :id
			`, warehouse)
			if err != nil {
				return fmt.Errorf("failed to update warehouse: %w", err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func GetWarehouseByID(ctx context.Context, db sqlx.QueryerContext, id int) (*models.Warehouse, error) {
	var warehouse models.Warehouse
	err := sqlx.GetContext(ctx, db, &warehouse, "SELECT id, name FROM warehouses WHERE id = $1", id)
	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			return nil, app_errors.ErrNotFound
		}
		return nil, fmt.Errorf("failed to get warehouse by ID: %w", err)
	}
	return &warehouse, nil
}

func GetAllWarehouses(ctx context.Context, db *sqlx.DB) ([]models.Warehouse, error) {
	var warehouses []models.Warehouse
	err := sqlx.SelectContext(ctx, db, &warehouses, "SELECT id, name FROM warehouses")
	if err != nil {
		return nil, fmt.Errorf("failed to get all warehouses: %w", err)
	}
	return warehouses, nil
}

func GetPricesForPeriod(ctx context.Context, db *sqlx.DB, productID int, startDate, endDate time.Time) ([]models.PriceRecord, error) {
	prices := []models.PriceRecord{}
	err := db.SelectContext(ctx, &prices,
		"SELECT * FROM prices WHERE product_id = $1 AND recorded_at BETWEEN $2 AND $3 ORDER BY recorded_at",
		productID, startDate, endDate)
	if err != nil {
		return nil, fmt.Errorf("fetching prices for period from DB: %w", err)
	}
	return prices, nil
}

func GetStocksForPeriod(ctx context.Context, db *sqlx.DB, productID int, warehouseID int64, startDate, endDate time.Time) ([]models.StockRecord, error) {
	stocks := []models.StockRecord{}
	err := db.SelectContext(ctx, &stocks,
		"SELECT * FROM stocks WHERE product_id = $1 AND warehouse_id = $2 AND recorded_at BETWEEN $3 AND $4 ORDER BY recorded_at",
		productID, warehouseID, startDate, endDate)
	if err != nil {
		return nil, fmt.Errorf("fetching stocks for period from DB: %w", err)
	}
	return stocks, nil
}

// GetBatchPricesForProducts загружает цены для списка продуктов за период одним запросом
func GetBatchPricesForProducts(ctx context.Context, db *sqlx.DB, productIDs []int, startDate, endDate time.Time) (map[int][]models.PriceRecord, error) {
	// Преобразуем slice в строку для запроса
	if len(productIDs) == 0 {
		return make(map[int][]models.PriceRecord), nil
	}

	query := `
        SELECT * FROM prices 
        WHERE product_id = ANY($1) AND recorded_at BETWEEN $2 AND $3 
        ORDER BY product_id, recorded_at
    `

	// Преобразуем productIDs в pq.Array
	var prices []models.PriceRecord
	err := db.SelectContext(ctx, &prices, query, pq.Array(productIDs), startDate, endDate)
	if err != nil {
		return nil, fmt.Errorf("fetching batch prices: %w", err)
	}

	// Группируем результаты по product_id
	result := make(map[int][]models.PriceRecord)
	for _, price := range prices {
		result[price.ProductID] = append(result[price.ProductID], price)
	}

	return result, nil
}

// GetBatchStocksForProducts загружает остатки для списка продуктов и складов за период одним запросом
func GetBatchStocksForProducts(ctx context.Context, db *sqlx.DB, productIDs []int, warehouseIDs []int64, startDate, endDate time.Time) (map[int]map[int64][]models.StockRecord, error) {
	if len(productIDs) == 0 || len(warehouseIDs) == 0 {
		return make(map[int]map[int64][]models.StockRecord), nil
	}

	query := `
        SELECT * FROM stocks 
        WHERE product_id = ANY($1) AND warehouse_id = ANY($2) AND recorded_at BETWEEN $3 AND $4
        ORDER BY product_id, warehouse_id, recorded_at
    `

	var stocks []models.StockRecord
	err := db.SelectContext(ctx, &stocks, query, pq.Array(productIDs), pq.Array(warehouseIDs), startDate, endDate)
	if err != nil {
		return nil, fmt.Errorf("fetching batch stocks: %w", err)
	}

	// Группируем результаты по product_id и warehouse_id
	result := make(map[int]map[int64][]models.StockRecord)
	for _, stock := range stocks {
		if _, ok := result[stock.ProductID]; !ok {
			result[stock.ProductID] = make(map[int64][]models.StockRecord)
		}
		result[stock.ProductID][stock.WarehouseID] = append(result[stock.ProductID][stock.WarehouseID], stock)
	}

	return result, nil
}

// GetLatestPricesForProducts получает последние цены для списка продуктов
func GetLatestPricesForProducts(ctx context.Context, db *sqlx.DB, productIDs []int) (map[int]models.PriceRecord, error) {
	if len(productIDs) == 0 {
		return make(map[int]models.PriceRecord), nil
	}

	// Используем оконную функцию для получения последних цен
	query := `
        WITH ranked_prices AS (
            SELECT 
                p.*,
                ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY recorded_at DESC) as rn
            FROM prices p
            WHERE product_id = ANY($1)
        )
        SELECT * FROM ranked_prices WHERE rn = 1
    `

	var prices []models.PriceRecord
	err := db.SelectContext(ctx, &prices, query, pq.Array(productIDs))
	if err != nil {
		return nil, fmt.Errorf("fetching latest prices: %w", err)
	}

	result := make(map[int]models.PriceRecord)
	for _, price := range prices {
		result[price.ProductID] = price
	}

	return result, nil
}

// GetLatestStocksForProducts получает последние остатки для списка продуктов и складов
func GetLatestStocksForProducts(ctx context.Context, db *sqlx.DB, productIDs []int, warehouseIDs []int64) (map[int]map[int64]models.StockRecord, error) {
	if len(productIDs) == 0 || len(warehouseIDs) == 0 {
		return make(map[int]map[int64]models.StockRecord), nil
	}

	// Используем оконную функцию для получения последних остатков
	query := `
        WITH ranked_stocks AS (
            SELECT 
                s.*,
                ROW_NUMBER() OVER (PARTITION BY product_id, warehouse_id ORDER BY recorded_at DESC) as rn
            FROM stocks s
            WHERE product_id = ANY($1) AND warehouse_id = ANY($2)
        )
        SELECT * FROM ranked_stocks WHERE rn = 1
    `

	var stocks []models.StockRecord
	err := db.SelectContext(ctx, &stocks, query, pq.Array(productIDs), pq.Array(warehouseIDs))
	if err != nil {
		return nil, fmt.Errorf("fetching latest stocks: %w", err)
	}

	result := make(map[int]map[int64]models.StockRecord)
	for _, stock := range stocks {
		if _, ok := result[stock.ProductID]; !ok {
			result[stock.ProductID] = make(map[int64]models.StockRecord)
		}
		result[stock.ProductID][stock.WarehouseID] = stock
	}

	return result, nil
}
