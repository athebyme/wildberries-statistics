package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/lib/pq"
	"golang.org/x/time/rate"
	"log"
	"net/http"
	"sort"
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

CREATE INDEX IF NOT EXISTS idx_stocks_product_warehouse_time ON stocks(product_id, warehouse_id, recorded_at DESC);

CREATE INDEX IF NOT EXISTS idx_stocks_time_only ON stocks(recorded_at);

-- Базовые индексы для быстрого поиска
CREATE INDEX IF NOT EXISTS idx_prices_product_date_composite ON prices(product_id, recorded_at);
CREATE INDEX IF NOT EXISTS idx_stocks_product_warehouse_date_composite ON stocks(product_id, warehouse_id, recorded_at);

-- Индексы для сортировки и фильтрации
CREATE INDEX IF NOT EXISTS idx_prices_product_id_recorded_at ON prices(product_id, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_stocks_product_id_warehouse_id_recorded_at ON stocks(product_id, warehouse_id, recorded_at DESC);

-- Индекс для запросов по временным диапазонам
CREATE INDEX IF NOT EXISTS idx_prices_recorded_at_range ON prices(recorded_at);
CREATE INDEX IF NOT EXISTS idx_stocks_recorded_at_range ON stocks(recorded_at);

-- Индексы для эффективной фильтрации по складам
CREATE INDEX IF NOT EXISTS idx_stocks_warehouse_date ON stocks(warehouse_id, recorded_at);

-- Индексы для поддержки оконных функций
CREATE INDEX IF NOT EXISTS idx_prices_product_price_date ON prices(product_id, price, recorded_at);
CREATE INDEX IF NOT EXISTS idx_stocks_product_warehouse_amount_date ON stocks(product_id, warehouse_id, amount, recorded_at);


-- ALTER SYSTEM SET work_mem = '32MB';         -- Увеличить память для операций сортировки
-- ALTER SYSTEM SET maintenance_work_mem = '256MB'; -- Больше памяти для обслуживания индексов
-- ALTER SYSTEM SET random_page_cost = 1.1;    -- Оптимизировать для SSD дисков
-- ALTER SYSTEM SET effective_cache_size = '4GB'; -- Настройка с учетом объема ОЗУ сервера
-- ALTER SYSTEM SET shared_buffers = '1GB';    -- Буфер памяти для кэширования данных
-- SELECT pg_reload_conf();                    -- Перезагрузить конфигурацию


ANALYZE prices;
ANALYZE stocks;
ANALYZE products;
ANALYZE warehouses;
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

func GetBatchPricesForProductsPaginated(ctx context.Context, db *sqlx.DB, productIDs []int,
	startDate, endDate time.Time, pageSize int) (map[int][]models.PriceRecord, error) {

	if len(productIDs) == 0 {
		return make(map[int][]models.PriceRecord), nil
	}

	result := make(map[int][]models.PriceRecord)

	// Process in batches to avoid overwhelming the database
	for i := 0; i < len(productIDs); i += pageSize {
		end := i + pageSize
		if end > len(productIDs) {
			end = len(productIDs)
		}

		batchIDs := productIDs[i:end]

		query := `
            WITH daily_prices AS (
                SELECT DISTINCT ON (product_id, DATE_TRUNC('day', recorded_at))
                    id, product_id, size_id, price, discount, club_discount, final_price, 
                    club_final_price, currency_iso_code, tech_size_name, editable_size_price, recorded_at
                FROM prices
                WHERE product_id = ANY($1)
                AND recorded_at BETWEEN $2 AND $3
                ORDER BY product_id, DATE_TRUNC('day', recorded_at), recorded_at DESC
            )
            SELECT * FROM daily_prices
            ORDER BY product_id, recorded_at
        `

		startTime := time.Now()

		var prices []models.PriceRecord
		err := db.SelectContext(ctx, &prices, query, pq.Array(batchIDs), startDate, endDate)
		queryTime := time.Since(startTime)

		if err != nil {
			log.Printf("Error in paginated batch query (time: %v): %v", queryTime, err)
			return nil, fmt.Errorf("fetching batch prices: %w", err)
		}

		log.Printf("Paginated batch query completed in %v (products=%d)", queryTime, len(batchIDs))

		// Group results by product_id
		for _, price := range prices {
			result[price.ProductID] = append(result[price.ProductID], price)
		}
	}

	return result, nil
}

// Similarly for stocks
func GetBatchStocksForProductsPaginated(ctx context.Context, db *sqlx.DB, productIDs []int,
	warehouseIDs []int64, startDate, endDate time.Time, pageSize int) (map[int]map[int64][]models.StockRecord, error) {

	if len(productIDs) == 0 || len(warehouseIDs) == 0 {
		return make(map[int]map[int64][]models.StockRecord), nil
	}

	result := make(map[int]map[int64][]models.StockRecord)

	// Process products in batches
	for i := 0; i < len(productIDs); i += pageSize {
		end := i + pageSize
		if end > len(productIDs) {
			end = len(productIDs)
		}

		batchIDs := productIDs[i:end]

		query := `
            WITH daily_stocks AS (
                SELECT DISTINCT ON (product_id, warehouse_id, DATE_TRUNC('day', recorded_at))
                    id, product_id, warehouse_id, amount, recorded_at
                FROM stocks
                WHERE product_id = ANY($1)
                AND warehouse_id = ANY($2)
                AND recorded_at BETWEEN $3 AND $4
                ORDER BY product_id, warehouse_id, DATE_TRUNC('day', recorded_at), recorded_at DESC
            )
            SELECT * FROM daily_stocks
            ORDER BY product_id, warehouse_id, recorded_at
        `

		startTime := time.Now()

		var stocks []models.StockRecord
		err := db.SelectContext(ctx, &stocks, query, pq.Array(batchIDs), pq.Array(warehouseIDs), startDate, endDate)
		queryTime := time.Since(startTime)

		if err != nil {
			log.Printf("Error in paginated batch stock query (time: %v): %v", queryTime, err)
			return nil, fmt.Errorf("fetching batch stocks: %w", err)
		}

		log.Printf("Paginated batch stock query completed in %v (products=%d, warehouses=%d)",
			queryTime, len(batchIDs), len(warehouseIDs))

		// Group results by product_id and warehouse_id
		for _, stock := range stocks {
			if _, ok := result[stock.ProductID]; !ok {
				result[stock.ProductID] = make(map[int64][]models.StockRecord)
			}
			result[stock.ProductID][stock.WarehouseID] = append(result[stock.ProductID][stock.WarehouseID], stock)
		}
	}

	return result, nil
}

func GetPricesForPeriod(ctx context.Context, db *sqlx.DB, productID int, startDate, endDate time.Time) ([]models.PriceRecord, error) {
	// Оптимизированный запрос с группировкой по дням
	query := `
        WITH daily_prices AS (
            SELECT DISTINCT ON (DATE_TRUNC('day', recorded_at))
                id, product_id, size_id, price, discount, club_discount, final_price, 
                club_final_price, currency_iso_code, tech_size_name, editable_size_price, recorded_at
            FROM prices
            WHERE product_id = $1
            AND recorded_at BETWEEN $2 AND $3
            ORDER BY DATE_TRUNC('day', recorded_at), recorded_at DESC
        )
        SELECT * FROM daily_prices
        ORDER BY recorded_at
    `

	startTime := time.Now()
	prices := []models.PriceRecord{}
	err := db.SelectContext(ctx, &prices, query, productID, startDate, endDate)
	queryTime := time.Since(startTime)

	if err != nil {
		log.Printf("Ошибка запроса цен для периода (time: %v): %v", queryTime, err)
		return nil, fmt.Errorf("fetching prices for period from DB: %w", err)
	}

	log.Printf("Запрос цен для периода выполнен за %v", queryTime)
	return prices, nil
}

func GetStocksForPeriod(ctx context.Context, db *sqlx.DB, productID int, warehouseID int64, startDate, endDate time.Time) ([]models.StockRecord, error) {
	query := `
        WITH daily_stocks AS (
            SELECT DISTINCT ON (DATE_TRUNC('day', recorded_at))
                id, product_id, warehouse_id, amount, recorded_at
            FROM stocks
            WHERE product_id = $1
            AND warehouse_id = $2
            AND recorded_at BETWEEN $3 AND $4
            ORDER BY DATE_TRUNC('day', recorded_at), recorded_at DESC
        )
        SELECT * FROM daily_stocks
        ORDER BY recorded_at
    `

	startTime := time.Now()
	stocks := []models.StockRecord{}
	err := db.SelectContext(ctx, &stocks, query, productID, warehouseID, startDate, endDate)
	queryTime := time.Since(startTime)

	if err != nil {
		log.Printf("Ошибка запроса остатков для периода (time: %v): %v", queryTime, err)
		return nil, fmt.Errorf("fetching stocks for period from DB: %w", err)
	}

	log.Printf("Запрос остатков для периода выполнен за %v", queryTime)
	return stocks, nil
}

// TimeChunk represents a smaller chunk of time for processing
type TimeChunk struct {
	Start time.Time
	End   time.Time
}

func GetBatchPricesForProducts(ctx context.Context, db *sqlx.DB, productIDs []int,
	startDate, endDate time.Time) (map[int][]models.PriceRecord, error) {

	if len(productIDs) == 0 {
		return make(map[int][]models.PriceRecord), nil
	}

	// Break the date range into smaller chunks if it's large
	// For example, split into weeks or months if the range is more than a month
	timeChunks := splitDateRange(startDate, endDate, 7) // 7-day chunks

	result := make(map[int][]models.PriceRecord)

	// Process each time chunk separately
	for _, timeChunk := range timeChunks {
		log.Printf("Processing price data from %s to %s",
			timeChunk.Start.Format("2006-01-02"),
			timeChunk.End.Format("2006-01-02"))

		query := `
            WITH daily_prices AS (
                SELECT DISTINCT ON (product_id, DATE_TRUNC('day', recorded_at))
                    id, product_id, size_id, price, discount, club_discount, final_price, 
                    club_final_price, currency_iso_code, tech_size_name, editable_size_price, recorded_at
                FROM prices
                WHERE product_id = ANY($1)
                AND recorded_at BETWEEN $2 AND $3
                ORDER BY product_id, DATE_TRUNC('day', recorded_at), recorded_at DESC
            )
            SELECT * FROM daily_prices
            ORDER BY product_id, recorded_at
        `

		startTime := time.Now()

		var prices []models.PriceRecord
		err := db.SelectContext(ctx, &prices, query, pq.Array(productIDs), timeChunk.Start, timeChunk.End)

		queryTime := time.Since(startTime)

		if err != nil {
			log.Printf("Error in time-chunked query (time: %v): %v", queryTime, err)
			return nil, fmt.Errorf("fetching prices for time chunk: %w", err)
		}

		log.Printf("Time chunk query completed in %v (products=%d, records=%d)",
			queryTime, len(productIDs), len(prices))

		// Add results to the combined result map
		for _, price := range prices {
			result[price.ProductID] = append(result[price.ProductID], price)
		}
	}

	// Verify all products have data in expected time order
	for productID, prices := range result {
		if len(prices) > 1 {
			// Ensure the data is sorted by time
			sort.Slice(prices, func(i, j int) bool {
				return prices[i].RecordedAt.Before(prices[j].RecordedAt)
			})
			result[productID] = prices
		}
	}

	return result, nil
}

// splitDateRange breaks a large date range into smaller chunks
func splitDateRange(start, end time.Time, chunkSizeInDays int) []TimeChunk {
	var chunks []TimeChunk

	// If the range is small enough, just return it as a single chunk
	totalDays := int(end.Sub(start).Hours() / 24)
	if totalDays <= chunkSizeInDays {
		return []TimeChunk{{Start: start, End: end}}
	}

	// Split into chunks of the specified size
	chunkStart := start
	for chunkStart.Before(end) {
		chunkEnd := chunkStart.AddDate(0, 0, chunkSizeInDays)
		if chunkEnd.After(end) {
			chunkEnd = end
		}

		chunks = append(chunks, TimeChunk{
			Start: chunkStart,
			End:   chunkEnd,
		})

		chunkStart = chunkEnd
	}

	return chunks
}

// GetBatchStocksForProducts загружает остатки для списка продуктов и складов за период одним запросом
func GetBatchStocksForProducts(ctx context.Context, db *sqlx.DB, productIDs []int,
	warehouseIDs []int64, startDate, endDate time.Time) (map[int]map[int64][]models.StockRecord, error) {

	if len(productIDs) == 0 || len(warehouseIDs) == 0 {
		return make(map[int]map[int64][]models.StockRecord), nil
	}

	// Break the date range into smaller chunks
	timeChunks := splitDateRange(startDate, endDate, 7) // 7-day chunks

	result := make(map[int]map[int64][]models.StockRecord)

	// Process each time chunk separately
	for _, timeChunk := range timeChunks {
		log.Printf("Processing stock data from %s to %s",
			timeChunk.Start.Format("2006-01-02"),
			timeChunk.End.Format("2006-01-02"))

		query := `
            WITH daily_stocks AS (
                SELECT DISTINCT ON (product_id, warehouse_id, DATE_TRUNC('day', recorded_at))
                    id, product_id, warehouse_id, amount, recorded_at
                FROM stocks
                WHERE product_id = ANY($1)
                AND warehouse_id = ANY($2)
                AND recorded_at BETWEEN $3 AND $4
                ORDER BY product_id, warehouse_id, DATE_TRUNC('day', recorded_at), recorded_at DESC
            )
            SELECT * FROM daily_stocks
            ORDER BY product_id, warehouse_id, recorded_at
        `

		startTime := time.Now()

		var stocks []models.StockRecord
		err := db.SelectContext(ctx, &stocks, query, pq.Array(productIDs), pq.Array(warehouseIDs),
			timeChunk.Start, timeChunk.End)

		queryTime := time.Since(startTime)

		if err != nil {
			log.Printf("Error in time-chunked stock query (time: %v): %v", queryTime, err)
			return nil, fmt.Errorf("fetching stocks for time chunk: %w", err)
		}

		log.Printf("Time chunk stock query completed in %v (products=%d, warehouses=%d, records=%d)",
			queryTime, len(productIDs), len(warehouseIDs), len(stocks))

		// Add results to the combined result map
		for _, stock := range stocks {
			if _, ok := result[stock.ProductID]; !ok {
				result[stock.ProductID] = make(map[int64][]models.StockRecord)
			}
			result[stock.ProductID][stock.WarehouseID] = append(
				result[stock.ProductID][stock.WarehouseID], stock)
		}
	}

	// Ensure data is sorted by time for each product/warehouse
	for productID, warehouseMap := range result {
		for warehouseID, stocks := range warehouseMap {
			if len(stocks) > 1 {
				sort.Slice(stocks, func(i, j int) bool {
					return stocks[i].RecordedAt.Before(stocks[j].RecordedAt)
				})
				result[productID][warehouseID] = stocks
			}
		}
	}

	return result, nil
}

// GetLatestPricesForProducts fetches the most recent price records for multiple products
func GetLatestPricesForProducts(ctx context.Context, db *sqlx.DB, productIDs []int) (map[int]models.PriceRecord, error) {
	if len(productIDs) == 0 {
		return make(map[int]models.PriceRecord), nil
	}

	// Более эффективный запрос с использованием DISTINCT ON
	query := `
        SELECT DISTINCT ON (product_id)
            id, product_id, size_id, price, discount, club_discount, final_price, 
            club_final_price, currency_iso_code, tech_size_name, editable_size_price, recorded_at
        FROM prices
        WHERE product_id = ANY($1)
        ORDER BY product_id, recorded_at DESC
    `

	startTime := time.Now()

	var prices []models.PriceRecord
	err := db.SelectContext(ctx, &prices, query, pq.Array(productIDs))
	queryTime := time.Since(startTime)

	if err != nil {
		log.Printf("Ошибка запроса последних цен (time: %v): %v", queryTime, err)
		return nil, fmt.Errorf("fetching latest prices: %w", err)
	}

	// Индексируем по product_id
	result := make(map[int]models.PriceRecord)
	for _, price := range prices {
		result[price.ProductID] = price
	}

	return result, nil
}

// GetLatestStocksForProducts fetches the most recent stock records for multiple products and warehouses
func GetLatestStocksForProducts(ctx context.Context, db *sqlx.DB, productIDs []int, warehouseIDs []int64) (map[int]map[int64]models.StockRecord, error) {
	if len(productIDs) == 0 || len(warehouseIDs) == 0 {
		return make(map[int]map[int64]models.StockRecord), nil
	}

	// Более эффективный запрос с использованием DISTINCT ON
	query := `
        SELECT DISTINCT ON (product_id, warehouse_id)
            id, product_id, warehouse_id, amount, recorded_at
        FROM stocks
        WHERE product_id = ANY($1)
        AND warehouse_id = ANY($2)
        ORDER BY product_id, warehouse_id, recorded_at DESC
    `

	startTime := time.Now()

	var stocks []models.StockRecord
	err := db.SelectContext(ctx, &stocks, query, pq.Array(productIDs), pq.Array(warehouseIDs))
	queryTime := time.Since(startTime)

	if err != nil {
		log.Printf("Ошибка запроса последних остатков (time: %v): %v", queryTime, err)
		return nil, fmt.Errorf("fetching latest stocks: %w", err)
	}

	// Индексируем по product_id и warehouse_id
	result := make(map[int]map[int64]models.StockRecord)
	for _, stock := range stocks {
		if _, ok := result[stock.ProductID]; !ok {
			result[stock.ProductID] = make(map[int64]models.StockRecord)
		}
		result[stock.ProductID][stock.WarehouseID] = stock
	}

	return result, nil
}
