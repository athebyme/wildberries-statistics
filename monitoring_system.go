package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-telegram-bot-api/telegram-bot-api"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"golang.org/x/time/rate"
)

// Конфигурация сервиса
const (
	WorkerCount          = 5
	MaxRetries           = 3
	RetryInterval        = 2 * time.Second
	RequestTimeout       = 100 * time.Second
	MonitoringInterval   = 5 * time.Minute
	PriceChangeThreshold = 30 // процент изменения цены для уведомления
	StockChangeThreshold = 50 // процент изменения количества для уведомления

	ProductUpdateInterval = 5 * time.Hour // Интервал обновления продуктов
)

var (
	ErrNoMoreData         = errors.New("no more data to process")
	ErrRateLimiter        = errors.New("rate limiter error")
	ErrConnectionAborted  = errors.New("connection aborted")
	ErrTotalLimitReached  = errors.New("total limit reached")
	ErrContextCanceled    = errors.New("context canceled")
	ErrFailedAfterRetries = errors.New("failed after retries")
)

// Конфигурация сервиса
type Config struct {
	WorkerCount           int
	MaxRetries            int
	RetryInterval         time.Duration
	RequestTimeout        time.Duration
	MonitoringInterval    time.Duration
	ProductUpdateInterval time.Duration
	ApiKey                string
	TelegramToken         string
	TelegramChatID        int64
	WarehouseID           int64
	PGConnString          string
	PriceThreshold        float64
	StockThreshold        float64
}

// Структура для мониторинга
type MonitoringService struct {
	db               *sqlx.DB
	config           Config
	pricesLimiter    *rate.Limiter
	stocksLimiter    *rate.Limiter
	warehouseLimiter *rate.Limiter
	bot              *tgbotapi.BotAPI
	searchEngine     *SearchEngine // Добавляем SearchEngine
}

// Создание нового сервиса мониторинга
func NewMonitoringService(config Config) (*MonitoringService, error) {
	db, err := sqlx.Connect("postgres", config.PGConnString)
	if err != nil {
		return nil, fmt.Errorf("connecting to database: %w", err)
	}

	bot, err := tgbotapi.NewBotAPI(config.TelegramToken)
	if err != nil {
		return nil, fmt.Errorf("initializing telegram bot: %w", err)
	}

	searchConfig := SearchEngineConfig{ // Используем отдельную конфиг для SearchEngine
		WorkerCount:    config.WorkerCount,
		MaxRetries:     config.MaxRetries,
		RetryInterval:  config.RetryInterval,
		RequestTimeout: config.RequestTimeout,
		ApiKey:         config.ApiKey, // Передаем API ключ
	}
	searchEngine := NewSearchEngine(db.DB, os.Stdout, searchConfig) // Используем db.DB, так как SearchEngine ожидает *sql.DB

	return &MonitoringService{
		db:               db,
		config:           config,
		pricesLimiter:    rate.NewLimiter(rate.Every(time.Second*6/10), 1), // 10 запросов за 6 секунд
		stocksLimiter:    rate.NewLimiter(rate.Every(time.Minute/300), 10), // 300 запросов в минуту
		warehouseLimiter: rate.NewLimiter(rate.Every(time.Minute/300), 10), // 300 запросов в минуту
		bot:              bot,
		searchEngine:     searchEngine, // Инициализируем SearchEngine
	}, nil
}

// Структуры для запросов и ответов API

// Склады WB
type Warehouse struct {
	Name         string `json:"name"`
	OfficeID     int64  `json:"officeId"`
	ID           int64  `json:"id"`
	CargoType    int    `json:"cargoType"`
	DeliveryType int    `json:"deliveryType"`
}

// Запрос остатков
type StockRequest struct {
	Skus []string `json:"skus"`
}

// Ответ по остаткам
type StockResponse struct {
	Stocks []Stock `json:"stocks"`
}

type Stock struct {
	Sku    string `json:"sku"`
	Amount int    `json:"amount"`
}

// Информация о ценах и скидках
type PriceHistoryResponse struct {
	Data struct {
		UploadID     int           `json:"uploadID"`
		HistoryGoods []GoodHistory `json:"historyGoods"`
	} `json:"data"`
}

type GoodHistory struct {
	NmID            int    `json:"nmID"`
	VendorCode      string `json:"vendorCode"`
	SizeID          int    `json:"sizeID"`
	TechSizeName    string `json:"techSizeName"`
	Price           int    `json:"price"`
	CurrencyIsoCode string `json:"currencyIsoCode4217"`
	Discount        int    `json:"discount"`
	ClubDiscount    int    `json:"clubDiscount"`
	Status          int    `json:"status"`
	ErrorText       string `json:"errorText,omitempty"`
}

// Модели данных в БД
type ProductRecord struct {
	ID          int       `db:"id"`
	NmID        int       `db:"nm_id"`
	VendorCode  string    `db:"vendor_code"`
	Barcode     string    `db:"barcode"`
	Name        string    `db:"name"`
	LastChecked time.Time `db:"last_checked"`
	CreatedAt   time.Time `db:"created_at"`
}

type PriceRecord struct {
	ID           int       `db:"id"`
	ProductID    int       `db:"product_id"`
	Price        int       `db:"price"`
	Discount     int       `db:"discount"`
	ClubDiscount int       `db:"club_discount"`
	FinalPrice   int       `db:"final_price"`
	RecordedAt   time.Time `db:"recorded_at"`
}

type StockRecord struct {
	ID          int       `db:"id"`
	ProductID   int       `db:"product_id"`
	WarehouseID int64     `db:"warehouse_id"`
	Amount      int       `db:"amount"`
	RecordedAt  time.Time `db:"recorded_at"`
}

// Получение списка складов
func (m *MonitoringService) GetWarehouses(ctx context.Context) ([]Warehouse, error) {
	if err := m.warehouseLimiter.Wait(ctx); err != nil {
		return nil, fmt.Errorf("rate limiter error: %w", err)
	}

	client := &http.Client{Timeout: m.config.RequestTimeout}

	req, err := http.NewRequest("GET", "https://marketplace-api.wildberries.ru/api/v3/warehouses", nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+m.config.ApiKey)

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %s", resp.Status)
	}

	var warehouses []Warehouse
	if err := json.NewDecoder(resp.Body).Decode(&warehouses); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	return warehouses, nil
}

// Получение остатков товаров
func (m *MonitoringService) GetStocks(ctx context.Context, warehouseID int64, skus []string) (*StockResponse, error) {
	if err := m.stocksLimiter.Wait(ctx); err != nil {
		return nil, fmt.Errorf("rate limiter error: %w", err)
	}

	client := &http.Client{Timeout: m.config.RequestTimeout}

	requestBody := StockRequest{
		Skus: skus,
	}

	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("marshaling request body: %w", err)
	}

	url := fmt.Sprintf("https://marketplace-api.wildberries.ru/api/v3/stocks/%d", warehouseID)
	req, err := http.NewRequest("POST", url, io.NopCloser(bytes.NewBuffer(jsonBody))) // Use bytes.Buffer and NopCloser
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+m.config.ApiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %s", resp.Status)
	}

	var stockResponse StockResponse
	if err := json.NewDecoder(resp.Body).Decode(&stockResponse); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	return &stockResponse, nil
}

// Получение истории цен
func (m *MonitoringService) GetPriceHistory(ctx context.Context, uploadID int, limit, offset int) (*PriceHistoryResponse, error) {
	if err := m.pricesLimiter.Wait(ctx); err != nil {
		return nil, fmt.Errorf("rate limiter error: %w", err)
	}

	client := &http.Client{Timeout: m.config.RequestTimeout}

	url := fmt.Sprintf("https://discounts-prices-api.wildberries.ru/api/v2/history/goods/task?uploadID=%d&limit=%d&offset=%d",
		uploadID, limit, offset)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+m.config.ApiKey)

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %s", resp.Status)
	}

	var priceHistoryResponse PriceHistoryResponse
	if err := json.NewDecoder(resp.Body).Decode(&priceHistoryResponse); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	return &priceHistoryResponse, nil
}

// Загрузка всех продуктов из БД
func (m *MonitoringService) GetAllProducts(ctx context.Context) ([]ProductRecord, error) {
	var products []ProductRecord
	err := m.db.SelectContext(ctx, &products, "SELECT * FROM products")
	if err != nil {
		return nil, fmt.Errorf("fetching products from DB: %w", err)
	}
	return products, nil
}

// Получение последней цены товара
func (m *MonitoringService) GetLastPrice(ctx context.Context, productID int) (*PriceRecord, error) {
	var price PriceRecord
	err := m.db.GetContext(ctx, &price,
		"SELECT * FROM prices WHERE product_id = $1 ORDER BY recorded_at DESC LIMIT 1",
		productID)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("fetching last price from DB: %w", err)
	}

	return &price, nil
}

// Получение последнего остатка товара
func (m *MonitoringService) GetLastStock(ctx context.Context, productID int, warehouseID int64) (*StockRecord, error) {
	var stock StockRecord
	err := m.db.GetContext(ctx, &stock,
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

// Сохранение новой цены
func (m *MonitoringService) SavePrice(ctx context.Context, price *PriceRecord) error {
	query := `
		INSERT INTO prices (product_id, price, discount, club_discount, final_price, recorded_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id
	`

	row := m.db.QueryRowContext(ctx, query,
		price.ProductID, price.Price, price.Discount, price.ClubDiscount,
		price.FinalPrice, price.RecordedAt)

	return row.Scan(&price.ID)
}

// Сохранение нового остатка
func (m *MonitoringService) SaveStock(ctx context.Context, stock *StockRecord) error {
	query := `
		INSERT INTO stocks (product_id, warehouse_id, amount, recorded_at)
		VALUES ($1, $2, $3, $4)
		RETURNING id
	`

	row := m.db.QueryRowContext(ctx, query,
		stock.ProductID, stock.WarehouseID, stock.Amount, stock.RecordedAt)

	return row.Scan(&stock.ID)
}

// Отправка уведомления в Telegram
func (m *MonitoringService) SendTelegramAlert(message string) error {
	msg := tgbotapi.NewMessage(m.config.TelegramChatID, message)
	_, err := m.bot.Send(msg)
	return err
}

// Проверка изменения цен и остатков
func (m *MonitoringService) CheckPriceChanges(ctx context.Context, product *ProductRecord, newPrice *PriceRecord) error {
	lastPrice, err := m.GetLastPrice(ctx, product.ID)
	if err != nil {
		return fmt.Errorf("getting last price: %w", err)
	}

	// Если нет предыдущей цены - просто сохраняем текущую
	if lastPrice == nil {
		err := m.SavePrice(ctx, newPrice)
		if err != nil {
			return fmt.Errorf("saving initial price: %w", err)
		}
		return nil
	}

	// Вычисляем процент изменения
	priceDiff := float64(0)
	if lastPrice.FinalPrice > 0 {
		priceDiff = (float64(newPrice.FinalPrice-lastPrice.FinalPrice) / float64(lastPrice.FinalPrice)) * 100
	}

	// Сохраняем новую цену
	err = m.SavePrice(ctx, newPrice)
	if err != nil {
		return fmt.Errorf("saving price: %w", err)
	}

	// Если изменение значительное - отправляем уведомление
	if priceDiff <= -m.config.PriceThreshold || priceDiff >= m.config.PriceThreshold {
		message := fmt.Sprintf(
			"🚨 Обнаружено значительное изменение цены!\n"+
				"Товар: %s (арт. %s)\n"+
				"Старая цена: %d руб (скидка %d%%)\n"+
				"Новая цена: %d руб (скидка %d%%)\n"+
				"Изменение: %.2f%%",
			product.Name, product.VendorCode,
			lastPrice.FinalPrice, lastPrice.Discount,
			newPrice.FinalPrice, newPrice.Discount,
			priceDiff,
		)

		if err := m.SendTelegramAlert(message); err != nil {
			log.Printf("Failed to send Telegram alert about price change: %v", err)
		}
	}

	return nil
}

// Проверка изменения остатков
func (m *MonitoringService) CheckStockChanges(ctx context.Context, product *ProductRecord, newStock *StockRecord) error {
	lastStock, err := m.GetLastStock(ctx, product.ID, newStock.WarehouseID)
	if err != nil {
		return fmt.Errorf("getting last stock: %w", err)
	}

	// Если нет предыдущих остатков - просто сохраняем текущие
	if lastStock == nil {
		err := m.SaveStock(ctx, newStock)
		if err != nil {
			return fmt.Errorf("saving initial stock: %w", err)
		}
		return nil
	}

	// Вычисляем процент изменения
	stockDiff := float64(0)
	if lastStock.Amount > 0 {
		stockDiff = (float64(newStock.Amount-lastStock.Amount) / float64(lastStock.Amount)) * 100
	} else if newStock.Amount > 0 {
		stockDiff = 100 // Появление товара в наличии
	}

	// Сохраняем новые остатки
	err = m.SaveStock(ctx, newStock)
	if err != nil {
		return fmt.Errorf("saving stock: %w", err)
	}

	// Если изменение значительное - отправляем уведомление
	if stockDiff <= -m.config.StockThreshold || stockDiff >= m.config.StockThreshold {
		message := fmt.Sprintf(
			"📦 Обнаружено значительное изменение остатков!\n"+
				"Товар: %s (арт. %s)\n"+
				"Старое количество: %d шт.\n"+
				"Новое количество: %d шт.\n"+
				"Изменение: %.2f%%",
			product.Name, product.VendorCode,
			lastStock.Amount,
			newStock.Amount,
			stockDiff,
		)

		if err := m.SendTelegramAlert(message); err != nil {
			log.Printf("Failed to send Telegram alert about stock change: %v", err)
		}
	}

	return nil
}

// RunProductUpdater запускает обновление продуктов по расписанию.
func (m *MonitoringService) RunProductUpdater(ctx context.Context) error {
	ticker := time.NewTicker(m.config.ProductUpdateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			log.Println("Starting product update cycle")
			if err := m.UpdateProducts(ctx); err != nil {
				log.Printf("Error during product update: %v", err)
				err := m.SendTelegramAlert(fmt.Sprintf("Ошибка при обновлении продуктов: %v", err))
				if err != nil {
					return err
				} // Уведомление об ошибке
			} else {
				log.Println("Product update cycle completed successfully")
				err := m.SendTelegramAlert(fmt.Sprintf("Обновление продуктов успешно завершено"))
				if err != nil {
					return err
				} // Уведомление об успехе
			}
		}
	}
}

// UpdateProducts обновляет список продуктов в базе данных, используя SearchEngine.
func (m *MonitoringService) UpdateProducts(ctx context.Context) error {
	nomenclatureChan := make(chan Nomenclature) // Канал для приема номенклатур
	settings := Settings{
		Sort:   Sort{Ascending: false},
		Filter: Filter{WithPhoto: -1, TagIDs: []int{}, TextSearch: "", AllowedCategoriesOnly: true, ObjectIDs: []int{}, Brands: []string{}, ImtID: 0},
		Cursor: Cursor{Limit: 20000}}
	locale := ""

	go func() {
		err := m.searchEngine.GetNomenclaturesWithLimitConcurrentlyPutIntoChannel(ctx, settings, locale, nomenclatureChan)
		if err != nil {
			log.Printf("GetNomenclaturesWithLimitConcurrentlyPutIntoChannel failed: %v", err)
			close(nomenclatureChan) // Ensure channel is closed on error
		}
	}()

	for nomenclature := range nomenclatureChan {
		if err := m.ProcessNomenclature(ctx, nomenclature); err != nil {
			log.Printf("Error processing nomenclature %d: %v", nomenclature.NmID, err)
			return fmt.Errorf("processing nomenclature %d: %w", nomenclature.NmID, err) // Return error to stop update cycle
		}
	}

	return nil
}

// ProcessNomenclature обрабатывает одну номенклатуру, сохраняя или обновляя данные в базе.
func (m *MonitoringService) ProcessNomenclature(ctx context.Context, nomenclature Nomenclature) error {
	log.Printf("processing nomenclature %s", nomenclature.VendorCode)
	barcode := ""
	if len(nomenclature.Sizes) > 0 && len(nomenclature.Sizes[0].Skus) > 0 {
		barcode = nomenclature.Sizes[0].Skus[0] // Берем первый баркод из первого размера. Уточнить логику, если нужно иначе.
	}

	var existingProduct ProductRecord
	err := m.db.GetContext(ctx, &existingProduct, "SELECT id, nm_id FROM products WHERE nm_id = $1", nomenclature.NmID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("checking existing product: %w", err)
	}

	if errors.Is(err, sql.ErrNoRows) {
		// Продукт не существует, добавляем новый
		_, err = m.db.ExecContext(ctx, `
			INSERT INTO products (nm_id, vendor_code, barcode, name, created_at)
			VALUES ($1, $2, $3, $4, NOW())
		`, nomenclature.NmID, nomenclature.VendorCode, barcode, nomenclature.Title)
		if err != nil {
			return fmt.Errorf("inserting new product: %w", err)
		}
		log.Printf("Added new product NmID: %d, VendorCode: %s", nomenclature.NmID, nomenclature.VendorCode)
	} else {
		// Продукт существует, обновляем данные (например, name, vendor_code, barcode - если нужно)
		_, err = m.db.ExecContext(ctx, `
			UPDATE products SET vendor_code = $2, barcode = $3, name = $4, last_checked = NOW()
			WHERE nm_id = $1
		`, nomenclature.NmID, nomenclature.VendorCode, barcode, nomenclature.Title)
		if err != nil {
			return fmt.Errorf("updating existing product: %w", err)
		}
		log.Printf("Updated product NmID: %d, VendorCode: %s", nomenclature.NmID, nomenclature.VendorCode)
	}

	return nil
}

// Запуск мониторинга
func (m *MonitoringService) RunMonitoring(ctx context.Context) error {
	monitoringTicker := time.NewTicker(m.config.MonitoringInterval)
	defer monitoringTicker.Stop()

	productUpdateTicker := time.NewTicker(m.config.ProductUpdateInterval)
	defer productUpdateTicker.Stop()

	// Запускаем ProductUpdater в отдельной горутине
	go func() {
		if err := m.RunProductUpdater(ctx); err != nil {
			log.Printf("Product updater stopped with error: %v", err)
			err := m.SendTelegramAlert(fmt.Sprintf("Product updater stopped with error: %v", err))
			if err != nil {
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-monitoringTicker.C:
			if err := m.ProcessMonitoring(ctx); err != nil {
				log.Printf("Error during monitoring: %v", err)
			}
		}
	}
}

// Обработка данных мониторинга
func (m *MonitoringService) ProcessMonitoring(ctx context.Context) error {
	log.Println("Starting monitoring cycle")

	// Получаем список всех продуктов
	products, err := m.GetAllProducts(ctx)
	if err != nil {
		return fmt.Errorf("getting products: %w", err)
	}

	// Получаем список складов
	warehouses, err := m.GetWarehouses(ctx)
	if err != nil {
		return fmt.Errorf("getting warehouses: %w", err)
	}

	// Для каждого склада получаем остатки
	for _, warehouse := range warehouses {
		// Разбиваем продукты на партии по 1000 товаров (ограничение API)
		var barcodes []string
		productMap := make(map[string]*ProductRecord)

		for _, product := range products {
			barcodes = append(barcodes, product.Barcode)
			productMap[product.Barcode] = &product

			// Если набрали 1000 товаров или это последний товар, делаем запрос
			if len(barcodes) == 1000 || &product == &products[len(products)-1] {
				stockResp, err := m.GetStocks(ctx, warehouse.ID, barcodes)
				if err != nil {
					log.Printf("Error getting stocks for warehouse %d: %v", warehouse.ID, err)
					continue
				}

				// Обрабатываем полученные остатки
				for _, stock := range stockResp.Stocks {
					product, ok := productMap[stock.Sku]
					if !ok {
						continue
					}

					newStock := &StockRecord{
						ProductID:   product.ID,
						WarehouseID: warehouse.ID,
						Amount:      stock.Amount,
						RecordedAt:  time.Now(),
					}

					if err := m.CheckStockChanges(ctx, product, newStock); err != nil {
						log.Printf("Error checking stock changes for product %d: %v", product.ID, err)
					}
				}

				// Очищаем для следующей партии
				barcodes = nil
				productMap = make(map[string]*ProductRecord)
			}
		}
	}

	// Получаем последнюю загрузку цен
	// Для демонстрации используем фиксированный uploadID, в реальности нужно брать актуальный
	uploadID := 146567 // Пример из документации

	offset := 0
	limit := 1000

	for {
		priceResp, err := m.GetPriceHistory(ctx, uploadID, limit, offset)
		if err != nil {
			return fmt.Errorf("getting price history: %w", err)
		}

		// Если нет данных или меньше лимита - выходим из цикла
		if len(priceResp.Data.HistoryGoods) == 0 {
			break
		}

		// Обрабатываем полученные цены
		for _, goodHistory := range priceResp.Data.HistoryGoods {
			// Находим продукт по NmID
			var product ProductRecord
			err := m.db.GetContext(ctx, &product, "SELECT * FROM products WHERE nm_id = $1", goodHistory.NmID)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					continue
				}
				log.Printf("Error finding product by nm_id %d: %v", goodHistory.NmID, err)
				continue
			}

			// Вычисляем итоговую цену с учетом скидок
			finalPrice := goodHistory.Price
			if goodHistory.Discount > 0 {
				finalPrice = int(float64(goodHistory.Price) * (1 - float64(goodHistory.Discount)/100))
			}

			newPrice := &PriceRecord{
				ProductID:    product.ID,
				Price:        goodHistory.Price,
				Discount:     goodHistory.Discount,
				ClubDiscount: goodHistory.ClubDiscount,
				FinalPrice:   finalPrice,
				RecordedAt:   time.Now(),
			}

			if err := m.CheckPriceChanges(ctx, &product, newPrice); err != nil {
				log.Printf("Error checking price changes for product %d: %v", product.ID, err)
			}
		}

		// Если получено меньше данных, чем запрошено - выходим из цикла
		if len(priceResp.Data.HistoryGoods) < limit {
			break
		}

		// Увеличиваем смещение для следующего запроса
		offset += limit
	}

	log.Println("Monitoring cycle completed")
	return nil
}

func (m *MonitoringService) getProductCount(ctx context.Context) (int, error) {
	var count int
	err := m.db.GetContext(ctx, &count, "SELECT COUNT(*) FROM products")
	if err != nil {
		return 0, fmt.Errorf("getting product count from DB: %w", err)
	}
	return count, nil
}

// SQL-скрипт для создания схемы БД
const schemaSQL = `
CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    nm_id INTEGER NOT NULL UNIQUE,
    vendor_code VARCHAR(50) NOT NULL,
    barcode VARCHAR(50) NOT NULL,
    name VARCHAR(255) NOT NULL,
    last_checked TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_products_nm_id ON products(nm_id); -- Уникальный индекс
CREATE INDEX IF NOT EXISTS idx_products_barcode ON products(barcode);

CREATE TABLE IF NOT EXISTS prices (
    id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL REFERENCES products(id),
    price INTEGER NOT NULL,
    discount INTEGER NOT NULL,
    club_discount INTEGER NOT NULL,
    final_price INTEGER NOT NULL,
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

// Инициализация схемы БД
func (m *MonitoringService) InitDB() error {
	_, err := m.db.Exec(schemaSQL)
	if err != nil {
		return fmt.Errorf("initializing database schema: %w", err)
	}
	return nil
}

func main() {
	// Получаем настройки из переменных окружения
	config := Config{
		WorkerCount:           getEnvInt("WORKER_COUNT", WorkerCount),
		MaxRetries:            getEnvInt("MAX_RETRIES", MaxRetries),
		RetryInterval:         time.Duration(getEnvInt("RETRY_INTERVAL_SEC", int(RetryInterval.Seconds()))) * time.Second,
		RequestTimeout:        time.Duration(getEnvInt("REQUEST_TIMEOUT_SEC", int(RequestTimeout.Seconds()))) * time.Second,
		MonitoringInterval:    time.Duration(getEnvInt("MONITORING_INTERVAL_MIN", int(MonitoringInterval.Minutes()))) * time.Minute,
		ProductUpdateInterval: time.Duration(getEnvInt("PRODUCT_UPDATE_INTERVAL_HOUR", int(ProductUpdateInterval.Hours()))) * time.Hour, // Интервал обновления продуктов
		ApiKey:                getEnvString("WB_API_KEY", ""),
		TelegramToken:         getEnvString("TELEGRAM_TOKEN", ""),
		TelegramChatID:        int64(getEnvInt("TELEGRAM_CHAT_ID", 0)),
		WarehouseID:           int64(getEnvInt("WAREHOUSE_ID", 0)),
		PGConnString:          getEnvString("PG_CONN_STRING", "postgres://bananzza:bananzza_monitor@localhost:5432/wbmonitoring?sslmode=disable"),
		PriceThreshold:        getEnvFloat("PRICE_THRESHOLD", PriceChangeThreshold),
		StockThreshold:        getEnvFloat("STOCK_THRESHOLD", StockChangeThreshold),
	}

	// Проверяем обязательные параметры
	if config.ApiKey == "" {
		log.Fatal("WB_API_KEY environment variable is required")
	}

	if config.TelegramToken == "" {
		log.Fatal("TELEGRAM_TOKEN environment variable is required")
	}

	if config.TelegramChatID == 0 {
		log.Fatal("TELEGRAM_CHAT_ID environment variable is required")
	}

	service, err := NewMonitoringService(config)
	if err != nil {
		log.Fatalf("Failed to create monitoring service: %v", err)
	}

	// Инициализируем БД
	if err := service.InitDB(); err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}

	// Проверяем, нужно ли инициализировать продукты при первом запуске
	ctx := context.Background()
	productCount, err := service.getProductCount(ctx)
	if err != nil {
		log.Fatalf("Failed to check product count: %v", err)
	}

	if productCount == 0 {
		log.Println("Products table is empty, starting initial product load...")
		err = service.SendTelegramAlert("Products table is empty, starting initial product load...")
		if err != nil {
			log.Printf("Failed to send Telegram alert: %v", err)
		}
		if err := service.UpdateProducts(ctx); err != nil {
			log.Printf("Initial product load failed: %v", err)
			err = service.SendTelegramAlert(fmt.Sprintf("Initial product load failed: %v", err))
			log.Fatalf("Initial product load failed: %v", err) // Stop if initial load fails critically
		} else {
			log.Println("Initial product load completed successfully.")
			err = service.SendTelegramAlert("Initial product load completed successfully.")
			if err != nil {
				return
			}
		}
	} else {
		log.Printf("Products table already contains %d products. Skipping initial load.", productCount)
	}

	log.Println("Starting Wildberries Monitoring Service")
	err = service.SendTelegramAlert(fmt.Sprintf("Мониторинг запущен"))
	if err != nil {
		log.Fatalf("Monitoring service stopped with error: %v", err)
	}

	// Запускаем мониторинг и product updater
	go func() {
		if err := service.RunProductUpdater(ctx); err != nil {
			log.Fatalf("Product updater stopped with error: %v", err)
		}
	}()

	if err := service.RunMonitoring(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("Monitoring service stopped with error: %v", err)
	}
}

// Вспомогательные функции для получения переменных окружения
func getEnvString(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func getEnvInt(key string, defaultValue int) int {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}

	intValue := defaultValue
	_, err := fmt.Sscanf(value, "%d", &intValue)
	if err != nil {
		log.Printf("Warning: invalid value for %s: %s, using default: %d", key, value, defaultValue)
		return defaultValue
	}

	return intValue
}

func getEnvFloat(key string, defaultValue float64) float64 {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}

	floatValue := defaultValue
	_, err := fmt.Sscanf(value, "%f", &floatValue)
	if err != nil {
		log.Printf("Warning: invalid value for %s: %s, using default: %f", key, value, defaultValue)
		return defaultValue
	}

	return floatValue
}

// --- SearchEngine related code ---

type SearchEngineConfig struct { // Используем отдельную конфиг для SearchEngine
	WorkerCount    int
	MaxRetries     int
	RetryInterval  time.Duration
	RequestTimeout time.Duration
	ApiKey         string // Добавляем API ключ в конфиг
}

type SearchEngine struct {
	db      *sql.DB
	writer  io.Writer
	config  SearchEngineConfig // Используем SearchEngineConfig
	limiter *rate.Limiter
}

func NewSearchEngine(db *sql.DB, writer io.Writer, config SearchEngineConfig) *SearchEngine { // Используем SearchEngineConfig
	return &SearchEngine{
		db:      db,
		writer:  writer,
		config:  config,
		limiter: rate.NewLimiter(rate.Every(time.Minute/70), 10),
	}
}

const postNomenclature = "https://content-api.wildberries.ru/content/v2/get/cards/list"

func (d *SearchEngine) GetNomenclatures(settings Settings, locale string) (*NomenclatureResponse, error) {
	url := postNomenclature
	if locale != "" {
		url = fmt.Sprintf("%s?locale=%s", url, locale)
	}

	client := &http.Client{Timeout: d.config.RequestTimeout}

	requestBody, err := settings.CreateRequestBody()
	if err != nil {
		return nil, fmt.Errorf("creating request body: %w", err)
	}

	req, err := http.NewRequest("POST", url, requestBody)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+d.config.ApiKey) // Используем API ключ из конфига
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %s", resp.Status)
	}

	var nomenclatureResponse NomenclatureResponse
	if err := json.NewDecoder(resp.Body).Decode(&nomenclatureResponse); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	return &nomenclatureResponse, nil
}

type SafeCursorManager struct {
	mu          sync.Mutex
	usedCursors map[string]bool
	lastCursor  Cursor
}

func NewSafeCursorManager() *SafeCursorManager {
	return &SafeCursorManager{
		usedCursors: make(map[string]bool),
	}
}

func (scm *SafeCursorManager) GetUniqueCursor(nmID int, updatedAt string) (Cursor, bool) {
	scm.mu.Lock()
	defer scm.mu.Unlock()

	cursorKey := fmt.Sprintf("%d_%s", nmID, updatedAt)

	if scm.usedCursors[cursorKey] {
		return Cursor{}, false
	}

	scm.usedCursors[cursorKey] = true
	cursor := Cursor{
		NmID:      nmID,
		UpdatedAt: updatedAt,
		Limit:     100,
	}
	scm.lastCursor = cursor

	return cursor, true
}

func (d *SearchEngine) GetNomenclaturesWithLimitConcurrentlyPutIntoChannel(
	ctx context.Context,
	settings Settings,
	locale string,
	nomenclatureChan chan Nomenclature,
) error {
	defer log.Printf("Search engine finished its job.")

	limit := settings.Cursor.Limit
	log.Printf("Getting Wildberries nomenclatures with limit: %d", limit)

	ctx, cancel := context.WithTimeout(ctx, time.Minute*20)
	defer cancel()

	cursorManager := NewSafeCursorManager()
	taskChan := make(chan Cursor, d.config.WorkerCount)
	errChan := make(chan error, d.config.WorkerCount)

	var (
		wg             sync.WaitGroup
		totalProcessed atomic.Int32
	)

	for i := 0; i < d.config.WorkerCount; i++ {
		wg.Add(1)
		go d.safeWorker(
			ctx,
			i,
			&wg,
			taskChan,
			nomenclatureChan,
			errChan,
			settings,
			locale,
			limit,
			&totalProcessed,
			cursorManager,
		)
	}

	log.Printf("Sending initial task to taskChan")
	select {
	case taskChan <- Cursor{NmID: 0, UpdatedAt: "", Limit: 100}:
		log.Printf("Initial task sent successfully")
	case <-ctx.Done():
		log.Printf("Context cancelled while sending initial task")
		return ctx.Err()
	}

	go func() {
		wg.Wait()
		log.Printf("Starting cleanup goroutine")
		log.Printf("All workers finished, closing channels")
		close(taskChan)
		close(errChan)
		log.Printf("Channels closed")
	}()

	for err := range errChan {
		if err == nil {
			continue
		}
		log.Printf("Received error from worker: %v", err)

		if errors.Is(err, ErrNoMoreData) {
			log.Printf("No more data to process")
			continue
		}
		if errors.Is(err, ErrTotalLimitReached) {
			log.Printf("Total limit reached")
			continue
		}
		if errors.Is(err, ErrContextCanceled) {
			log.Printf("Context was canceled")
			continue
		}

		cancel()
		return fmt.Errorf("worker error: %w", err)
	}

	return nil
}

func (d *SearchEngine) safeWorker(
	ctx context.Context,
	workerID int,
	wg *sync.WaitGroup,
	taskChan chan Cursor,
	nomenclatureChan chan<- Nomenclature,
	errChan chan error,
	settings Settings,
	locale string,
	totalLimit int,
	totalProcessed *atomic.Int32,
	cursorManager *SafeCursorManager,
) {
	defer func() {
		log.Printf("Worker %d: job is done.", workerID)
		wg.Done()
	}()

	for {
		select {
		case <-ctx.Done():
			errChan <- ErrContextCanceled
			return
		case cursor, ok := <-taskChan:
			if !ok {
				return
			}

			uniqueCursor, ok := cursorManager.GetUniqueCursor(cursor.NmID, cursor.UpdatedAt)
			if !ok {
				continue
			}

			err := d.processSafeCursorTask(
				ctx,
				workerID,
				uniqueCursor,
				nomenclatureChan,
				settings,
				locale,
				totalLimit,
				totalProcessed,
				taskChan,
			)

			if err != nil && !errors.Is(err, ErrNoMoreData) && !errors.Is(err, ErrTotalLimitReached) {
				errChan <- err
			}
		}
	}
}

func (d *SearchEngine) processSafeCursorTask(
	ctx context.Context,
	workerID int,
	cursor Cursor,
	nomenclatureChan chan<- Nomenclature,
	settings Settings,
	locale string,
	totalLimit int,
	totalProcessed *atomic.Int32,
	taskChan chan<- Cursor,
) error {
	if err := d.limiter.Wait(ctx); err != nil {
		log.Printf("Worker %d: Rate limiter error: %v", workerID, err)
		return fmt.Errorf("%w: %v", ErrRateLimiter, err)
	}

	settings.Cursor = cursor
	log.Printf("Worker %d: Fetching nomenclatures with cursor: NmID=%d, UpdatedAt=%v, Limit=%d",
		workerID, cursor.NmID, cursor.UpdatedAt, cursor.Limit)

	nomenclatureResponse, err := d.retryGetNomenclatures(ctx, settings, &cursor, locale)
	if err != nil {
		if errors.Is(err, ErrContextCanceled) {
			return err
		}
		return fmt.Errorf("failed to get nomenclatures: %w", err)
	}

	lastNomenclature := nomenclatureResponse.Data[len(nomenclatureResponse.Data)-1]

	if len(nomenclatureResponse.Data) == cursor.Limit {
		select {
		case taskChan <- Cursor{
			NmID:      lastNomenclature.NmID,
			UpdatedAt: lastNomenclature.UpdatedAt,
			Limit:     cursor.Limit,
		}:
		case <-ctx.Done():
			return ErrContextCanceled
		}
	}

	for _, nomenclature := range nomenclatureResponse.Data {
		totalProcessed.Add(1)
		if totalProcessed.Load() >= int32(totalLimit) {
			return ErrTotalLimitReached
		}

		select {
		case <-ctx.Done():
			return ErrContextCanceled
		case nomenclatureChan <- nomenclature:
		}
	}

	if len(nomenclatureResponse.Data) < cursor.Limit {
		defer close(nomenclatureChan)
		return ErrNoMoreData
	}

	return nil
}

func (d *SearchEngine) retryGetNomenclatures(
	ctx context.Context,
	settings Settings,
	cursor *Cursor,
	locale string,
) (*NomenclatureResponse, error) {
	var lastErr error

	for retry := 0; retry < d.config.MaxRetries; retry++ {
		select {
		case <-ctx.Done():
			return nil, ErrContextCanceled
		default:
		}

		settings.Cursor = *cursor
		nomenclatureResponse, err := d.GetNomenclatures(settings, locale)
		if err == nil {
			return nomenclatureResponse, nil
		}

		if errors.Is(err, ErrConnectionAborted) {
			log.Printf("Retrying to get nomenclatures due to connection error. Attempt: %d", retry+1)
			lastErr = err
			time.Sleep(d.config.RetryInterval)
			continue
		}

		return nil, fmt.Errorf("failed to get nomenclatures: %w", err)
	}

	return nil, fmt.Errorf("%w: %v", ErrFailedAfterRetries, lastErr)
}

// --- Request and Response structures ---

type SettingsRequestWrapper struct {
	Settings Settings `json:"settings"`
}

type Settings struct {
	Sort   Sort   `json:"sort"`
	Filter Filter `json:"filter"`
	Cursor Cursor `json:"cursor"`
}

func (s *Settings) CreateRequestBody() (*bytes.Buffer, error) {
	wrapper := SettingsRequestWrapper{Settings: *s}

	jsonData, err := json.Marshal(wrapper)
	if err != nil {
		return nil, fmt.Errorf("marshalling settings: %w", err)
	}
	return bytes.NewBuffer(jsonData), nil
}

type Sort struct {
	Ascending bool `json:"ascending"` // Сортировать по полю updatedAt (false - по убыванию, true - по возрастанию)
}

type Filter struct {
	/*
		Фильтр по фото:
		0 — только карточки без фото
		1 — только карточки с фото
		-1 — все карточки товара
	*/
	WithPhoto int `json:"withPhoto"`

	/*
		Поиск по артикулу продавца, артикулу WB, баркоду
	*/
	TextSearch string `json:"textSearch"`

	/*
		Поиск по ID тегов
	*/
	TagIDs []int `json:"tagIDs"`

	/*
		Фильтр по категории. true - только разрешённые, false - все. Не используется в песочнице.
	*/
	AllowedCategoriesOnly bool `json:"allowedCategoriesOnly"`

	/*
		Поиск по id предметов
	*/
	ObjectIDs []int `json:"objectIDs"`

	/*
		Поиск по брендам
	*/
	Brands []string `json:"brands"`

	/*
		Поиск по ID карточки товара
	*/
	ImtID int `json:"imtID"`
}

type Cursor struct {
	Limit     int    `json:"limit"` // Сколько карточек товара выдать в ответе.
	UpdatedAt string `json:"updatedAt,omitempty"`
	NmID      int    `json:"nmId,omitempty"`
}

type NomenclatureResponse struct {
	Data   []Nomenclature `json:"cards"`
	Cursor Cursor         `json:"cursor"`
}

type Nomenclature struct {
	NmID            int        `json:"nmID"`
	ImtID           int        `json:"imtID"`
	NmUUID          string     `json:"nmUUID"`
	SubjectID       int        `json:"subjectID"`
	VendorCode      string     `json:"vendorCode"`
	SubjectName     string     `json:"subjectName"`
	Brand           string     `json:"brand"`
	Title           string     `json:"title"`
	Photos          []Photo    `json:"photos"`
	Video           string     `json:"video"`
	Dimensions      Dimensions `json:"dimensions"`
	Characteristics []Charc    `json:"characteristics"`
	Sizes           []Size     `json:"sizes"`
	Tags            []Tag      `json:"tags"`
	CreatedAt       string     `json:"createdAt"`
	UpdatedAt       string     `json:"updatedAt"`
}

type Photo struct {
	Big    string `json:"big"`
	Tiny   string `json:"tm"`
	Small  string `json:"c246x328"`
	Square string `json:"square"`
	Medium string `json:"c516x688"`
}

type Dimensions struct {
	Length  int  `json:"length"`
	Width   int  `json:"width"`
	Height  int  `json:"height"`
	IsValid bool `json:"isValid"`
}

type Charc struct {
	Id    int    `json:"id"`    // ID характеристики
	Name  string `json:"name"`  // Название характеристики
	Value any    `json:"value"` // Значение характеристики. Тип значения зависит от типа характеристики
}
type Size struct {
	ChrtID   int      `json:"chrtID"`   //Числовой ID размера для данного артикула WB
	TechSize string   `json:"techSize"` // Размер товара (А, XXL, 57 и др.)
	WbSize   string   `json:"wbSize"`   // Российский размер товара
	Skus     []string `json:"skus"`     // Баркод товара
}

type Tag struct {
	Id    int    `json:"id"`
	Name  string `json:"name"`
	Color string `json:"color"`
}
