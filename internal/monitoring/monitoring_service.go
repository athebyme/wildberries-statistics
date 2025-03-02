package monitoring

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"time"
	"wbmonitoring/monitoring/internal/api"
	"wbmonitoring/monitoring/internal/config"
	"wbmonitoring/monitoring/internal/db"
	"wbmonitoring/monitoring/internal/models"
	"wbmonitoring/monitoring/internal/search"
	"wbmonitoring/monitoring/internal/telegram"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"golang.org/x/time/rate"
)

// MonitoringService struct
type MonitoringService struct {
	db               *sqlx.DB
	config           config.Config
	pricesLimiter    *rate.Limiter
	stocksLimiter    *rate.Limiter
	warehouseLimiter *rate.Limiter
	searchEngine     *search.SearchEngine
	httpClient       *http.Client
	telegramBot      *telegram.Bot
	recordCleanupSvc *RecordCleanupService
}

// NewMonitoringService creates a new MonitoringService.
func NewMonitoringService(cfg config.Config) (*MonitoringService, error) {
	database, err := sqlx.Connect("postgres", cfg.PGConnString)
	if err != nil {
		return nil, fmt.Errorf("connecting to database: %w", err)
	}

	telegramBot, err := telegram.NewBot(cfg.TelegramToken, cfg.TelegramChatID, database, cfg.AllowedUserIDs)
	if err != nil {
		return nil, fmt.Errorf("initializing telegram bot: %w", err)
	}

	searchConfig := search.SearchEngineConfig{ // Используем отдельную конфиг для SearchEngine
		WorkerCount:    cfg.WorkerCount,
		MaxRetries:     cfg.MaxRetries,
		RetryInterval:  cfg.RetryInterval,
		RequestTimeout: cfg.RequestTimeout,
		ApiKey:         cfg.ApiKey, // Передаем API ключ
	}

	cleanupSvc := NewRecordCleanupService(
		database,
		24*time.Hour,    // Запускаем очистку раз в сутки
		30*24*time.Hour, // Храним данные за 30 дней
	)

	searchEngine := search.NewSearchEngine(database.DB, os.Stdout, searchConfig) // Используем db.DB, так как SearchEngine ожидает *sql.DB
	client := http.Client{Timeout: cfg.RequestTimeout}
	return &MonitoringService{
		db:               database,
		config:           cfg,
		pricesLimiter:    rate.NewLimiter(rate.Every(time.Second*6/10), 1), // 10 запросов за 6 секунд
		stocksLimiter:    rate.NewLimiter(rate.Every(time.Minute/300), 10), // 300 запросов в минуту
		warehouseLimiter: rate.NewLimiter(rate.Every(time.Minute/300), 10), // 300 запросов в минуту
		telegramBot:      telegramBot,
		recordCleanupSvc: cleanupSvc,
		searchEngine:     searchEngine, // Инициализируем SearchEngine
		httpClient:       &client,
	}, nil
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
			} else {
				log.Println("Product update cycle completed successfully")
			}
		}
	}
}

// UpdateProducts обновляет список продуктов в базе данных, используя SearchEngine.
func (m *MonitoringService) UpdateProducts(ctx context.Context) error {
	nomenclatureChan := make(chan models.Nomenclature) // Канал для приема номенклатур
	settings := models.Settings{
		Sort:   models.Sort{Ascending: false},
		Filter: models.Filter{WithPhoto: -1, TagIDs: []int{}, TextSearch: "", AllowedCategoriesOnly: true, ObjectIDs: []int{}, Brands: []string{}, ImtID: 0},
		Cursor: models.Cursor{Limit: 20000}}
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
func (m *MonitoringService) ProcessNomenclature(ctx context.Context, nomenclature models.Nomenclature) error {
	barcode := ""
	if len(nomenclature.Sizes) > 0 && len(nomenclature.Sizes[0].Skus) > 0 {
		barcode = nomenclature.Sizes[0].Skus[0] // Берем первый баркод из первого размера. Уточнить логику, если нужно иначе.
	}

	var existingProduct models.ProductRecord
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
	} else {
		// Продукт существует, обновляем данные (например, name, vendor_code, barcode - если нужно)
		_, err = m.db.ExecContext(ctx, `
			UPDATE products SET vendor_code = $2, barcode = $3, name = $4
			WHERE nm_id = $1
		`, nomenclature.NmID, nomenclature.VendorCode, barcode, nomenclature.Title)
		if err != nil {
			return fmt.Errorf("updating existing product: %w", err)
		}
	}

	return nil
}

// RunMonitoring запускает основной цикл мониторинга.
func (m *MonitoringService) RunMonitoring(ctx context.Context) error {
	// Отправляем приветственное сообщение
	if err := m.telegramBot.SendTelegramAlert("🔄 Сервис мониторинга запущен"); err != nil {
		log.Printf("Failed to send welcome message: %v", err)
	}

	// Запускаем бота в отдельной горутине
	go m.telegramBot.StartBot(ctx)

	// Запускаем сервис очистки записей
	go m.recordCleanupSvc.RunCleanupProcess(ctx)

	// Запускаем процесс обновления информации о товарах
	go m.RunProductUpdater(ctx)

	// Запускаем отправку ежедневных отчетов
	go m.runDailyReporting(ctx)

	// Основной цикл мониторинга
	ticker := time.NewTicker(m.config.MonitoringInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := m.ProcessMonitoring(ctx); err != nil {
				log.Printf("Error during monitoring process: %v", err)
			}
		case <-ctx.Done():
			// Отправляем сообщение о завершении работы
			if err := m.telegramBot.SendTelegramAlert("⚠️ Сервис мониторинга остановлен"); err != nil {
				log.Printf("Failed to send shutdown message: %v", err)
			}
			return ctx.Err()
		}
	}
}

// runDailyReporting запускает процесс отправки ежедневных отчетов
func (m *MonitoringService) runDailyReporting(ctx context.Context) {
	// Вычисляем время до следующего запуска (10:00 утра)
	now := time.Now()
	nextRun := time.Date(now.Year(), now.Month(), now.Day(), 10, 0, 0, 0, now.Location())
	if now.After(nextRun) {
		nextRun = nextRun.Add(24 * time.Hour)
	}

	initialDelay := nextRun.Sub(now)
	log.Printf("Daily report scheduled at %s (in %s)", nextRun.Format("15:04:05"), initialDelay)

	// Ждем до первого запуска
	timer := time.NewTimer(initialDelay)

	for {
		select {
		case <-timer.C:
			// Отправляем ежедневный отчет
			if err := m.telegramBot.SendDailyReport(ctx); err != nil {
				log.Printf("Error sending daily report: %v", err)
			}

			// Настраиваем таймер на следующие сутки
			timer.Reset(24 * time.Hour)
		case <-ctx.Done():
			timer.Stop()
			return
		}
	}
}

func (m *MonitoringService) SendGreetings(ctx context.Context) error {
	log.Println("Sending greetings")
	seller, err := api.GetSellerInfo(ctx, *m.httpClient, m.config.ApiKey, rate.NewLimiter(rate.Every(1*time.Minute), 1))
	if err != nil {
		err = m.telegramBot.SendTelegramAlert("Ошибка получения информации продавца с WB")
		if err != nil {
			return err
		}
		log.Fatalf("Error getting seller info: %v", err)
	}
	err = m.telegramBot.SendTelegramAlertWithParseMode(fmt.Sprintf("`%s`, успешная авторизация WB", seller.Name), "Markdown")
	if err != nil {
		return err
	}
	return nil
}

// ProcessMonitoring обрабатывает данные мониторинга.
func (m *MonitoringService) ProcessMonitoring(ctx context.Context) error {
	log.Println("Starting monitoring cycle")

	// Получаем список всех продуктов (без использования last_checked)
	products, err := m.GetAllProducts(ctx)
	if err != nil {
		return fmt.Errorf("failed to get products: %w", err)
	}

	// Обработка складских остатков
	if err := m.processStocks(ctx, products); err != nil {
		log.Printf("Error processing stocks: %v", err)
		// Продолжаем выполнение, чтобы обработать цены
	}

	// Обработка цен через API цен и скидок
	if err := m.processPrices(ctx, products); err != nil {
		log.Printf("Error processing prices: %v", err)
	}

	log.Println("Monitoring cycle completed")
	return nil
}

// GetAllProducts retrieves all products from the database.
func (m *MonitoringService) GetAllProducts(ctx context.Context) ([]models.ProductRecord, error) {
	var products []models.ProductRecord
	query := `SELECT id, nm_id, vendor_code, barcode, name, created_at FROM products`

	if err := m.db.SelectContext(ctx, &products, query); err != nil {
		return nil, fmt.Errorf("fetching products from DB: %w", err)
	}

	return products, nil
}

// processStocks обрабатывает складские остатки для всех товаров.
func (m *MonitoringService) processStocks(ctx context.Context, products []models.ProductRecord) error {
	// Получаем список складов
	warehouses, err := m.GetWarehouses(ctx)
	if err != nil {
		return fmt.Errorf("failed to get warehouses: %w", err)
	}

	// Для каждого склада получаем остатки
	for _, warehouse := range warehouses {
		if err := m.processWarehouseStocks(ctx, warehouse, products); err != nil {
			log.Printf("Error processing warehouse %d: %v", warehouse.ID, err)
		}
	}

	return nil
}

// processWarehouseStocks обрабатывает остатки для конкретного склада.
func (m *MonitoringService) processWarehouseStocks(ctx context.Context, warehouse models.Warehouse, products []models.ProductRecord) error {
	// Разбиваем продукты на партии по 1000 товаров (ограничение API)
	batchSize := 1000

	for i := 0; i < len(products); i += batchSize {
		end := i + batchSize
		if end > len(products) {
			end = len(products)
		}

		batch := products[i:end]

		// Собираем баркоды для текущей партии
		var barcodes []string
		productMap := make(map[string]*models.ProductRecord)

		for j := range batch {
			product := &batch[j]
			barcodes = append(barcodes, product.Barcode)
			productMap[product.Barcode] = product
		}

		// Запрашиваем остатки
		stockResp, err := m.GetStocks(ctx, warehouse.ID, barcodes)
		if err != nil {
			return fmt.Errorf("failed to get stocks for warehouse %d: %w", warehouse.ID, err)
		}

		// Обрабатываем полученные остатки
		for _, stock := range stockResp.Stocks {
			product, ok := productMap[stock.Sku]
			if !ok {
				continue
			}

			newStock := &models.StockRecord{
				ProductID:   product.ID,
				WarehouseID: warehouse.ID,
				Amount:      stock.Amount,
				RecordedAt:  time.Now(),
			}

			// Проверяем изменения остатков
			if err := m.CheckStockChanges(ctx, product, newStock); err != nil {
				log.Printf("Error checking stock changes for product %d: %v", product.ID, err)
			}

			// Обновляем статус проверки остатков
			if err := m.UpdateStockCheckStatus(ctx, product.ID); err != nil {
				log.Printf("Error updating stock check status for product %d: %v", product.ID, err)
			}
		}
	}

	return nil
}

// processPrices обрабатывает цены товаров.
func (m *MonitoringService) processPrices(ctx context.Context, products []models.ProductRecord) error {
	// Разбиваем продукты на группы для запросов с лимитом
	// API позволяет максимум 1000 товаров на страницу
	limit := 1000

	// Группируем продукты по nmID
	nmIDToProduct := make(map[int]*models.ProductRecord)
	var nmIDs []int

	for i := range products {
		product := &products[i]
		nmIDs = append(nmIDs, product.NmID)
		nmIDToProduct[product.NmID] = product
	}

	// Обрабатываем все товары с пагинацией
	offset := 0

	for {
		// Получаем информацию о ценах и скидках через API
		goodsResp, err := m.GetGoodsPrices(ctx, limit, offset, 0) // 0 - без фильтра по nmID
		if err != nil {
			return fmt.Errorf("failed to get goods prices: %w", err)
		}

		// Если нет данных, выходим из цикла
		if len(goodsResp.Data.ListGoods) == 0 {
			break
		}

		// Обрабатываем полученные цены
		for _, good := range goodsResp.Data.ListGoods {
			product, ok := nmIDToProduct[good.NmID]
			if !ok {
				continue // Пропускаем товары, которых нет в нашей базе
			}

			// Обрабатываем каждый размер товара отдельно
			for _, size := range good.Sizes {
				if err := m.processPriceRecord(ctx, product, size); err != nil {
					log.Printf("Error processing price for nmID %d, sizeID %d: %v",
						good.NmID, size.SizeID, err)
				}
			}

			// Обновляем статус проверки цен после обработки всех размеров товара
			if err := m.UpdatePriceCheckStatus(ctx, product.ID); err != nil {
				log.Printf("Error updating price check status for product %d: %v", product.ID, err)
			}
		}

		// Если получено меньше данных, чем запрошено - выходим из цикла
		if len(goodsResp.Data.ListGoods) < limit {
			break
		}

		// Увеличиваем смещение для следующего запроса
		offset += limit
	}

	return nil
}

// processPriceRecord обрабатывает запись о цене размера товара.
func (m *MonitoringService) processPriceRecord(ctx context.Context, product *models.ProductRecord, size models.GoodSize) error {
	// Вычисляем итоговую цену с учетом скидок
	finalPrice := size.DiscountedPrice
	clubPrice := size.ClubDiscountedPrice

	newPrice := &models.PriceRecord{
		ProductID:         product.ID,
		SizeID:            size.SizeID,
		Price:             size.Price,
		Discount:          size.Discount,
		ClubDiscount:      size.ClubDiscount,
		FinalPrice:        int(finalPrice),
		ClubFinalPrice:    int(clubPrice),
		CurrencyIsoCode:   size.CurrencyIsoCode4217,
		TechSizeName:      size.TechSizeName,
		EditableSizePrice: size.EditableSizePrice,
		RecordedAt:        time.Now(),
	}

	// Проверяем изменения цен
	if err := m.CheckPriceChanges(ctx, product, newPrice); err != nil {
		return fmt.Errorf("failed to check price changes: %w", err)
	}

	return nil
}

// CheckPriceChanges проверяет изменения цен и отправляет уведомления.
func (m *MonitoringService) CheckPriceChanges(ctx context.Context, product *models.ProductRecord, newPrice *models.PriceRecord) error {
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
				"Товар: %s (арт. `%s`)\n"+
				"Старая цена: %d руб (скидка %d%%)\n"+
				"Новая цена: %d руб (скидка %d%%)\n"+
				"Изменение: %.2f%%",
			product.Name, product.VendorCode,
			lastPrice.FinalPrice, lastPrice.Discount,
			newPrice.FinalPrice, newPrice.Discount,
			priceDiff,
		)

		if err := m.telegramBot.SendTelegramAlertWithParseMode(message, "Markdown"); err != nil {
			log.Printf("Failed to send Telegram alert about price change: %v", err)
		}
	}

	return nil
}

// CheckStockChanges проверяет изменения остатков и отправляет уведомления.
func (m *MonitoringService) CheckStockChanges(ctx context.Context, product *models.ProductRecord, newStock *models.StockRecord) error {
	lastStock, err := db.GetLastStock(ctx, m.db, product.ID, newStock.WarehouseID)
	if err != nil {
		return fmt.Errorf("getting last stock: %w", err)
	}

	// Если нет предыдущих остатков - просто сохраняем текущие
	if lastStock == nil {
		err := db.SaveStock(ctx, m.db, newStock)
		if err != nil {
			return fmt.Errorf("saving initial stock: %w", err)
		}
		return nil
	}

	// Вычисляем процент изменения
	stockDiff := float64(0)
	if lastStock.Amount > 0 {
		stockDiff = math.Abs(float64(newStock.Amount-lastStock.Amount)) / float64(lastStock.Amount) * 100
	} else if newStock.Amount > 0 {
		stockDiff = 100 // Появление товара в наличии
	}

	// Сохраняем новые остатки
	err = db.SaveStock(ctx, m.db, newStock)
	if err != nil {
		return fmt.Errorf("saving stock: %w", err)
	}

	// Если изменение значительное - отправляем уведомление
	if stockDiff <= -m.config.StockThreshold || stockDiff >= m.config.StockThreshold {
		message := fmt.Sprintf(
			"📦 Обнаружено значительное изменение остатков!\n"+
				"Товар: %s (арт. `%s`)\n"+
				"Старое количество: %d шт.\n"+
				"Новое количество: %d шт.\n"+
				"Изменение: %.2f%%",
			product.Name, product.VendorCode,
			lastStock.Amount,
			newStock.Amount,
			stockDiff,
		)

		if err := m.telegramBot.SendTelegramAlertWithParseMode(message, "Markdown"); err != nil {
			log.Printf("Failed to send Telegram alert about stock change: %v", err)
		}
	}

	return nil
}

// GetWarehouses retrieves warehouses from the API.
func (m *MonitoringService) GetWarehouses(ctx context.Context) ([]models.Warehouse, error) {
	return api.GetWarehouses(ctx, m.httpClient, m.config.ApiKey, m.warehouseLimiter)
}

// GetStocks retrieves stock information from the API.
func (m *MonitoringService) GetStocks(ctx context.Context, warehouseID int64, skus []string) (*models.StockResponse, error) {
	return api.GetStocks(ctx, m.httpClient, m.config.ApiKey, m.stocksLimiter, warehouseID, skus)
}

// GetPriceHistory retrieves price history from the API.
func (m *MonitoringService) GetPriceHistory(ctx context.Context, uploadID int, limit, offset int) (*models.PriceHistoryResponse, error) {
	return api.GetPriceHistory(ctx, m.httpClient, m.config.ApiKey, m.pricesLimiter, uploadID, limit, offset)
}

// GetGoodsPrices retrieves goods prices from the API.
func (m *MonitoringService) GetGoodsPrices(ctx context.Context, limit int, offset int, filterNmID int) (*models.GoodsPricesResponse, error) {
	return api.GetGoodsPrices(ctx, m.httpClient, m.config.ApiKey, m.pricesLimiter, limit, offset, filterNmID)
}

// GetLastPrice retrieves the last price record from the database.
func (m *MonitoringService) GetLastPrice(ctx context.Context, productID int) (*models.PriceRecord, error) {
	return db.GetLastPrice(ctx, m.db, productID)
}

// GetLastStock retrieves the last stock record from the database.
func (m *MonitoringService) GetLastStock(ctx context.Context, productID int, warehouseID int64) (*models.StockRecord, error) {
	return db.GetLastStock(ctx, m.db, productID, warehouseID)
}

// SavePrice saves a price record to the database.
func (m *MonitoringService) SavePrice(ctx context.Context, price *models.PriceRecord) error {
	return db.SavePrice(ctx, m.db, price)
}

// SaveStock saves a stock record to the database.
func (m *MonitoringService) SaveStock(ctx context.Context, stock *models.StockRecord) error {
	return db.SaveStock(ctx, m.db, stock)
}

// InitDB initializes the database schema.
func (m *MonitoringService) InitDB() error {
	return db.InitDB(m.db)
}

// GetProductCount retrieves the count of products in the database.
func (m *MonitoringService) GetProductCount(ctx context.Context) (int, error) {
	return db.GetProductCount(ctx, m.db)
}

// UpdatePriceCheckStatus updates the last price check status in the database.
func (m *MonitoringService) UpdatePriceCheckStatus(ctx context.Context, productID int) error {
	return db.UpdatePriceCheckStatus(ctx, m.db, productID)
}

// UpdateStockCheckStatus updates the last stock check status in the database.
func (m *MonitoringService) UpdateStockCheckStatus(ctx context.Context, productID int) error {
	return db.UpdateStockCheckStatus(ctx, m.db, productID)
}
