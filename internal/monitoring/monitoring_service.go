package monitoring

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"golang.org/x/time/rate"
	"log"
	"math"
	"net/http"
	"os"
	"sync"
	"time"
	"wbmonitoring/monitoring/internal/api"
	"wbmonitoring/monitoring/internal/config"
	"wbmonitoring/monitoring/internal/db"
	"wbmonitoring/monitoring/internal/models"
	"wbmonitoring/monitoring/internal/search"
	"wbmonitoring/monitoring/internal/stats/sse"
	"wbmonitoring/monitoring/internal/telegram"
	"wbmonitoring/monitoring/internal/telegram/report"
)

// SSENotifier handles sending notifications to SSE clients
type SSENotifier struct {
	sseManager *sse.SSEManager
	enabled    bool
}

// NewSSENotifier creates a new SSE notifier
func NewSSENotifier(sseManager *sse.SSEManager) *SSENotifier {
	return &SSENotifier{
		sseManager: sseManager,
		enabled:    sseManager != nil,
	}
}

// NotifyPriceChange sends a price change notification to SSE clients
func (n *SSENotifier) NotifyPriceChange(productID int, productName, vendorCode string, oldPrice, newPrice, changeAmount int, changePercent float64) {
	if !n.enabled {
		return
	}

	// Create a price change event
	priceChange := map[string]interface{}{
		"productId":     productID,
		"productName":   productName,
		"vendorCode":    vendorCode,
		"oldPrice":      oldPrice,
		"newPrice":      newPrice,
		"changeAmount":  changeAmount,
		"changePercent": changePercent,
		"date":          time.Now().Format(time.RFC3339),
	}

	// Broadcast to all SSE clients
	n.sseManager.BroadcastPriceChange(priceChange)
	log.Printf("SSE: Broadcasted price change for product %s (%d)", productName, productID)
}

// NotifyStockChange sends a stock change notification to SSE clients
func (n *SSENotifier) NotifyStockChange(productID int, productName, vendorCode string, warehouseID int64, warehouseName string, oldAmount, newAmount, changeAmount int, changePercent float64) {
	if !n.enabled {
		return
	}

	// Create a stock change event
	stockChange := map[string]interface{}{
		"productId":     productID,
		"productName":   productName,
		"vendorCode":    vendorCode,
		"warehouseId":   warehouseID,
		"warehouseName": warehouseName,
		"oldAmount":     oldAmount,
		"newAmount":     newAmount,
		"changeAmount":  changeAmount,
		"changePercent": changePercent,
		"date":          time.Now().Format(time.RFC3339),
	}

	// Broadcast to all SSE clients
	n.sseManager.BroadcastStockChange(stockChange)
	log.Printf("SSE: Broadcasted stock change for product %s (%d) at warehouse %s", productName, productID, warehouseName)
}

type Service struct {
	db               *sqlx.DB
	config           config.Config
	pricesLimiter    *rate.Limiter
	stocksLimiter    *rate.Limiter
	warehouseLimiter *rate.Limiter
	searchEngine     *search.SearchEngine
	httpClient       *http.Client
	telegramBot      *telegram.Bot
	recordCleanupSvc *RecordCleanupService
	sseNotifier      *SSENotifier

	ctx context.Context
}

func NewMonitoringService(cfg config.Config) (*Service, error) {
	// Подключаемся к базе данных
	database, err := sqlx.Connect("postgres", cfg.PGConnString)
	if err != nil {
		return nil, fmt.Errorf("connecting to database: %w", err)
	}

	log.Printf("allowed users %v", cfg.AllowedUserIDs)

	// Создаем конфигурацию для отчетов
	reportConfig := report.ReportConfig{
		MinPriceChangePercent: cfg.PriceThreshold,
		MinStockChangePercent: cfg.StockThreshold,
	}

	// Инициализируем Telegram бота
	telegramBot, err := telegram.NewBot(
		cfg.TelegramToken,
		cfg.TelegramChatID,
		database,
		cfg.AllowedUserIDs,
		reportConfig)
	if err != nil {
		return nil, fmt.Errorf("initializing telegram bot: %w", err)
	}

	if cfg.UseImprovedServices {
		emailService, err := telegram.NewEmailService(database)
		if err != nil {
			log.Printf("Warning: Failed to initialize EmailService: %v", err)
		}

		pdfGenerator := report.NewPDFGenerator(database)

		excelGenerator := report.NewExcelGenerator(
			database,
			reportConfig,
			cfg.WorkerCount,
		)

		if emailService != nil && pdfGenerator != nil && excelGenerator != nil {
			if err := telegramBot.UpdateReportServices(emailService, pdfGenerator, excelGenerator); err != nil {
				log.Printf("Warning: Failed to update bot with improved services: %v", err)
			} else {
				log.Println("Successfully initialized improved reporting services")
			}
		}
	}

	searchConfig := search.SearchEngineConfig{
		WorkerCount:    cfg.WorkerCount,
		MaxRetries:     cfg.MaxRetries,
		RetryInterval:  cfg.RetryInterval,
		RequestTimeout: cfg.RequestTimeout,
		ApiKey:         cfg.ApiKey,
	}

	searchEngine := search.NewSearchEngine(database.DB, os.Stdout, searchConfig)

	client := http.Client{Timeout: cfg.RequestTimeout}

	return &Service{
		db:               database,
		config:           cfg,
		pricesLimiter:    rate.NewLimiter(rate.Every(time.Second*6/11), 1), // 10 запросов за 6 секунд
		stocksLimiter:    rate.NewLimiter(rate.Every(time.Minute/200), 10), // 300 запросов в минуту
		warehouseLimiter: rate.NewLimiter(rate.Every(time.Minute/200), 10), // 300 запросов в минуту
		telegramBot:      telegramBot,
		searchEngine:     searchEngine,
		httpClient:       &client,
		ctx:              context.Background(), // Инициализируем контекст по умолчанию
	}, nil
}

// AddSSENotification adds SSE notification to the monitoring service
func (m *Service) AddSSENotification(sseManager *sse.SSEManager) {
	m.sseNotifier = NewSSENotifier(sseManager)
	log.Println("SSE notification added to monitoring service")
}

func (m *Service) RunProductUpdater(ctx context.Context) error {
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

func (m *Service) UpdateProducts(ctx context.Context) error {
	nomenclatureChan := make(chan models.Nomenclature) // Канал для приема номенклатур
	settings := models.Settings{
		Sort:   models.Sort{Ascending: false},
		Filter: models.Filter{WithPhoto: -1, TagIDs: []int{}, TextSearch: "", AllowedCategoriesOnly: true, ObjectIDs: []int{}, Brands: []string{}, ImtID: 0},
		Cursor: models.Cursor{Limit: 20000}}
	locale := ""

	go func() {
		err := m.searchEngine.FetchNomenclaturesChan(ctx, settings, locale, nomenclatureChan)
		if err != nil {
			log.Printf("FetchNomenclaturesChan failed: %v", err)
			close(nomenclatureChan)
		}
	}()

	for nomenclature := range nomenclatureChan {
		if err := m.ProcessNomenclature(ctx, nomenclature); err != nil {
			log.Printf("Error processing nomenclature %d: %v", nomenclature.NmID, err)
			return fmt.Errorf("processing nomenclature %d: %w", nomenclature.NmID, err)
		}
	}

	return nil
}

func (m *Service) ProcessNomenclature(ctx context.Context, nomenclature models.Nomenclature) error {
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

func (m *Service) RunMonitoring(ctx context.Context) error {

	m.ctx = ctx

	// Отправляем приветственное сообщение

	//}

	// Запускаем бота в отдельной горутине
	go m.telegramBot.StartBot(ctx)

	// Запускаем сервис очистки записей

	if m.config.UseImprovedServices {
		if err := m.UpdateWithImprovedComponents(); err != nil {
			log.Printf("Error updating with improved components: %v", err)
		}
	}

	go func() {
		err := m.UpdateWarehouses(ctx)
		if err != nil {
			log.Printf("Error updating Warehouses: %v", err)
		}
	}()

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

func (m *Service) UpdateWarehouses(ctx context.Context) error {
	apiKey := m.config.ApiKey
	if apiKey == "" {
		return fmt.Errorf("API key for Wildberries is not configured")
	}

	err := db.UpdateWarehousesFromAPI(ctx, m.db, m.httpClient, apiKey, m.warehouseLimiter)
	if err != nil {
		return fmt.Errorf("failed to update warehouses from API: %w", err)
	}

	log.Println("Successfully updated warehouses in the database.")
	return nil
}

func (m *Service) ModifiedCheckPriceChanges(ctx context.Context, product *models.ProductRecord, newPrice *models.PriceRecord) error {
	lastPrice, err := m.GetLastPrice(ctx, product.ID)
	if err != nil {
		return fmt.Errorf("getting last price: %w", err)
	}

	// If no previous price - just save the current one
	if lastPrice == nil {
		err := m.SavePrice(ctx, newPrice)
		if err != nil {
			return fmt.Errorf("saving initial price: %w", err)
		}
		return nil
	}

	// Calculate change percentage
	priceDiff := float64(0)
	if lastPrice.Price > 0 {
		priceDiff = (float64(newPrice.Price-lastPrice.Price) / float64(lastPrice.Price)) * 100
	}

	// Save the new price
	err = m.SavePrice(ctx, newPrice)
	if err != nil {
		return fmt.Errorf("saving price: %w", err)
	}

	// If the change is significant - send notification
	if priceDiff <= -m.config.PriceThreshold || priceDiff >= m.config.PriceThreshold {
		// Send Telegram notification (existing code)
		message := fmt.Sprintf(
			"🚨 Обнаружено значительное изменение цены!\n"+
				"Товар: %s (арт. `%s`)\n"+
				"Старая цена: %d руб (скидка %d%%)\n"+
				"Новая цена: %d руб (скидка %d%%)\n"+
				"Изменение: %.2f%%",
			product.Name, product.VendorCode,
			lastPrice.Price, int((1.0-float64(lastPrice.FinalPrice)/float64(lastPrice.Price))*100),
			newPrice.Price, int((1.0-float64(newPrice.FinalPrice)/float64(newPrice.Price))*100),
			priceDiff,
		)

		if err := m.telegramBot.SendTelegramAlertWithParseMode(message, "Markdown"); err != nil {
			log.Printf("Failed to send Telegram alert about price change: %v", err)
		}

		if m.sseNotifier != nil {
			m.sseNotifier.NotifyPriceChange(
				product.ID,
				product.Name,
				product.VendorCode,
				lastPrice.Price,
				newPrice.Price,
				newPrice.Price-lastPrice.Price,
				priceDiff,
			)
		}
	}

	return nil
}

func (m *Service) ModifiedCheckStockChanges(ctx context.Context, product *models.ProductRecord, newStock *models.StockRecord) error {
	lastStock, err := db.GetLastStock(ctx, m.db, product.ID, newStock.WarehouseID)
	if err != nil {
		return fmt.Errorf("getting last stock: %w", err)
	}

	// If no previous stock - just save the current one
	if lastStock == nil {
		err := db.SaveStock(ctx, m.db, newStock)
		if err != nil {
			return fmt.Errorf("saving initial stock: %w", err)
		}
		return nil
	}

	// Calculate change percentage
	stockDiff := float64(0)
	if lastStock.Amount > 0 {
		stockDiff = math.Abs(float64(newStock.Amount-lastStock.Amount)) / float64(lastStock.Amount) * 100
	} else if newStock.Amount > 0 {
		stockDiff = 100 // Product appeared in stock
	}

	err = db.SaveStock(ctx, m.db, newStock)
	if err != nil {
		return fmt.Errorf("saving stock: %w", err)
	}

	if stockDiff <= -m.config.StockThreshold || stockDiff >= m.config.StockThreshold {
		var warehouseName string
		err := m.db.GetContext(ctx, &warehouseName, "SELECT name FROM warehouses WHERE id = $1", newStock.WarehouseID)
		if err != nil {
			warehouseName = fmt.Sprintf("Склад %d", newStock.WarehouseID)
		}

		message := fmt.Sprintf(
			"📦 Обнаружено значительное изменение остатков!\n"+
				"Товар: %s (арт. `%s`)\n"+
				"Склад: %s\n"+
				"Старое количество: %d шт.\n"+
				"Новое количество: %d шт.\n"+
				"Изменение: %.2f%%",
			product.Name, product.VendorCode,
			warehouseName,
			lastStock.Amount,
			newStock.Amount,
			stockDiff,
		)

		if err := m.telegramBot.SendTelegramAlertWithParseMode(message, "Markdown"); err != nil {
			log.Printf("Failed to send Telegram alert about stock change: %v", err)
		}

		if m.sseNotifier != nil {
			m.sseNotifier.NotifyStockChange(
				product.ID,
				product.Name,
				product.VendorCode,
				newStock.WarehouseID,
				warehouseName,
				lastStock.Amount,
				newStock.Amount,
				newStock.Amount-lastStock.Amount,
				stockDiff,
			)
		}
	}

	return nil
}

func (m *Service) runDailyReporting(ctx context.Context) {
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

func (m *Service) SendGreetings(ctx context.Context) error {
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

func (m *Service) ProcessMonitoring(ctx context.Context) error {
	log.Println("Starting monitoring cycle")

	// Добавим таймаут для операции мониторинга, чтобы избежать зависаний
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()

	// Получаем список всех продуктов
	products, err := m.GetAllProducts(ctxWithTimeout)
	if err != nil {
		return fmt.Errorf("failed to get products: %w", err)
	}

	log.Printf("Найдено %d товаров для обработки", len(products))

	// Ограничение на размер пакета продуктов
	// Разбиваем обработку на группы для избежания проблем с памятью
	const maxBatchSize = 500
	var batches [][]models.ProductRecord

	for i := 0; i < len(products); i += maxBatchSize {
		end := i + maxBatchSize
		if end > len(products) {
			end = len(products)
		}
		batches = append(batches, products[i:end])
	}

	log.Printf("Товары разделены на %d групп для обработки", len(batches))

	// Обрабатываем группы последовательно для избежания перегрузки API и базы данных
	for i, batch := range batches {
		log.Printf("Обработка группы %d из %d (%d товаров)", i+1, len(batches), len(batch))

		// Установим таймаут для каждой группы
		batchCtx, batchCancel := context.WithTimeout(ctxWithTimeout, 5*time.Minute)

		// Обработка складских остатков для группы
		if err := m.processStocksForBatch(batchCtx, batch); err != nil {
			log.Printf("Error processing stocks for batch %d: %v", i+1, err)
		}

		// Обработка цен для группы
		if err := m.processPricesForBatch(batchCtx, batch); err != nil {
			log.Printf("Error processing prices for batch %d: %v", i+1, err)
		}

		batchCancel()

		// Небольшая пауза между группами для снижения нагрузки
		time.Sleep(2 * time.Second)
	}

	log.Println("Monitoring cycle completed")
	return nil
}

func (m *Service) processStocksForBatch(ctx context.Context, products []models.ProductRecord) error {
	// Получаем список складов
	warehouses, err := m.GetWarehouses(ctx)
	if err != nil {
		return fmt.Errorf("failed to get warehouses: %w", err)
	}

	// Получаем все ID продуктов для пакетного запроса
	productIDs := make([]int, len(products))
	for i, p := range products {
		productIDs[i] = p.ID
	}

	// Создаем карту продуктов для быстрого доступа
	productMap := make(map[string]*models.ProductRecord)
	for i := range products {
		product := &products[i]
		productMap[product.Barcode] = product
	}

	// Группируем обработку складов для параллельного выполнения
	var wg sync.WaitGroup
	errChan := make(chan error, len(warehouses))

	// Создаем буфер для работы с ограниченным числом складов одновременно
	semaphore := make(chan struct{}, m.config.WorkerCount)

	for _, warehouse := range warehouses {
		wg.Add(1)

		go func(wh models.Warehouse) {
			defer wg.Done()

			// Захватываем слот в семафоре
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			// Проверяем, не был ли контекст отменен
			select {
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			default:
				// Продолжаем выполнение
			}

			if err := m.processWarehouseStocksOptimized(ctx, wh, products, productMap); err != nil {
				log.Printf("Error processing warehouse %d: %v", wh.ID, err)
				errChan <- err
			}
		}(warehouse)
	}

	// Ожидаем завершения всех горутин
	wg.Wait()
	close(errChan)

	// Проверяем, были ли ошибки
	var errs []error
	for err := range errChan {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors occurred during stock processing: %v", errs)
	}

	return nil
}

func (m *Service) processWarehouseStocksOptimized(
	ctx context.Context,
	warehouse models.Warehouse,
	products []models.ProductRecord,
	productMap map[string]*models.ProductRecord,
) error {
	// Разбиваем продукты на партии по 1000 товаров (ограничение API)
	batchSize := 1000

	for i := 0; i < len(products); i += batchSize {
		end := i + batchSize
		if end > len(products) {
			end = len(products)
		}

		batch := products[i:end]

		// Отслеживаем время выполнения запроса
		batchStartTime := time.Now()

		// Собираем баркоды для текущей партии
		var barcodes []string
		for j := range batch {
			product := &batch[j]
			barcodes = append(barcodes, product.Barcode)
		}

		// Запрашиваем остатки с повторными попытками при ошибках
		var stockResp *models.StockResponse
		var err error

		for attempt := 1; attempt <= m.config.MaxRetries; attempt++ {
			// Запрашиваем остатки
			stockResp, err = m.GetStocks(ctx, warehouse.ID, barcodes)
			if err == nil {
				break
			}

			log.Printf("Attempt %d: Error getting stocks for warehouse %d: %v",
				attempt, warehouse.ID, err)

			if attempt < m.config.MaxRetries {
				// Ждем перед повторной попыткой
				select {
				case <-time.After(m.config.RetryInterval):
					// Продолжаем после задержки
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}

		if err != nil {
			return fmt.Errorf("failed to get stocks after %d attempts: %w",
				m.config.MaxRetries, err)
		}

		log.Printf("Получены данные об остатках для %d товаров на складе %d за %v",
			len(barcodes), warehouse.ID, time.Since(batchStartTime))

		// Подготовим массив обновлений для пакетной записи в БД
		stockTime := time.Now()

		// Подготовим массив записей для пакетной вставки в БД
		var stockRecords []models.StockRecord

		for _, stock := range stockResp.Stocks {
			product, ok := productMap[stock.Sku]
			if !ok {
				continue
			}

			newStock := models.StockRecord{
				ProductID:   product.ID,
				WarehouseID: warehouse.ID,
				Amount:      stock.Amount,
				RecordedAt:  stockTime,
			}

			stockRecords = append(stockRecords, newStock)
		}

		// Проверяем изменения остатков и сохраняем данные
		err = m.checkAndSaveStockChanges(ctx, stockRecords)
		if err != nil {
			log.Printf("Error checking and saving stock changes: %v", err)
			return err
		}

		// Небольшая пауза между запросами к API для избежания блокировки
		select {
		case <-time.After(200 * time.Millisecond):
			// Продолжаем после короткой паузы
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

func (m *Service) checkAndSaveStockChanges(ctx context.Context, newStocks []models.StockRecord) error {
	if len(newStocks) == 0 {
		return nil
	}

	// Собираем ID продуктов и складов для пакетного запроса предыдущих остатков
	productIDs := make([]int, 0, len(newStocks))
	warehouseIDs := make([]int64, 0, len(newStocks))
	productWarehouseMap := make(map[string]bool)

	for _, stock := range newStocks {
		key := fmt.Sprintf("%d_%d", stock.ProductID, stock.WarehouseID)
		if !productWarehouseMap[key] {
			productIDs = append(productIDs, stock.ProductID)
			warehouseIDs = append(warehouseIDs, stock.WarehouseID)
			productWarehouseMap[key] = true
		}
	}

	// Получаем последние остатки для всех продуктов и складов одним запросом
	lastStocks, err := db.GetLatestStocksForProducts(ctx, m.db, productIDs, warehouseIDs)
	if err != nil {
		return fmt.Errorf("error fetching last stocks: %w", err)
	}

	// Проверяем изменения и готовим уведомления
	var _ []string

	// Транзакция для пакетного сохранения
	tx, err := m.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Подготовить запрос для пакетной вставки
	stockInsertStmt, err := tx.PrepareContext(ctx, `
        INSERT INTO stocks (product_id, warehouse_id, amount, recorded_at)
        VALUES ($1, $2, $3, $4)
    `)
	if err != nil {
		return fmt.Errorf("failed to prepare stock insert statement: %w", err)
	}
	defer stockInsertStmt.Close()

	// Подготовить запрос для обновления статуса
	updateStatusStmt, err := tx.PrepareContext(ctx, `
        INSERT INTO monitoring_status (product_id, last_stock_check)
        VALUES ($1, NOW())
        ON CONFLICT (product_id)
        DO UPDATE SET last_stock_check = NOW()
    `)
	if err != nil {
		return fmt.Errorf("failed to prepare status update statement: %w", err)
	}
	defer updateStatusStmt.Close()

	// Продукты, для которых нужно отправить уведомление
	productsToNotify := make(map[int]struct{})
	productNames := make(map[int]string) // Кеш имен продуктов для уведомлений

	for _, newStock := range newStocks {
		_ = fmt.Sprintf("%d_%d", newStock.ProductID, newStock.WarehouseID)

		// Проверяем, есть ли предыдущие данные
		var lastStock models.StockRecord
		var lastStockExists bool

		if productMap, ok := lastStocks[newStock.ProductID]; ok {
			lastStock, lastStockExists = productMap[newStock.WarehouseID]
		}

		// Сохраняем новые данные
		_, err := stockInsertStmt.ExecContext(ctx,
			newStock.ProductID,
			newStock.WarehouseID,
			newStock.Amount,
			newStock.RecordedAt)
		if err != nil {
			return fmt.Errorf("failed to insert stock record: %w", err)
		}

		// Обновляем статус проверки
		_, err = updateStatusStmt.ExecContext(ctx, newStock.ProductID)
		if err != nil {
			log.Printf("Warning: failed to update stock check status for product %d: %v",
				newStock.ProductID, err)
		}

		// Проверяем значительные изменения
		if lastStockExists {
			stockDiff := float64(0)

			if lastStock.Amount > 0 {
				stockDiff = math.Abs(float64(newStock.Amount-lastStock.Amount)) / float64(lastStock.Amount) * 100
			} else if newStock.Amount > 0 {
				stockDiff = 100 // Появление товара в наличии
			}

			if stockDiff >= m.config.StockThreshold {
				// Помечаем продукт для уведомления
				productsToNotify[newStock.ProductID] = struct{}{}

				// Получаем данные о продукте для уведомления
				var product models.ProductRecord
				err := tx.GetContext(ctx, &product,
					"SELECT name, vendor_code FROM products WHERE id = $1",
					newStock.ProductID)
				if err == nil {
					productNames[newStock.ProductID] = fmt.Sprintf("%s (арт. `%s`)",
						product.Name, product.VendorCode)
				}
			}
		}
	}

	// Фиксируем транзакцию
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Отправляем уведомления с ограничением частоты
	if len(productsToNotify) > 0 {
		// Ограничиваем количество уведомлений
		maxNotifications := 10
		notificationCount := 0

		for productID := range productsToNotify {
			if notificationCount >= maxNotifications {
				break
			}

			productName, ok := productNames[productID]
			if !ok {
				continue
			}

			message := fmt.Sprintf(
				"📦 Обнаружено значительное изменение остатков!\n"+
					"Товар: %s\n", productName)

			if err := m.telegramBot.SendTelegramAlertWithParseMode(message, "Markdown"); err != nil {
				log.Printf("Failed to send Telegram alert about stock change: %v", err)
			}

			notificationCount++
		}

		// Если уведомлений больше, чем лимит, добавляем общее сообщение
		if len(productsToNotify) > maxNotifications {
			extraMessage := fmt.Sprintf(
				"... и еще %d товаров с изменениями остатков",
				len(productsToNotify)-maxNotifications)

			if err := m.telegramBot.SendTelegramAlert(extraMessage); err != nil {
				log.Printf("Failed to send extra notification message: %v", err)
			}
		}
	}

	return nil
}

func (m *Service) processPricesForBatch(ctx context.Context, products []models.ProductRecord) error {
	// Собираем nmIDs для API запроса
	nmIDs := make([]int, 0, len(products))
	nmIDToProduct := make(map[int]*models.ProductRecord)

	for i := range products {
		product := &products[i]
		nmIDs = append(nmIDs, product.NmID)
		nmIDToProduct[product.NmID] = product
	}

	// Обрабатываем товары с пагинацией
	limit := 1000
	offset := 0

	for {
		// Следим за таймаутом контекста
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Продолжаем выполнение
		}

		// Запрашиваем цены с повторными попытками при ошибках
		var goodsResp *models.GoodsPricesResponse
		var err error

		for attempt := 1; attempt <= m.config.MaxRetries; attempt++ {
			goodsResp, err = m.GetGoodsPrices(ctx, limit, offset, 0) // 0 - без фильтра по nmID
			if err == nil {
				break
			}

			log.Printf("Attempt %d: Error getting goods prices: %v", attempt, err)

			if attempt < m.config.MaxRetries {
				// Ждем перед повторной попыткой
				select {
				case <-time.After(m.config.RetryInterval):
					// Продолжаем после задержки
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}

		if err != nil {
			return fmt.Errorf("failed to get goods prices after %d attempts: %w",
				m.config.MaxRetries, err)
		}

		// Если нет данных, выходим из цикла
		if len(goodsResp.Data.ListGoods) == 0 {
			break
		}

		log.Printf("Получено %d товаров с ценами (offset: %d)",
			len(goodsResp.Data.ListGoods), offset)

		convertedGoods := make([]models.GoodsPricesResponseListGoods, len(goodsResp.Data.ListGoods))
		for i, good := range goodsResp.Data.ListGoods {
			convertedGoods[i] = models.GoodsPricesResponseListGoods{
				NmID:         good.NmID,
				VendorCode:   good.VendorCode,
				Sizes:        good.Sizes,
				Discount:     good.Discount,
				ClubDiscount: good.ClubDiscount,
			}
		}

		// Пакетная обработка полученных цен
		err = m.processPriceBatch(ctx, convertedGoods, nmIDToProduct)
		if err != nil {
			log.Printf("Error processing price batch: %v", err)
			return err
		}

		// Если получено меньше данных, чем запрошено - выходим из цикла
		if len(goodsResp.Data.ListGoods) < limit {
			break
		}

		// Увеличиваем смещение для следующего запроса
		offset += limit

		// Пауза между запросами
		select {
		case <-time.After(200 * time.Millisecond):
			// Продолжаем после короткой паузы
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

func (m *Service) processPriceBatch(
	ctx context.Context,
	goods []models.GoodsPricesResponseListGoods,
	nmIDToProduct map[int]*models.ProductRecord,
) error {
	if len(goods) == 0 {
		return nil
	}

	// Подготовка данных для пакетной обработки
	var priceRecords []models.PriceRecord
	var productIDs []int

	recordTime := time.Now()

	for _, good := range goods {
		product, ok := nmIDToProduct[good.NmID]
		if !ok {
			continue
		}

		productIDs = append(productIDs, product.ID)

		// Обрабатываем каждый размер товара
		for _, size := range good.Sizes {
			finalPrice := size.DiscountedPrice
			clubPrice := size.ClubDiscountedPrice

			priceRecords = append(priceRecords, models.PriceRecord{
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
				RecordedAt:        recordTime,
			})
		}
	}

	// Получаем последние цены для сравнения
	lastPrices, err := db.GetLatestPricesForProducts(ctx, m.db, productIDs)
	if err != nil {
		return fmt.Errorf("error fetching last prices: %w", err)
	}

	// Транзакция для пакетной вставки
	tx, err := m.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Подготовка запросов
	priceInsertStmt, err := tx.PrepareContext(ctx, `
        INSERT INTO prices (
            product_id, size_id, price, discount, club_discount, final_price, 
            club_final_price, currency_iso_code, tech_size_name, editable_size_price, recorded_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
    `)
	if err != nil {
		return fmt.Errorf("failed to prepare price insert statement: %w", err)
	}
	defer priceInsertStmt.Close()

	updateStatusStmt, err := tx.PrepareContext(ctx, `
        INSERT INTO monitoring_status (product_id, last_price_check)
        VALUES ($1, NOW())
        ON CONFLICT (product_id)
        DO UPDATE SET last_price_check = NOW()
    `)
	if err != nil {
		return fmt.Errorf("failed to prepare status update statement: %w", err)
	}
	defer updateStatusStmt.Close()

	// Продукты со значительными изменениями цен для уведомлений
	significantChanges := make(map[int]struct{})
	productChangeInfo := make(map[int]struct {
		Name          string
		VendorCode    string
		OldPrice      int
		OldDiscount   int
		NewPrice      int
		NewDiscount   int
		ChangePercent float64
	})

	// Обрабатываем и сохраняем данные
	for _, newPrice := range priceRecords {
		// Поиск последней цены
		lastPrice, hasLastPrice := lastPrices[newPrice.ProductID]

		// Сохраняем новую цену
		_, err := priceInsertStmt.ExecContext(ctx,
			newPrice.ProductID, newPrice.SizeID, newPrice.Price,
			newPrice.Discount, newPrice.ClubDiscount, newPrice.FinalPrice,
			newPrice.ClubFinalPrice, newPrice.CurrencyIsoCode,
			newPrice.TechSizeName, newPrice.EditableSizePrice, newPrice.RecordedAt)
		if err != nil {
			return fmt.Errorf("failed to insert price record: %w", err)
		}

		// Обновляем статус проверки
		_, err = updateStatusStmt.ExecContext(ctx, newPrice.ProductID)
		if err != nil {
			log.Printf("Warning: failed to update price check status for product %d: %v",
				newPrice.ProductID, err)
		}

		// Проверяем значительные изменения цен
		if hasLastPrice && lastPrice.SizeID == newPrice.SizeID {
			priceDiff := float64(0)

			if lastPrice.Price > 0 {
				priceDiff = (float64(newPrice.Price-lastPrice.Price) / float64(lastPrice.Price)) * 100
			}

			if priceDiff <= -m.config.PriceThreshold || priceDiff >= m.config.PriceThreshold {
				significantChanges[newPrice.ProductID] = struct{}{}

				// Получаем информацию о продукте для уведомления
				var product models.ProductRecord
				err := tx.GetContext(ctx, &product,
					"SELECT name, vendor_code FROM products WHERE id = $1",
					newPrice.ProductID)
				if err == nil {
					productChangeInfo[newPrice.ProductID] = struct {
						Name          string
						VendorCode    string
						OldPrice      int
						OldDiscount   int
						NewPrice      int
						NewDiscount   int
						ChangePercent float64
					}{
						Name:          product.Name,
						VendorCode:    product.VendorCode,
						OldPrice:      lastPrice.FinalPrice,
						OldDiscount:   lastPrice.Discount,
						NewPrice:      newPrice.FinalPrice,
						NewDiscount:   newPrice.Discount,
						ChangePercent: priceDiff,
					}
				}
			}
		}
	}

	// Фиксируем транзакцию
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Отправляем уведомления с ограничением частоты
	if len(significantChanges) > 0 {
		// Ограничиваем количество уведомлений
		maxNotifications := 10
		notificationCount := 0

		for productID := range significantChanges {
			if notificationCount >= maxNotifications {
				break
			}

			changeInfo, ok := productChangeInfo[productID]
			if !ok {
				continue
			}

			message := fmt.Sprintf(
				"🚨 Обнаружено значительное изменение цены!\n"+
					"Товар: %s (арт. `%s`)\n"+
					"Старая цена: %d руб (скидка %d%%)\n"+
					"Новая цена: %d руб (скидка %d%%)\n"+
					"Изменение: %.2f%%",
				changeInfo.Name, changeInfo.VendorCode,
				changeInfo.OldPrice, changeInfo.OldDiscount,
				changeInfo.NewPrice, changeInfo.NewDiscount,
				changeInfo.ChangePercent)

			if err := m.telegramBot.SendTelegramAlertWithParseMode(message, "Markdown"); err != nil {
				log.Printf("Failed to send Telegram alert about price change: %v", err)
			}

			notificationCount++
		}

		// Если уведомлений больше, чем лимит, добавляем общее сообщение
		if len(significantChanges) > maxNotifications {
			extraMessage := fmt.Sprintf(
				"... и еще %d товаров с изменениями цен",
				len(significantChanges)-maxNotifications)

			if err := m.telegramBot.SendTelegramAlert(extraMessage); err != nil {
				log.Printf("Failed to send extra price notification message: %v", err)
			}
		}
	}

	return nil
}

func (m *Service) GetAllProducts(ctx context.Context) ([]models.ProductRecord, error) {
	var products []models.ProductRecord
	query := `SELECT id, nm_id, vendor_code, barcode, name, created_at FROM products`

	if err := m.db.SelectContext(ctx, &products, query); err != nil {
		return nil, fmt.Errorf("fetching products from DB: %w", err)
	}

	return products, nil
}

func (m *Service) processStocks(ctx context.Context, products []models.ProductRecord) error {
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

func (m *Service) processWarehouseStocks(ctx context.Context, warehouse models.Warehouse, products []models.ProductRecord) error {
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

func (m *Service) processPrices(ctx context.Context, products []models.ProductRecord) error {
	// Разбиваем продукты на группы для запросов с лимитом

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

func (m *Service) processPriceRecord(ctx context.Context, product *models.ProductRecord, size models.GoodSize) error {
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

func (m *Service) CheckPriceChanges(ctx context.Context, product *models.ProductRecord, newPrice *models.PriceRecord) error {
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
	if lastPrice.Price > 0 {
		priceDiff = (float64(newPrice.Price-lastPrice.Price) / float64(lastPrice.Price)) * 100
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
			lastPrice.Price, int((1.0-float64(lastPrice.FinalPrice)/float64(lastPrice.Price))*100),
			newPrice.Price, int((1.0-float64(newPrice.FinalPrice)/float64(newPrice.Price))*100),
			priceDiff,
		)

		if err := m.telegramBot.SendTelegramAlertWithParseMode(message, "Markdown"); err != nil {
			log.Printf("Failed to send Telegram alert about price change: %v", err)
		}
	}

	return nil
}

func (m *Service) CheckStockChanges(ctx context.Context, product *models.ProductRecord, newStock *models.StockRecord) error {
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

func (m *Service) GetWarehouses(ctx context.Context) ([]models.Warehouse, error) {
	return api.GetWarehouses(ctx, m.httpClient, m.config.ApiKey, m.warehouseLimiter)
}

func (m *Service) GetStocks(ctx context.Context, warehouseID int64, skus []string) (*models.StockResponse, error) {
	return api.GetStocks(ctx, m.httpClient, m.config.ApiKey, m.stocksLimiter, warehouseID, skus)
}

func (m *Service) GetPriceHistory(ctx context.Context, uploadID int, limit, offset int) (*models.PriceHistoryResponse, error) {
	return api.GetPriceHistory(ctx, m.httpClient, m.config.ApiKey, m.pricesLimiter, uploadID, limit, offset)
}

func (m *Service) GetGoodsPrices(ctx context.Context, limit int, offset int, filterNmID int) (*models.GoodsPricesResponse, error) {
	return api.GetGoodsPrices(ctx, m.httpClient, m.config.ApiKey, m.pricesLimiter, limit, offset, filterNmID)
}

func (m *Service) GetLastPrice(ctx context.Context, productID int) (*models.PriceRecord, error) {
	return db.GetLastPrice(ctx, m.db, productID)
}

func (m *Service) GetLastStock(ctx context.Context, productID int, warehouseID int64) (*models.StockRecord, error) {
	return db.GetLastStock(ctx, m.db, productID, warehouseID)
}

func (m *Service) SavePrice(ctx context.Context, price *models.PriceRecord) error {
	return db.SavePrice(ctx, m.db, price)
}

func (m *Service) SaveStock(ctx context.Context, stock *models.StockRecord) error {
	return db.SaveStock(ctx, m.db, stock)
}

func (m *Service) InitDB() error {
	return db.InitDB(m.db)
}

func (m *Service) GetProductCount(ctx context.Context) (int, error) {
	return db.GetProductCount(ctx, m.db)
}

func (m *Service) UpdatePriceCheckStatus(ctx context.Context, productID int) error {
	return db.UpdatePriceCheckStatus(ctx, m.db, productID)
}

func (m *Service) UpdateStockCheckStatus(ctx context.Context, productID int) error {
	return db.UpdateStockCheckStatus(ctx, m.db, productID)
}

func (m *Service) GetDB() *sqlx.DB {
	return m.db
}
