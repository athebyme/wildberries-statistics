package report

import (
	"context"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/xuri/excelize/v2"
	"wbmonitoring/monitoring/internal/db"
	"wbmonitoring/monitoring/internal/models"
)

type ExcelGenerator struct {
	db             *sqlx.DB
	workerPoolSize int
	reportConfig   ReportConfig
}

func NewExcelGenerator(db *sqlx.DB, reportConfig ReportConfig, workerPoolSize int) *ExcelGenerator {
	if workerPoolSize <= 0 {
		workerPoolSize = 5
	}

	return &ExcelGenerator{
		db:             db,
		workerPoolSize: workerPoolSize,
		reportConfig:   reportConfig,
	}
}

func (g *ExcelGenerator) GenerateStockReportExcel(ctx context.Context, startDate, endDate time.Time) (string, string, error) {
	// Загрузка продуктов и складов
	type productsResult struct {
		Products []models.ProductRecord
		Error    error
	}

	type warehousesResult struct {
		Warehouses []models.Warehouse
		Error      error
	}

	productsCh := make(chan productsResult, 1)
	warehousesCh := make(chan warehousesResult, 1)

	go func() {
		products, err := db.GetAllProducts(ctx, g.db)
		productsCh <- productsResult{Products: products, Error: err}
	}()

	go func() {
		warehouses, err := db.GetAllWarehouses(ctx, g.db)
		warehousesCh <- warehousesResult{Warehouses: warehouses, Error: err}
	}()

	productsRes := <-productsCh
	warehousesRes := <-warehousesCh

	if productsRes.Error != nil {
		return "", "", fmt.Errorf("error getting products: %w", productsRes.Error)
	}

	if warehousesRes.Error != nil {
		return "", "", fmt.Errorf("error getting warehouses: %w", warehousesRes.Error)
	}

	products := productsRes.Products
	warehouses := warehousesRes.Warehouses

	if len(products) == 0 {
		return "", "", fmt.Errorf("no products found in database")
	}

	// Подготовка данных для пакетных запросов
	productIDs := make([]int, len(products))
	for i, p := range products {
		productIDs[i] = p.ID
	}

	warehouseIDs := make([]int64, len(warehouses))
	for i, w := range warehouses {
		warehouseIDs[i] = w.ID
	}

	// Получение остатков для всех продуктов и складов одним запросом
	productStocks, err := db.GetBatchStocksForProducts(ctx, g.db, productIDs, warehouseIDs, startDate, endDate)
	if err != nil {
		return "", "", fmt.Errorf("error fetching stocks data: %w", err)
	}

	// Создание Excel-файла
	f := excelize.NewFile()
	defer func() {
		if err := f.Close(); err != nil {
			log.Printf("Error closing Excel file: %v", err)
		}
	}()

	const summarySheetName = "Stock Summary"
	f.SetSheetName("Sheet1", summarySheetName)

	const detailSheetName = "Stock by Warehouse"
	_, err = f.NewSheet(detailSheetName)
	if err != nil {
		return "", "", fmt.Errorf("error creating detail sheet: %w", err)
	}

	// Создание стилей и заголовков
	headerStyle, numberStyle, percentStyle, err := g.createExcelStyles(f)
	if err != nil {
		return "", "", fmt.Errorf("error creating excel styles: %w", err)
	}

	// Добавляем заголовки
	summaryHeaders := []string{
		"Product", "Vendor Code", "Initial Stock", "Final Stock",
		"Change", "Change (%)", "Records",
	}

	g.addHeaders(f, summarySheetName, summaryHeaders, headerStyle)

	detailHeaders := []string{
		"Товар", "Артикул", "Склад", "Начальный остаток", "Конечный остаток",
		"Изменение", "Изменение (%)", "Мин. остаток", "Макс. остаток", "Записей",
	}

	g.addHeaders(f, detailSheetName, detailHeaders, headerStyle)

	// Подготовка данных для отчета
	var summaries []models.ProductSummary
	var details []models.WarehouseDetail

	for _, product := range products {
		totalInitialStock := 0
		totalFinalStock := 0
		totalRecords := 0
		hasData := false

		for _, warehouse := range warehouses {
			stocks, exists := getProductWarehouseStocks(productStocks, product.ID, warehouse.ID)
			if !exists || len(stocks) == 0 {
				continue
			}

			hasData = true
			initialStock := stocks[0].Amount
			finalStock := stocks[len(stocks)-1].Amount
			totalInitialStock += initialStock
			totalFinalStock += finalStock
			totalRecords += len(stocks)

			minStock, maxStock := initialStock, initialStock
			for _, stock := range stocks {
				if stock.Amount < minStock {
					minStock = stock.Amount
				}
				if stock.Amount > maxStock {
					maxStock = stock.Amount
				}
			}

			stockChange := finalStock - initialStock
			changePercent := 0.0
			if initialStock > 0 {
				changePercent = float64(stockChange) / float64(initialStock) * 100
			} else if finalStock > 0 {
				changePercent = 100.0
			}

			hasSignificantChange := math.Abs(changePercent) >= g.reportConfig.MinStockChangePercent

			details = append(details, models.WarehouseDetail{
				Product:           product,
				Warehouse:         warehouse,
				InitialStock:      initialStock,
				FinalStock:        finalStock,
				Change:            stockChange,
				ChangePercent:     changePercent,
				MinStock:          minStock,
				MaxStock:          maxStock,
				RecordsCount:      len(stocks),
				HasSignificantChg: hasSignificantChange,
			})
		}

		if !hasData {
			continue
		}

		totalChange := totalFinalStock - totalInitialStock
		totalChangePercent := 0.0
		if totalInitialStock > 0 {
			totalChangePercent = float64(totalChange) / float64(totalInitialStock) * 100
		} else if totalFinalStock > 0 {
			totalChangePercent = 100.0
		}

		hasSignificantChange := math.Abs(totalChangePercent) >= g.reportConfig.MinStockChangePercent

		summaries = append(summaries, models.ProductSummary{
			Product:           product,
			TotalInitialStock: totalInitialStock,
			TotalFinalStock:   totalFinalStock,
			TotalChange:       totalChange,
			ChangePercent:     totalChangePercent,
			RecordsCount:      totalRecords,
			HasSignificantChg: hasSignificantChange,
		})
	}

	// Сортировка результатов
	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].Product.Name < summaries[j].Product.Name
	})

	sort.Slice(details, func(i, j int) bool {
		if details[i].Product.Name != details[j].Product.Name {
			return details[i].Product.Name < details[j].Product.Name
		}
		return details[i].Warehouse.Name < details[j].Warehouse.Name
	})

	// Заполнение листа с итогами
	summaryRow := 2
	for _, summary := range summaries {
		f.SetCellValue(summarySheetName, fmt.Sprintf("A%d", summaryRow), summary.Product.Name)
		f.SetCellValue(summarySheetName, fmt.Sprintf("B%d", summaryRow), summary.Product.VendorCode)
		f.SetCellValue(summarySheetName, fmt.Sprintf("C%d", summaryRow), summary.TotalInitialStock)
		f.SetCellValue(summarySheetName, fmt.Sprintf("D%d", summaryRow), summary.TotalFinalStock)
		f.SetCellValue(summarySheetName, fmt.Sprintf("E%d", summaryRow), summary.TotalChange)
		f.SetCellValue(summarySheetName, fmt.Sprintf("F%d", summaryRow), summary.ChangePercent/100)
		f.SetCellValue(summarySheetName, fmt.Sprintf("G%d", summaryRow), summary.RecordsCount)

		summaryRow++
	}

	// Заполнение детального листа
	detailRow := 2
	for _, detail := range details {
		f.SetCellValue(detailSheetName, fmt.Sprintf("A%d", detailRow), detail.Product.Name)
		f.SetCellValue(detailSheetName, fmt.Sprintf("B%d", detailRow), detail.Product.VendorCode)
		f.SetCellValue(detailSheetName, fmt.Sprintf("C%d", detailRow), detail.Warehouse.Name)
		f.SetCellValue(detailSheetName, fmt.Sprintf("D%d", detailRow), detail.InitialStock)
		f.SetCellValue(detailSheetName, fmt.Sprintf("E%d", detailRow), detail.FinalStock)
		f.SetCellValue(detailSheetName, fmt.Sprintf("F%d", detailRow), detail.Change)
		f.SetCellValue(detailSheetName, fmt.Sprintf("G%d", detailRow), detail.ChangePercent/100)
		f.SetCellValue(detailSheetName, fmt.Sprintf("H%d", detailRow), detail.MinStock)
		f.SetCellValue(detailSheetName, fmt.Sprintf("I%d", detailRow), detail.MaxStock)
		f.SetCellValue(detailSheetName, fmt.Sprintf("J%d", detailRow), detail.RecordsCount)

		detailRow++
	}

	// Применение стилей
	if summaryRow > 2 {
		f.SetCellStyle(summarySheetName, "C2", fmt.Sprintf("E%d", summaryRow-1), numberStyle)
		f.SetCellStyle(summarySheetName, "G2", fmt.Sprintf("G%d", summaryRow-1), numberStyle)
		f.SetCellStyle(summarySheetName, "F2", fmt.Sprintf("F%d", summaryRow-1), percentStyle)
	}

	if detailRow > 2 {
		f.SetCellStyle(detailSheetName, "D2", fmt.Sprintf("F%d", detailRow-1), numberStyle)
		f.SetCellStyle(detailSheetName, "H2", fmt.Sprintf("J%d", detailRow-1), numberStyle)
		f.SetCellStyle(detailSheetName, "G2", fmt.Sprintf("G%d", detailRow-1), percentStyle)
	}

	// Добавление листа с трендами (с ограничением количества продуктов)
	g.addStockTrendSheetOptimized(ctx, f, products, warehouses, productStocks, startDate, endDate)

	// Настройка ширины столбцов
	g.adjustColumnWidths(f, summarySheetName, summaryHeaders)
	g.adjustColumnWidths(f, detailSheetName, detailHeaders)

	// Сохранение файла
	filename := fmt.Sprintf("stock_report_%s_%s.xlsx",
		startDate.Format("02-01-2006"),
		endDate.Format("02-01-2006"))
	filePath := filepath.Join(os.TempDir(), filename)

	if err := f.SaveAs(filePath); err != nil {
		return "", "", fmt.Errorf("error saving Excel file: %w", err)
	}

	return filePath, filename, nil
}

// Оптимизированная версия для добавления листа с трендами
func (g *ExcelGenerator) addStockTrendSheetOptimized(
	ctx context.Context,
	f *excelize.File,
	products []models.ProductRecord,
	warehouses []models.Warehouse,
	productStocks map[int]map[int64][]models.StockRecord,
	startDate, endDate time.Time,
) error {
	trendSheetName := "Stock Trends"
	_, err := f.NewSheet(trendSheetName)
	if err != nil {
		return fmt.Errorf("error creating trend sheet: %w", err)
	}

	headers := []string{
		"Товар", "Артикул", "Склад", "Дата/Время", "Предыдущий остаток",
		"Новый остаток", "Изменение", "Изменение (%)",
	}

	headerStyle, err := f.NewStyle(&excelize.Style{
		Font:      &excelize.Font{Bold: true},
		Fill:      excelize.Fill{Type: "pattern", Color: []string{"#DDEBF7"}, Pattern: 1},
		Alignment: &excelize.Alignment{Horizontal: "center"},
		Border: []excelize.Border{
			{Type: "top", Color: "#000000", Style: 1},
			{Type: "left", Color: "#000000", Style: 1},
			{Type: "right", Color: "#000000", Style: 1},
			{Type: "bottom", Color: "#000000", Style: 1},
		},
	})
	if err != nil {
		return fmt.Errorf("error creating header style: %w", err)
	}

	g.addHeaders(f, trendSheetName, headers, headerStyle)

	// Анализ трендов только для ограниченного числа продуктов с наибольшими изменениями
	type ProductWithChange struct {
		Product       models.ProductRecord
		ChangePercent float64
	}

	var topChanges []ProductWithChange
	maxProductsToAnalyze := 20 // Ограничиваем количество продуктов для анализа трендов

	for _, product := range products {
		totalInitialStock := 0
		totalFinalStock := 0
		hasData := false

		for _, warehouse := range warehouses {
			stocks, exists := getProductWarehouseStocks(productStocks, product.ID, warehouse.ID)
			if !exists || len(stocks) == 0 {
				continue
			}

			initialStock := stocks[0].Amount
			finalStock := stocks[len(stocks)-1].Amount
			totalInitialStock += initialStock
			totalFinalStock += finalStock
			hasData = true
		}

		if !hasData || (totalInitialStock == 0 && totalFinalStock == 0) {
			continue
		}

		changePercent := 0.0
		if totalInitialStock > 0 {
			changePercent = math.Abs(float64(totalFinalStock-totalInitialStock) / float64(totalInitialStock) * 100)
		} else if totalFinalStock > 0 {
			changePercent = 100.0
		}

		if changePercent >= g.reportConfig.MinStockChangePercent {
			topChanges = append(topChanges, ProductWithChange{
				Product:       product,
				ChangePercent: changePercent,
			})
		}
	}

	// Сортируем по убыванию процента изменения
	sort.Slice(topChanges, func(i, j int) bool {
		return topChanges[i].ChangePercent > topChanges[j].ChangePercent
	})

	// Ограничиваем количество продуктов для детального анализа
	if len(topChanges) > maxProductsToAnalyze {
		topChanges = topChanges[:maxProductsToAnalyze]
	}

	row := 2

	// Добавляем значительные изменения в таблицу трендов
	for _, productChange := range topChanges {
		product := productChange.Product

		for _, warehouse := range warehouses {
			stocks, exists := getProductWarehouseStocks(productStocks, product.ID, warehouse.ID)
			if !exists || len(stocks) < 2 {
				continue
			}

			firstEntryForProductWarehouse := true

			// Анализируем только значительные изменения
			for i := 1; i < len(stocks); i++ {
				prevStock := stocks[i-1].Amount
				newStock := stocks[i].Amount

				if prevStock == newStock {
					continue
				}

				stockChange := newStock - prevStock
				changePercent := 0.0
				if prevStock > 0 {
					changePercent = float64(stockChange) / float64(prevStock) * 100
				} else if newStock > 0 {
					changePercent = 100.0
				}

				if math.Abs(changePercent) < g.reportConfig.MinStockChangePercent {
					continue
				}

				// Заполняем данные
				if firstEntryForProductWarehouse {
					f.SetCellValue(trendSheetName, fmt.Sprintf("A%d", row), product.Name)
					f.SetCellValue(trendSheetName, fmt.Sprintf("B%d", row), product.VendorCode)
					firstEntryForProductWarehouse = false
				} else {
					f.SetCellValue(trendSheetName, fmt.Sprintf("A%d", row), "")
					f.SetCellValue(trendSheetName, fmt.Sprintf("B%d", row), "")
				}

				f.SetCellValue(trendSheetName, fmt.Sprintf("C%d", row), warehouse.Name)
				f.SetCellValue(trendSheetName, fmt.Sprintf("D%d", row), stocks[i].RecordedAt.Format("02.01.2006 15:04:05"))
				f.SetCellValue(trendSheetName, fmt.Sprintf("E%d", row), prevStock)
				f.SetCellValue(trendSheetName, fmt.Sprintf("F%d", row), newStock)
				f.SetCellValue(trendSheetName, fmt.Sprintf("G%d", row), stockChange)
				f.SetCellValue(trendSheetName, fmt.Sprintf("H%d", row), changePercent/100)

				row++
			}

			if !firstEntryForProductWarehouse {
				row++ // Добавляем пустую строку между складами
			}
		}
	}

	// Применяем стили
	if row > 2 {
		numberStyle, _ := f.NewStyle(&excelize.Style{NumFmt: 1})
		percentStyle, _ := f.NewStyle(&excelize.Style{NumFmt: 10})

		f.SetCellStyle(trendSheetName, "E2", fmt.Sprintf("G%d", row-1), numberStyle)
		f.SetCellStyle(trendSheetName, "H2", fmt.Sprintf("H%d", row-1), percentStyle)
	}

	// Настройка ширины столбцов
	g.adjustColumnWidths(f, trendSheetName, headers)
	f.SetColWidth(trendSheetName, "D", "D", 20) // Для даты/времени

	return nil
}

func (g *ExcelGenerator) createExcelStyles(f *excelize.File) (headerStyle, numberStyle, percentStyle int, err error) {
	headerStyle, err = f.NewStyle(&excelize.Style{
		Font:      &excelize.Font{Bold: true},
		Fill:      excelize.Fill{Type: "pattern", Color: []string{"#DDEBF7"}, Pattern: 1},
		Alignment: &excelize.Alignment{Horizontal: "center", Vertical: "center"},
		Border: []excelize.Border{
			{Type: "top", Color: "#000000", Style: 1},
			{Type: "left", Color: "#000000", Style: 1},
			{Type: "right", Color: "#000000", Style: 1},
			{Type: "bottom", Color: "#000000", Style: 1},
		},
	})
	if err != nil {
		return 0, 0, 0, fmt.Errorf("error creating header style: %w", err)
	}

	numberStyle, err = f.NewStyle(&excelize.Style{
		NumFmt: 1, // Целое число
	})
	if err != nil {
		return 0, 0, 0, fmt.Errorf("error creating number style: %w", err)
	}

	percentStyle, err = f.NewStyle(&excelize.Style{
		NumFmt: 10, // Процент
	})
	if err != nil {
		return 0, 0, 0, fmt.Errorf("error creating percentage style: %w", err)
	}

	return headerStyle, numberStyle, percentStyle, nil
}

func (g *ExcelGenerator) addHeaders(f *excelize.File, sheetName string, headers []string, headerStyle int) {
	for i, header := range headers {
		cell := fmt.Sprintf("%c1", 'A'+i)
		f.SetCellValue(sheetName, cell, header)
	}

	f.SetCellStyle(sheetName, "A1", string(rune('A'+len(headers)-1))+"1", headerStyle)
}

func (g *ExcelGenerator) adjustColumnWidths(f *excelize.File, sheetName string, headers []string) {
	for i := range headers {
		col := string(rune('A' + i))
		width, _ := f.GetColWidth(sheetName, col)
		if width < 15 {
			f.SetColWidth(sheetName, col, col, 15)
		}
	}

	f.SetColWidth(sheetName, "A", "A", 30) // Для названия товара
}

// Функция для получения остатков продукта на складе из кеша
func getProductWarehouseStocks(stocksMap map[int]map[int64][]models.StockRecord, productID int, warehouseID int64) ([]models.StockRecord, bool) {
	productStocks, ok := stocksMap[productID]
	if !ok {
		return nil, false
	}

	warehouseStocks, ok := productStocks[warehouseID]
	return warehouseStocks, ok
}

func (g *ExcelGenerator) addStockTrendSheet(ctx context.Context, f *excelize.File, products []models.ProductRecord, warehouses []models.Warehouse, startDate, endDate time.Time) error {
	trendSheetName := "Stock Trends"
	_, err := f.NewSheet(trendSheetName)
	if err != nil {
		return fmt.Errorf("error creating trend sheet: %w", err)
	}

	headerStyle, err := f.NewStyle(&excelize.Style{
		Font:      &excelize.Font{Bold: true},
		Fill:      excelize.Fill{Type: "pattern", Color: []string{"#DDEBF7"}, Pattern: 1},
		Alignment: &excelize.Alignment{Horizontal: "center"},
		Border: []excelize.Border{
			{Type: "top", Color: "#000000", Style: 1},
			{Type: "left", Color: "#000000", Style: 1},
			{Type: "right", Color: "#000000", Style: 1},
			{Type: "bottom", Color: "#000000", Style: 1},
		},
	})
	if err != nil {
		return fmt.Errorf("error creating header style: %w", err)
	}

	headers := []string{
		"Товар", "Артикул", "Дата/Время", "Предыдущая цена (₽)",
		"Новая цена (₽)", "Изменение (₽)", "Изменение (%)", "Скидка (%)",
	}

	for i, header := range headers {
		cell := fmt.Sprintf("%c1", 'A'+i)
		f.SetCellValue(trendSheetName, cell, header)
	}

	f.SetCellStyle(trendSheetName, "A1", string(rune('A'+len(headers)-1))+"1", headerStyle)

	sem := make(chan struct{}, g.workerPoolSize)

	row := 2
	productsProcessed := 0
	maxProductsToProcess := 20

	for _, product := range products {
		if productsProcessed >= maxProductsToProcess {
			break
		}

		sem <- struct{}{}

		var significantChanges []struct {
			Warehouse     models.Warehouse
			Time          time.Time
			PreviousStock int
			NewStock      int
			Change        int
			ChangePercent float64
		}

		hasSignificantChanges := false

		for _, warehouse := range warehouses {

			stocks, err := db.GetStocksForPeriod(ctx, g.db, product.ID, warehouse.ID, startDate, endDate)
			if err != nil {
				log.Printf("Error getting stocks for product %d, warehouse %d: %v",
					product.ID, warehouse.ID, err)
				continue
			}

			if len(stocks) < 2 {
				continue
			}

			for i := 1; i < len(stocks); i++ {
				previousStock := stocks[i-1].Amount
				newStock := stocks[i].Amount

				if previousStock == newStock {
					continue
				}

				stockChange := newStock - previousStock
				changePercent := 0.0
				if previousStock > 0 {
					changePercent = float64(stockChange) / float64(previousStock) * 100
				} else if newStock > 0 {
					changePercent = 100.0
				}

				if math.Abs(changePercent) >= g.reportConfig.MinStockChangePercent {
					significantChanges = append(significantChanges, struct {
						Warehouse     models.Warehouse
						Time          time.Time
						PreviousStock int
						NewStock      int
						Change        int
						ChangePercent float64
					}{
						Warehouse:     warehouse,
						Time:          stocks[i].RecordedAt,
						PreviousStock: previousStock,
						NewStock:      newStock,
						Change:        stockChange,
						ChangePercent: changePercent,
					})

					hasSignificantChanges = true
				}
			}
		}

		<-sem

		if !hasSignificantChanges {
			continue
		}

		productsProcessed++
		firstEntryForProduct := true

		sort.Slice(significantChanges, func(i, j int) bool {
			return significantChanges[i].Time.Before(significantChanges[j].Time)
		})

		for _, change := range significantChanges {

			if firstEntryForProduct {
				f.SetCellValue(trendSheetName, fmt.Sprintf("A%d", row), product.Name)
				f.SetCellValue(trendSheetName, fmt.Sprintf("B%d", row), product.VendorCode)
				firstEntryForProduct = false
			} else {
				f.SetCellValue(trendSheetName, fmt.Sprintf("A%d", row), "")
				f.SetCellValue(trendSheetName, fmt.Sprintf("B%d", row), "")
			}

			f.SetCellValue(trendSheetName, fmt.Sprintf("C%d", row), change.Warehouse.Name)
			f.SetCellValue(trendSheetName, fmt.Sprintf("D%d", row), change.Time.Format("02.01.2006 15:04:05"))
			f.SetCellValue(trendSheetName, fmt.Sprintf("E%d", row), change.PreviousStock)
			f.SetCellValue(trendSheetName, fmt.Sprintf("F%d", row), change.NewStock)
			f.SetCellValue(trendSheetName, fmt.Sprintf("G%d", row), change.Change)
			f.SetCellValue(trendSheetName, fmt.Sprintf("H%d", row), change.ChangePercent/100)

			row++
		}

		row++
	}

	if row > 2 {
		numberStyle, _ := f.NewStyle(&excelize.Style{NumFmt: 1})
		percentStyle, _ := f.NewStyle(&excelize.Style{NumFmt: 10})

		f.SetCellStyle(trendSheetName, "E2", fmt.Sprintf("G%d", row-1), numberStyle)
		f.SetCellStyle(trendSheetName, "H2", fmt.Sprintf("H%d", row-1), percentStyle)
	}

	for i := range headers {
		col := string(rune('A' + i))
		f.SetColWidth(trendSheetName, col, col, 15)
	}

	f.SetColWidth(trendSheetName, "D", "D", 20)

	f.SetColWidth(trendSheetName, "A", "A", 30)

	return nil
}

func (g *ExcelGenerator) GeneratePriceReportExcel(ctx context.Context, startDate, endDate time.Time) (string, string, error) {

	products, err := db.GetAllProducts(ctx, g.db)
	if err != nil {
		return "", "", fmt.Errorf("error getting products: %w", err)
	}

	if len(products) == 0 {
		return "", "", fmt.Errorf("no products found in database")
	}

	f := excelize.NewFile()
	defer func() {
		if err := f.Close(); err != nil {
			log.Printf("Error closing Excel file: %v", err)
		}
	}()

	const sheetName = "Price Report"
	f.SetSheetName("Sheet1", sheetName)

	headerStyle, err := f.NewStyle(&excelize.Style{
		Font:      &excelize.Font{Bold: true},
		Fill:      excelize.Fill{Type: "pattern", Color: []string{"#DDEBF7"}, Pattern: 1},
		Alignment: &excelize.Alignment{Horizontal: "center", Vertical: "center"},
		Border: []excelize.Border{
			{Type: "top", Color: "#000000", Style: 1},
			{Type: "left", Color: "#000000", Style: 1},
			{Type: "right", Color: "#000000", Style: 1},
			{Type: "bottom", Color: "#000000", Style: 1},
		},
	})
	if err != nil {
		return "", "", fmt.Errorf("error creating header style: %w", err)
	}

	numberStyle, err := f.NewStyle(&excelize.Style{
		NumFmt: 2,
	})
	if err != nil {
		return "", "", fmt.Errorf("error creating number style: %w", err)
	}

	percentStyle, err := f.NewStyle(&excelize.Style{
		NumFmt: 10,
	})
	if err != nil {
		return "", "", fmt.Errorf("error creating percentage style: %w", err)
	}

	headers := []string{
		"Товар", "Артикул", "Начальная цена (₽)", "Конечная цена (₽)",
		"Изменение (₽)", "Изменение (%)", "Мин. цена (₽)", "Макс. цена (₽)", "Записей",
	}

	for i, header := range headers {
		cell := fmt.Sprintf("%c1", 'A'+i)
		f.SetCellValue(sheetName, cell, header)
	}

	f.SetCellStyle(sheetName, "A1", string(rune('A'+len(headers)-1))+"1", headerStyle)

	type ProductResult struct {
		Product           models.ProductRecord
		FirstPrice        int
		LastPrice         int
		PriceChange       int
		ChangePercent     float64
		MinPrice          int
		MaxPrice          int
		RecordsCount      int
		HasSignificantChg bool
	}

	resultCh := make(chan ProductResult)
	var wg sync.WaitGroup

	sem := make(chan struct{}, g.workerPoolSize)

	for _, product := range products {
		wg.Add(1)
		go func(prod models.ProductRecord) {
			defer wg.Done()

			sem <- struct{}{}
			defer func() { <-sem }()

			prices, err := db.GetPricesForPeriod(ctx, g.db, prod.ID, startDate, endDate)
			if err != nil {
				log.Printf("Error getting prices for product %d: %v", prod.ID, err)
				return
			}

			if len(prices) < 2 {
				return
			}

			firstPrice := prices[0].Price
			lastPrice := prices[len(prices)-1].Price

			minPrice, maxPrice := firstPrice, firstPrice
			for _, price := range prices {
				if price.Price < minPrice {
					minPrice = price.Price
				}
				if price.Price > maxPrice {
					maxPrice = price.Price
				}
			}

			priceChange := lastPrice - firstPrice
			changePercent := 0.0
			if firstPrice > 0 {
				changePercent = float64(priceChange) / float64(firstPrice) * 100
			}

			hasSignificantChange := false
			if math.Abs(changePercent) >= g.reportConfig.MinPriceChangePercent {
				hasSignificantChange = true
			}

			resultCh <- ProductResult{
				Product:           prod,
				FirstPrice:        firstPrice,
				LastPrice:         lastPrice,
				PriceChange:       priceChange,
				ChangePercent:     changePercent,
				MinPrice:          minPrice,
				MaxPrice:          maxPrice,
				RecordsCount:      len(prices),
				HasSignificantChg: hasSignificantChange,
			}
		}(product)
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	var results []ProductResult
	for result := range resultCh {
		results = append(results, result)
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Product.Name < results[j].Product.Name
	})

	row := 2
	for _, result := range results {
		f.SetCellValue(sheetName, fmt.Sprintf("A%d", row), result.Product.Name)
		f.SetCellValue(sheetName, fmt.Sprintf("B%d", row), result.Product.VendorCode)
		f.SetCellValue(sheetName, fmt.Sprintf("C%d", row), result.FirstPrice)
		f.SetCellValue(sheetName, fmt.Sprintf("D%d", row), result.LastPrice)
		f.SetCellValue(sheetName, fmt.Sprintf("E%d", row), result.PriceChange)
		f.SetCellValue(sheetName, fmt.Sprintf("F%d", row), result.ChangePercent/100)
		f.SetCellValue(sheetName, fmt.Sprintf("G%d", row), result.MinPrice)
		f.SetCellValue(sheetName, fmt.Sprintf("H%d", row), result.MaxPrice)
		f.SetCellValue(sheetName, fmt.Sprintf("I%d", row), result.RecordsCount)

		row++
	}

	if row > 2 {
		f.SetCellStyle(sheetName, "C2", fmt.Sprintf("E%d", row-1), numberStyle)
		f.SetCellStyle(sheetName, "G2", fmt.Sprintf("H%d", row-1), numberStyle)
		f.SetCellStyle(sheetName, "F2", fmt.Sprintf("F%d", row-1), percentStyle)
	}

	for i := range headers {
		col := string(rune('A' + i))
		width, _ := f.GetColWidth(sheetName, col)
		if width < 15 {
			f.SetColWidth(sheetName, col, col, 15)
		}
	}

	err = g.addPriceTrendSheet(ctx, f, products, startDate, endDate)
	if err != nil {
		log.Printf("Warning: Could not add price trend sheet: %v", err)
	}

	filename := fmt.Sprintf("price_report_%s_%s.xlsx",
		startDate.Format("02-01-2006"),
		endDate.Format("02-01-2006"))
	filePath := filepath.Join(os.TempDir(), filename)

	if err := f.SaveAs(filePath); err != nil {
		return "", "", fmt.Errorf("error saving Excel file: %w", err)
	}

	return filePath, filename, nil
}

// Исправленная функция addPriceTrendSheet для корректного отображения динамики цен
func (g *ExcelGenerator) addPriceTrendSheet(ctx context.Context, f *excelize.File, products []models.ProductRecord, startDate, endDate time.Time) error {
	trendSheetName := "Динамика цен"
	_, err := f.NewSheet(trendSheetName)
	if err != nil {
		return fmt.Errorf("error creating trend sheet: %w", err)
	}

	headerStyle, err := f.NewStyle(&excelize.Style{
		Font:      &excelize.Font{Bold: true},
		Fill:      excelize.Fill{Type: "pattern", Color: []string{"#DDEBF7"}, Pattern: 1},
		Alignment: &excelize.Alignment{Horizontal: "center"},
		Border: []excelize.Border{
			{Type: "top", Color: "#000000", Style: 1},
			{Type: "left", Color: "#000000", Style: 1},
			{Type: "right", Color: "#000000", Style: 1},
			{Type: "bottom", Color: "#000000", Style: 1},
		},
	})
	if err != nil {
		return fmt.Errorf("error creating header style: %w", err)
	}

	headers := []string{
		"Товар", "Артикул", "Дата/Время", "Предыдущая цена (коп.)",
		"Новая цена (коп.)", "Изменение (коп.)", "Изменение (%)", "Скидка (%)",
	}

	for i, header := range headers {
		cell := fmt.Sprintf("%c1", 'A'+i)
		f.SetCellValue(trendSheetName, cell, header)
	}

	f.SetCellStyle(trendSheetName, "A1", string(rune('A'+len(headers)-1))+"1", headerStyle)

	// Создаем стиль для выделения значительных изменений
	significantChangeStyle, _ := f.NewStyle(&excelize.Style{
		Fill: excelize.Fill{Type: "pattern", Color: []string{"#ffeb9c"}, Pattern: 1},
	})

	row := 2
	productsProcessed := 0
	maxProductsToProcess := 100 // Увеличено с 20

	// Для отладки - сохраняем информацию обо всех обработанных продуктах
	var debugInfo []string

	// Перебираем все продукты
	for _, product := range products {
		if productsProcessed >= maxProductsToProcess {
			break
		}

		// Отладочная информация
		debugInfo = append(debugInfo, fmt.Sprintf("Обработка продукта: %s (id: %d)", product.Name, product.ID))

		// Запрашиваем историю цен для продукта
		prices, err := db.GetPricesForPeriod(ctx, g.db, product.ID, startDate, endDate)
		if err != nil {
			debugInfo = append(debugInfo, fmt.Sprintf("  Ошибка при получении цен: %v", err))
			continue
		}

		if len(prices) < 2 {
			debugInfo = append(debugInfo, fmt.Sprintf("  Найдено только %d записей о ценах (нужно минимум 2)", len(prices)))
			continue
		}

		debugInfo = append(debugInfo, fmt.Sprintf("  Найдено %d записей о ценах", len(prices)))

		// Массив для хранения значительных изменений
		var significantChanges []struct {
			Time          time.Time
			PreviousPrice int
			NewPrice      int
			Change        int
			ChangePercent float64
			Discount      int
		}

		// ВАЖНОЕ ИСПРАВЛЕНИЕ: Проверяем все записи на наличие изменений
		// Используем Price вместо FinalPrice
		for i := 1; i < len(prices); i++ {
			previousPrice := prices[i-1].Price
			newPrice := prices[i].Price

			if previousPrice == newPrice {
				continue
			}

			priceChange := newPrice - previousPrice
			changePercent := 0.0
			if previousPrice > 0 {
				changePercent = float64(priceChange) / float64(previousPrice) * 100
			}

			// Отладка - записываем информацию об изменении
			debugInfo = append(debugInfo, fmt.Sprintf("  Изменение #%d: %d -> %d (%.2f%%)",
				i, previousPrice, newPrice, changePercent))

			// Проверяем, превышает ли изменение порог
			if math.Abs(changePercent) >= g.reportConfig.MinPriceChangePercent {
				debugInfo = append(debugInfo, "    Значительное изменение!")

				// Вычисляем скидку (соотношение FinalPrice к Price)
				discount := 0
				if prices[i].Price > 0 && prices[i].FinalPrice > 0 {
					discount = int((1.0 - float64(prices[i].FinalPrice)/float64(prices[i].Price)) * 100)
				}

				significantChanges = append(significantChanges, struct {
					Time          time.Time
					PreviousPrice int
					NewPrice      int
					Change        int
					ChangePercent float64
					Discount      int
				}{
					Time:          prices[i].RecordedAt,
					PreviousPrice: previousPrice,
					NewPrice:      newPrice,
					Change:        priceChange,
					ChangePercent: changePercent,
					Discount:      discount,
				})
			}
		}

		if len(significantChanges) == 0 {
			debugInfo = append(debugInfo, "  Нет значительных изменений цен для этого продукта")
			continue
		}

		productsProcessed++
		debugInfo = append(debugInfo, fmt.Sprintf("  Добавляется %d значительных изменений в отчет", len(significantChanges)))

		// Добавляем каждое значительное изменение в таблицу
		for _, change := range significantChanges {
			// Имя товара и артикул заполняются для каждой строки
			f.SetCellValue(trendSheetName, fmt.Sprintf("A%d", row), product.Name)
			f.SetCellValue(trendSheetName, fmt.Sprintf("B%d", row), product.VendorCode)

			f.SetCellValue(trendSheetName, fmt.Sprintf("C%d", row), change.Time.Format("02.01.2006 15:04:05"))
			f.SetCellValue(trendSheetName, fmt.Sprintf("D%d", row), change.PreviousPrice)
			f.SetCellValue(trendSheetName, fmt.Sprintf("E%d", row), change.NewPrice)
			f.SetCellValue(trendSheetName, fmt.Sprintf("F%d", row), change.Change)
			f.SetCellValue(trendSheetName, fmt.Sprintf("G%d", row), change.ChangePercent/100)
			f.SetCellValue(trendSheetName, fmt.Sprintf("H%d", row), float64(change.Discount)/100)

			// Применяем выделение цветом для строк с значительными изменениями
			if math.Abs(change.ChangePercent) > 10.0 {
				for col := 'A'; col <= 'H'; col++ {
					cellRef := fmt.Sprintf("%c%d", col, row)
					f.SetCellStyle(trendSheetName, cellRef, cellRef, significantChangeStyle)
				}
			}

			row++
		}

		row++ // Пустая строка между товарами
	}

	// Если нет данных о значительных изменениях, добавляем сообщение и отладочную информацию
	if row == 2 {
		f.SetCellValue(trendSheetName, "A2", "Не найдено значительных изменений цен за выбранный период")

		// Добавляем отладочную информацию на отдельный лист
		debugSheetName := "Debug Info"
		f.NewSheet(debugSheetName)
		for i, line := range debugInfo {
			f.SetCellValue(debugSheetName, fmt.Sprintf("A%d", i+1), line)
		}

		row++
	}

	// Применяем стили для чисел и процентов
	if row > 2 {
		numberStyle, _ := f.NewStyle(&excelize.Style{NumFmt: 0}) // целые числа для копеек
		percentStyle, _ := f.NewStyle(&excelize.Style{NumFmt: 10})

		f.SetCellStyle(trendSheetName, "D2", fmt.Sprintf("F%d", row-1), numberStyle)
		f.SetCellStyle(trendSheetName, "G2", fmt.Sprintf("H%d", row-1), percentStyle)
	}

	// Настраиваем ширину столбцов
	for i := range headers {
		col := string(rune('A' + i))
		f.SetColWidth(trendSheetName, col, col, 15)
	}

	f.SetColWidth(trendSheetName, "C", "C", 20) // Для даты/времени
	f.SetColWidth(trendSheetName, "A", "A", 30) // Для названия товара

	return nil
}
