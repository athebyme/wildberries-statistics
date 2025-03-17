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

// ExcelGenerator handles Excel report generation
type ExcelGenerator struct {
	db             *sqlx.DB
	workerPoolSize int
	reportConfig   Config
}

// NewExcelGenerator creates a new Excel report generator
func NewExcelGenerator(db *sqlx.DB, reportConfig Config, workerPoolSize int) *ExcelGenerator {
	if workerPoolSize <= 0 {
		workerPoolSize = 5 // Default worker pool size
	}

	return &ExcelGenerator{
		db:             db,
		workerPoolSize: workerPoolSize,
		reportConfig:   reportConfig,
	}
}

// GenerateStockReportExcel generates a stock report in Excel format
func (g *ExcelGenerator) GenerateStockReportExcel(ctx context.Context, startDate, endDate time.Time) (string, string, error) {
	// Create channels for concurrent data fetching
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

	// Fetch products and warehouses concurrently
	go func() {
		products, err := db.GetAllProducts(ctx, g.db)
		productsCh <- productsResult{Products: products, Error: err}
	}()

	go func() {
		warehouses, err := db.GetAllWarehouses(ctx, g.db)
		warehousesCh <- warehousesResult{Warehouses: warehouses, Error: err}
	}()

	// Get results
	productsRes := <-productsCh
	warehousesRes := <-warehousesCh

	// Check for errors
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

	// Create a new Excel file
	f := excelize.NewFile()
	defer func() {
		if err := f.Close(); err != nil {
			log.Printf("Error closing Excel file: %v", err)
		}
	}()

	// Rename the default sheet
	const summarySheetName = "Stock Summary"
	f.SetSheetName("Sheet1", summarySheetName)

	// Create a detail sheet
	const detailSheetName = "Stock by Warehouse"
	_, err := f.NewSheet(detailSheetName)
	if err != nil {
		return "", "", fmt.Errorf("error creating detail sheet: %w", err)
	}

	// Create styles
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
		NumFmt: 1, // Integer format
	})
	if err != nil {
		return "", "", fmt.Errorf("error creating number style: %w", err)
	}

	percentStyle, err := f.NewStyle(&excelize.Style{
		NumFmt: 10, // Percentage format
	})
	if err != nil {
		return "", "", fmt.Errorf("error creating percentage style: %w", err)
	}

	// Set up headers for summary sheet
	summaryHeaders := []string{
		"Product", "Vendor Code", "Initial Stock", "Final Stock",
		"Change", "Change (%)", "Records",
	}

	for i, header := range summaryHeaders {
		cell := fmt.Sprintf("%c1", 'A'+i)
		f.SetCellValue(summarySheetName, cell, header)
	}

	// Apply header style to summary sheet
	f.SetCellStyle(summarySheetName, "A1", string(rune('A'+len(summaryHeaders)-1))+"1", headerStyle)

	// Set up headers for detail sheet
	detailHeaders := []string{
		"Product", "Vendor Code", "Warehouse", "Initial Stock", "Final Stock",
		"Change", "Change (%)", "Min Stock", "Max Stock", "Records",
	}

	for i, header := range detailHeaders {
		cell := fmt.Sprintf("%c1", 'A'+i)
		f.SetCellValue(detailSheetName, cell, header)
	}

	// Apply header style to detail sheet
	f.SetCellStyle(detailSheetName, "A1", string(rune('A'+len(detailHeaders)-1))+"1", headerStyle)

	// Process products in parallel
	type ProductSummary struct {
		Product           models.ProductRecord
		TotalInitialStock int
		TotalFinalStock   int
		TotalChange       int
		ChangePercent     float64
		RecordsCount      int
		HasSignificantChg bool
	}

	type WarehouseDetail struct {
		Product           models.ProductRecord
		Warehouse         models.Warehouse
		InitialStock      int
		FinalStock        int
		Change            int
		ChangePercent     float64
		MinStock          int
		MaxStock          int
		RecordsCount      int
		HasSignificantChg bool
	}

	summaryCh := make(chan ProductSummary)
	detailCh := make(chan WarehouseDetail)
	var wg sync.WaitGroup

	// Create a semaphore to limit concurrent DB queries
	sem := make(chan struct{}, g.workerPoolSize)

	// Process products
	for _, product := range products {
		wg.Add(1)
		go func(prod models.ProductRecord) {
			defer wg.Done()

			// Acquire semaphore
			sem <- struct{}{}
			defer func() { <-sem }() // Release semaphore

			totalInitialStock := 0
			totalFinalStock := 0
			totalRecords := 0
			var warehouseDetails []WarehouseDetail

			// Process each warehouse for this product
			for _, warehouse := range warehouses {
				// Get stock history for this product-warehouse
				stocks, err := db.GetStocksForPeriod(ctx, g.db, prod.ID, warehouse.ID, startDate, endDate)
				if err != nil {
					log.Printf("Error getting stocks for product %d, warehouse %d: %v",
						prod.ID, warehouse.ID, err)
					continue
				}

				if len(stocks) == 0 {
					continue // No stock data for this warehouse
				}

				// Calculate stock metrics
				initialStock := stocks[0].Amount
				finalStock := stocks[len(stocks)-1].Amount

				// Find min and max stock
				minStock, maxStock := initialStock, initialStock
				for _, stock := range stocks {
					if stock.Amount < minStock {
						minStock = stock.Amount
					}
					if stock.Amount > maxStock {
						maxStock = stock.Amount
					}
				}

				// Calculate change
				stockChange := finalStock - initialStock
				changePercent := 0.0
				if initialStock > 0 {
					changePercent = float64(stockChange) / float64(initialStock) * 100
				} else if finalStock > 0 {
					changePercent = 100.0 // Starting from zero
				}

				// Check if change is significant based on threshold
				hasSignificantChange := false
				if math.Abs(changePercent) >= g.reportConfig.MinStockChangePercent {
					hasSignificantChange = true
				}

				// Update totals
				totalInitialStock += initialStock
				totalFinalStock += finalStock
				totalRecords += len(stocks)

				// Store warehouse detail
				warehouseDetails = append(warehouseDetails, WarehouseDetail{
					Product:           prod,
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

			// Skip if no stock data for any warehouse
			if len(warehouseDetails) == 0 {
				return
			}

			// Calculate overall change percentage
			totalChange := totalFinalStock - totalInitialStock
			totalChangePercent := 0.0
			if totalInitialStock > 0 {
				totalChangePercent = float64(totalChange) / float64(totalInitialStock) * 100
			} else if totalFinalStock > 0 {
				totalChangePercent = 100.0 // Starting from zero
			}

			// Check if overall change is significant
			hasSignificantChange := false
			if math.Abs(totalChangePercent) >= g.reportConfig.MinStockChangePercent {
				hasSignificantChange = true
			}

			// Send summary for the product
			summaryCh <- ProductSummary{
				Product:           prod,
				TotalInitialStock: totalInitialStock,
				TotalFinalStock:   totalFinalStock,
				TotalChange:       totalChange,
				ChangePercent:     totalChangePercent,
				RecordsCount:      totalRecords,
				HasSignificantChg: hasSignificantChange,
			}

			// Send details for each warehouse
			for _, detail := range warehouseDetails {
				detailCh <- detail
			}
		}(product)
	}

	// Close channels when all goroutines are done
	go func() {
		wg.Wait()
		close(summaryCh)
		close(detailCh)
	}()

	// Process summary data
	var summaries []ProductSummary
	for summary := range summaryCh {
		summaries = append(summaries, summary)
	}

	// Sort summaries by product name
	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].Product.Name < summaries[j].Product.Name
	})

	// Process detail data
	var details []WarehouseDetail
	for detail := range detailCh {
		details = append(details, detail)
	}

	// Sort details by product name and warehouse name
	sort.Slice(details, func(i, j int) bool {
		if details[i].Product.Name != details[j].Product.Name {
			return details[i].Product.Name < details[j].Product.Name
		}
		return details[i].Warehouse.Name < details[j].Warehouse.Name
	})

	// Fill in summary sheet
	summaryRow := 2
	for _, summary := range summaries {
		f.SetCellValue(summarySheetName, fmt.Sprintf("A%d", summaryRow), summary.Product.Name)
		f.SetCellValue(summarySheetName, fmt.Sprintf("B%d", summaryRow), summary.Product.VendorCode)
		f.SetCellValue(summarySheetName, fmt.Sprintf("C%d", summaryRow), summary.TotalInitialStock)
		f.SetCellValue(summarySheetName, fmt.Sprintf("D%d", summaryRow), summary.TotalFinalStock)
		f.SetCellValue(summarySheetName, fmt.Sprintf("E%d", summaryRow), summary.TotalChange)
		f.SetCellValue(summarySheetName, fmt.Sprintf("F%d", summaryRow), summary.ChangePercent/100) // Decimal for percentage
		f.SetCellValue(summarySheetName, fmt.Sprintf("G%d", summaryRow), summary.RecordsCount)

		summaryRow++
	}

	// Fill in detail sheet
	detailRow := 2
	for _, detail := range details {
		f.SetCellValue(detailSheetName, fmt.Sprintf("A%d", detailRow), detail.Product.Name)
		f.SetCellValue(detailSheetName, fmt.Sprintf("B%d", detailRow), detail.Product.VendorCode)
		f.SetCellValue(detailSheetName, fmt.Sprintf("C%d", detailRow), detail.Warehouse.Name)
		f.SetCellValue(detailSheetName, fmt.Sprintf("D%d", detailRow), detail.InitialStock)
		f.SetCellValue(detailSheetName, fmt.Sprintf("E%d", detailRow), detail.FinalStock)
		f.SetCellValue(detailSheetName, fmt.Sprintf("F%d", detailRow), detail.Change)
		f.SetCellValue(detailSheetName, fmt.Sprintf("G%d", detailRow), detail.ChangePercent/100) // Decimal for percentage
		f.SetCellValue(detailSheetName, fmt.Sprintf("H%d", detailRow), detail.MinStock)
		f.SetCellValue(detailSheetName, fmt.Sprintf("I%d", detailRow), detail.MaxStock)
		f.SetCellValue(detailSheetName, fmt.Sprintf("J%d", detailRow), detail.RecordsCount)

		detailRow++
	}

	// Apply styles to data
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

	// Add trend analysis sheet
	err = g.addStockTrendSheet(ctx, f, products, warehouses, startDate, endDate)
	if err != nil {
		log.Printf("Warning: Could not add stock trend sheet: %v", err)
	}

	// Auto-adjust column widths in summary sheet
	for i := range summaryHeaders {
		col := string(rune('A' + i))
		width, _ := f.GetColWidth(summarySheetName, col)
		if width < 15 {
			f.SetColWidth(summarySheetName, col, col, 15)
		}
	}

	// Auto-adjust column widths in detail sheet
	for i := range detailHeaders {
		col := string(rune('A' + i))
		width, _ := f.GetColWidth(detailSheetName, col)
		if width < 15 {
			f.SetColWidth(detailSheetName, col, col, 15)
		}
	}

	// Make product name column wider
	f.SetColWidth(summarySheetName, "A", "A", 30)
	f.SetColWidth(detailSheetName, "A", "A", 30)

	// Generate file path
	filename := fmt.Sprintf("stock_report_%s_%s.xlsx",
		startDate.Format("02-01-2006"),
		endDate.Format("02-01-2006"))
	filePath := filepath.Join(os.TempDir(), filename)

	// Save the file
	if err := f.SaveAs(filePath); err != nil {
		return "", "", fmt.Errorf("error saving Excel file: %w", err)
	}

	return filePath, filename, nil
}

// addStockTrendSheet adds a sheet with stock change trends
func (g *ExcelGenerator) addStockTrendSheet(ctx context.Context, f *excelize.File, products []models.ProductRecord, warehouses []models.Warehouse, startDate, endDate time.Time) error {
	trendSheetName := "Stock Trends"
	_, err := f.NewSheet(trendSheetName)
	if err != nil {
		return fmt.Errorf("error creating trend sheet: %w", err)
	}

	// Create header style
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

	// Headers for the trend sheet
	headers := []string{
		"Product", "Vendor Code", "Warehouse", "Date/Time",
		"Previous Stock", "New Stock", "Change", "Change (%)",
	}

	for i, header := range headers {
		cell := fmt.Sprintf("%c1", 'A'+i)
		f.SetCellValue(trendSheetName, cell, header)
	}

	// Apply header style
	f.SetCellStyle(trendSheetName, "A1", string(rune('A'+len(headers)-1))+"1", headerStyle)

	// Create a semaphore to limit concurrent DB queries
	sem := make(chan struct{}, g.workerPoolSize)

	// Process each product with significant stock changes
	row := 2
	productsProcessed := 0
	maxProductsToProcess := 20 // Limit to prevent very large files

	for _, product := range products {
		if productsProcessed >= maxProductsToProcess {
			break
		}

		// Acquire semaphore
		sem <- struct{}{}

		// Define significant changes for this product
		var significantChanges []struct {
			Warehouse     models.Warehouse
			Time          time.Time
			PreviousStock int
			NewStock      int
			Change        int
			ChangePercent float64
		}

		hasSignificantChanges := false

		// Check each warehouse
		for _, warehouse := range warehouses {
			// Get stock history
			stocks, err := db.GetStocksForPeriod(ctx, g.db, product.ID, warehouse.ID, startDate, endDate)
			if err != nil {
				log.Printf("Error getting stocks for product %d, warehouse %d: %v",
					product.ID, warehouse.ID, err)
				continue
			}

			if len(stocks) < 2 {
				continue // Need at least two data points
			}

			// Find stock changes
			for i := 1; i < len(stocks); i++ {
				previousStock := stocks[i-1].Amount
				newStock := stocks[i].Amount

				if previousStock == newStock {
					continue // No change
				}

				stockChange := newStock - previousStock
				changePercent := 0.0
				if previousStock > 0 {
					changePercent = float64(stockChange) / float64(previousStock) * 100
				} else if newStock > 0 {
					changePercent = 100.0 // Starting from zero
				}

				// Only include significant changes
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

		// Release semaphore
		<-sem

		// Skip if no significant changes
		if !hasSignificantChanges {
			continue
		}

		productsProcessed++
		firstEntryForProduct := true

		// Sort changes by time
		sort.Slice(significantChanges, func(i, j int) bool {
			return significantChanges[i].Time.Before(significantChanges[j].Time)
		})

		// Add all significant changes to the sheet
		for _, change := range significantChanges {
			// Product name and vendor code (only for first entry)
			if firstEntryForProduct {
				f.SetCellValue(trendSheetName, fmt.Sprintf("A%d", row), product.Name)
				f.SetCellValue(trendSheetName, fmt.Sprintf("B%d", row), product.VendorCode)
				firstEntryForProduct = false
			} else {
				f.SetCellValue(trendSheetName, fmt.Sprintf("A%d", row), "")
				f.SetCellValue(trendSheetName, fmt.Sprintf("B%d", row), "")
			}

			// Change details
			f.SetCellValue(trendSheetName, fmt.Sprintf("C%d", row), change.Warehouse.Name)
			f.SetCellValue(trendSheetName, fmt.Sprintf("D%d", row), change.Time.Format("02.01.2006 15:04:05"))
			f.SetCellValue(trendSheetName, fmt.Sprintf("E%d", row), change.PreviousStock)
			f.SetCellValue(trendSheetName, fmt.Sprintf("F%d", row), change.NewStock)
			f.SetCellValue(trendSheetName, fmt.Sprintf("G%d", row), change.Change)
			f.SetCellValue(trendSheetName, fmt.Sprintf("H%d", row), change.ChangePercent/100) // For percentage format

			row++
		}

		// Add a blank row between products
		row++
	}

	// Apply styles to numeric columns
	if row > 2 {
		numberStyle, _ := f.NewStyle(&excelize.Style{NumFmt: 1})   // Integer format
		percentStyle, _ := f.NewStyle(&excelize.Style{NumFmt: 10}) // Percentage format

		f.SetCellStyle(trendSheetName, "E2", fmt.Sprintf("G%d", row-1), numberStyle)
		f.SetCellStyle(trendSheetName, "H2", fmt.Sprintf("H%d", row-1), percentStyle)
	}

	// Auto-adjust column widths
	for i := range headers {
		col := string(rune('A' + i))
		f.SetColWidth(trendSheetName, col, col, 15)
	}

	// Make the date column wider
	f.SetColWidth(trendSheetName, "D", "D", 20)

	// Make product name column wider
	f.SetColWidth(trendSheetName, "A", "A", 30)

	return nil
}

// GeneratePriceReportExcel generates a price report in Excel format
func (g *ExcelGenerator) GeneratePriceReportExcel(ctx context.Context, startDate, endDate time.Time) (string, string, error) {
	// Get all products
	products, err := db.GetAllProducts(ctx, g.db)
	if err != nil {
		return "", "", fmt.Errorf("error getting products: %w", err)
	}

	if len(products) == 0 {
		return "", "", fmt.Errorf("no products found in database")
	}

	// Create a new Excel file
	f := excelize.NewFile()
	defer func() {
		if err := f.Close(); err != nil {
			log.Printf("Error closing Excel file: %v", err)
		}
	}()

	// Set up sheet
	const sheetName = "Price Report"
	f.SetSheetName("Sheet1", sheetName)

	// Create styles
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
		NumFmt: 2, // Format with 2 decimal places
	})
	if err != nil {
		return "", "", fmt.Errorf("error creating number style: %w", err)
	}

	percentStyle, err := f.NewStyle(&excelize.Style{
		NumFmt: 10, // Percentage format
	})
	if err != nil {
		return "", "", fmt.Errorf("error creating percentage style: %w", err)
	}

	// Set up headers
	headers := []string{
		"Product", "Vendor Code", "Initial Price (₽)", "Final Price (₽)",
		"Change (₽)", "Change (%)", "Min Price (₽)", "Max Price (₽)", "Records",
	}

	for i, header := range headers {
		cell := fmt.Sprintf("%c1", 'A'+i)
		f.SetCellValue(sheetName, cell, header)
	}

	// Apply header style
	f.SetCellStyle(sheetName, "A1", string(rune('A'+len(headers)-1))+"1", headerStyle)

	// Process products in parallel
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

	// Create a semaphore to limit concurrent DB queries
	sem := make(chan struct{}, g.workerPoolSize)

	// Process products in parallel
	for _, product := range products {
		wg.Add(1)
		go func(prod models.ProductRecord) {
			defer wg.Done()

			// Acquire semaphore
			sem <- struct{}{}
			defer func() { <-sem }() // Release semaphore

			// Get price history for this product
			prices, err := db.GetPricesForPeriod(ctx, g.db, prod.ID, startDate, endDate)
			if err != nil {
				log.Printf("Error getting prices for product %d: %v", prod.ID, err)
				return
			}

			if len(prices) < 2 {
				return // Need at least two price records
			}

			// Calculate price metrics
			firstPrice := prices[0].FinalPrice
			lastPrice := prices[len(prices)-1].FinalPrice

			// Find min and max prices
			minPrice, maxPrice := firstPrice, firstPrice
			for _, price := range prices {
				if price.FinalPrice < minPrice {
					minPrice = price.FinalPrice
				}
				if price.FinalPrice > maxPrice {
					maxPrice = price.FinalPrice
				}
			}

			// Calculate price change
			priceChange := lastPrice - firstPrice
			changePercent := 0.0
			if firstPrice > 0 {
				changePercent = float64(priceChange) / float64(firstPrice) * 100
			}

			// Check if change is significant based on threshold
			hasSignificantChange := false
			if math.Abs(changePercent) >= g.reportConfig.MinPriceChangePercent {
				hasSignificantChange = true
			}

			// Send result
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

	// Close result channel when all goroutines are done
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Collect and sort results
	var results []ProductResult
	for result := range resultCh {
		results = append(results, result)
	}

	// Sort by product name for readability
	sort.Slice(results, func(i, j int) bool {
		return results[i].Product.Name < results[j].Product.Name
	})

	// Fill in data rows
	row := 2
	for _, result := range results {
		// Convert prices from kopecks to rubles for display
		firstPrice := float64(result.FirstPrice) / 100
		lastPrice := float64(result.LastPrice) / 100
		priceChange := float64(result.PriceChange) / 100
		minPrice := float64(result.MinPrice) / 100
		maxPrice := float64(result.MaxPrice) / 100

		f.SetCellValue(sheetName, fmt.Sprintf("A%d", row), result.Product.Name)
		f.SetCellValue(sheetName, fmt.Sprintf("B%d", row), result.Product.VendorCode)
		f.SetCellValue(sheetName, fmt.Sprintf("C%d", row), firstPrice)
		f.SetCellValue(sheetName, fmt.Sprintf("D%d", row), lastPrice)
		f.SetCellValue(sheetName, fmt.Sprintf("E%d", row), priceChange)
		f.SetCellValue(sheetName, fmt.Sprintf("F%d", row), result.ChangePercent/100) // Decimal for percentage
		f.SetCellValue(sheetName, fmt.Sprintf("G%d", row), minPrice)
		f.SetCellValue(sheetName, fmt.Sprintf("H%d", row), maxPrice)
		f.SetCellValue(sheetName, fmt.Sprintf("I%d", row), result.RecordsCount)

		row++
	}

	// Apply styles to data
	if row > 2 {
		f.SetCellStyle(sheetName, "C2", fmt.Sprintf("E%d", row-1), numberStyle)
		f.SetCellStyle(sheetName, "G2", fmt.Sprintf("H%d", row-1), numberStyle)
		f.SetCellStyle(sheetName, "F2", fmt.Sprintf("F%d", row-1), percentStyle)
	}

	// Auto-adjust column widths
	for i := range headers {
		col := string(rune('A' + i))
		width, _ := f.GetColWidth(sheetName, col)
		if width < 15 {
			f.SetColWidth(sheetName, col, col, 15)
		}
	}

	// Add trend analysis sheet
	err = g.addPriceTrendSheet(ctx, f, products, startDate, endDate)
	if err != nil {
		log.Printf("Warning: Could not add price trend sheet: %v", err)
	}

	// Generate file path
	filename := fmt.Sprintf("price_report_%s_%s.xlsx",
		startDate.Format("02-01-2006"),
		endDate.Format("02-01-2006"))
	filePath := filepath.Join(os.TempDir(), filename)

	// Save the file
	if err := f.SaveAs(filePath); err != nil {
		return "", "", fmt.Errorf("error saving Excel file: %w", err)
	}

	return filePath, filename, nil
}

// addPriceTrendSheet adds a sheet with detailed price changes over time
func (g *ExcelGenerator) addPriceTrendSheet(ctx context.Context, f *excelize.File, products []models.ProductRecord, startDate, endDate time.Time) error {
	trendSheetName := "Price Trends"
	_, err := f.NewSheet(trendSheetName)
	if err != nil {
		return fmt.Errorf("error creating trend sheet: %w", err)
	}

	// Create header style
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

	// Headers for the trend sheet
	headers := []string{
		"Product", "Vendor Code", "Date/Time", "Previous Price (₽)",
		"New Price (₽)", "Change (₽)", "Change (%)", "Discount (%)",
	}

	for i, header := range headers {
		cell := fmt.Sprintf("%c1", 'A'+i)
		f.SetCellValue(trendSheetName, cell, header)
	}

	// Apply header style
	f.SetCellStyle(trendSheetName, "A1", string(rune('A'+len(headers)-1))+"1", headerStyle)

	// Process each product to find significant price changes
	row := 2
	productsProcessed := 0
	maxProductsToProcess := 20 // Limit to prevent very large files

	for _, product := range products {
		if productsProcessed >= maxProductsToProcess {
			break
		}

		// Fetch price history
		prices, err := db.GetPricesForPeriod(ctx, g.db, product.ID, startDate, endDate)
		if err != nil {
			log.Printf("Error getting prices for product %d: %v", product.ID, err)
			continue
		}

		if len(prices) < 2 {
			continue // Need at least two data points
		}

		// Find significant changes for this product
		var significantChanges []struct {
			Time          time.Time
			PreviousPrice int
			NewPrice      int
			Change        int
			ChangePercent float64
			Discount      int
		}

		for i := 1; i < len(prices); i++ {
			previousPrice := prices[i-1].FinalPrice
			newPrice := prices[i].FinalPrice

			if previousPrice == newPrice {
				continue // No change
			}

			priceChange := newPrice - previousPrice
			changePercent := 0.0
			if previousPrice > 0 {
				changePercent = float64(priceChange) / float64(previousPrice) * 100
			}

			// Only include significant changes
			if math.Abs(changePercent) >= g.reportConfig.MinPriceChangePercent {
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
					Discount:      prices[i].Discount,
				})
			}
		}

		// Skip if no significant changes
		if len(significantChanges) == 0 {
			continue
		}

		productsProcessed++
		firstEntryForProduct := true

		// Add all significant changes to the sheet
		for _, change := range significantChanges {
			// Convert prices from kopecks to rubles
			prevPrice := float64(change.PreviousPrice) / 100
			newPrice := float64(change.NewPrice) / 100
			priceChange := float64(change.Change) / 100

			// Product name and vendor code (only for first entry)
			if firstEntryForProduct {
				f.SetCellValue(trendSheetName, fmt.Sprintf("A%d", row), product.Name)
				f.SetCellValue(trendSheetName, fmt.Sprintf("B%d", row), product.VendorCode)
				firstEntryForProduct = false
			} else {
				f.SetCellValue(trendSheetName, fmt.Sprintf("A%d", row), "")
				f.SetCellValue(trendSheetName, fmt.Sprintf("B%d", row), "")
			}

			// Change details
			f.SetCellValue(trendSheetName, fmt.Sprintf("C%d", row), change.Time.Format("02.01.2006 15:04:05"))
			f.SetCellValue(trendSheetName, fmt.Sprintf("D%d", row), prevPrice)
			f.SetCellValue(trendSheetName, fmt.Sprintf("E%d", row), newPrice)
			f.SetCellValue(trendSheetName, fmt.Sprintf("F%d", row), priceChange)
			f.SetCellValue(trendSheetName, fmt.Sprintf("G%d", row), change.ChangePercent/100) // For percentage format
			f.SetCellValue(trendSheetName, fmt.Sprintf("H%d", row), float64(change.Discount)/100)

			row++
		}

		// Add a blank row between products
		row++
	}

	// Apply styles to numeric columns
	if row > 2 {
		numberStyle, _ := f.NewStyle(&excelize.Style{NumFmt: 2})   // 2 decimal places
		percentStyle, _ := f.NewStyle(&excelize.Style{NumFmt: 10}) // Percentage format

		f.SetCellStyle(trendSheetName, "D2", fmt.Sprintf("F%d", row-1), numberStyle)
		f.SetCellStyle(trendSheetName, "G2", fmt.Sprintf("H%d", row-1), percentStyle)
	}

	// Auto-adjust column widths
	for i := range headers {
		col := string(rune('A' + i))
		f.SetColWidth(trendSheetName, col, col, 15)
	}

	// Make the date column wider
	f.SetColWidth(trendSheetName, "C", "C", 20)

	// Make product name column wider
	f.SetColWidth(trendSheetName, "A", "A", 30)

	return nil
}
