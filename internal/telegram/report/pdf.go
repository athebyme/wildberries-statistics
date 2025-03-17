package report

import (
	"context"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"time"
	"wbmonitoring/monitoring/internal/db"
	"wbmonitoring/monitoring/internal/models"

	"github.com/jmoiron/sqlx"
	"github.com/signintech/gopdf"
)

const (
	// Fonts directory relative to application root
	fontsDir = "fonts"

	// Font file names - make sure these files exist in the fonts directory
	arialRegularFile = "arial.ttf"
	arialBoldFile    = "arialbd.ttf"

	// Default font names
	arialRegular = "arial"
	arialBold    = "arial-bold"

	// Fallback font name if fonts can't be loaded
	fallbackFont = ""
)

// PDFGenerator handles PDF report generation
type PDFGenerator struct {
	db          *sqlx.DB
	fontsLoaded bool
	fontDirPath string
	defaultFont string
	defaultBold string
}

// NewPDFGenerator creates a new PDF report generator
func NewPDFGenerator(db *sqlx.DB) *PDFGenerator {
	// Find application's root directory for fonts
	execPath, err := os.Executable()
	if err != nil {
		log.Printf("Warning: Could not determine executable path: %v", err)
		execPath = "."
	}

	appDir := filepath.Dir(execPath)
	fontPath := filepath.Join(appDir, fontsDir)

	// Check if fonts directory exists
	if _, err := os.Stat(fontPath); os.IsNotExist(err) {
		log.Printf("Warning: Fonts directory not found at %s", fontPath)

		// Try current directory
		cwd, err := os.Getwd()
		if err == nil {
			fontPath = filepath.Join(cwd, fontsDir)
			if _, err := os.Stat(fontPath); os.IsNotExist(err) {
				log.Printf("Warning: Fonts directory not found at %s either", fontPath)
				fontPath = ""
			}
		}
	}

	return &PDFGenerator{
		db:          db,
		fontsLoaded: false,
		fontDirPath: fontPath,
		defaultFont: fallbackFont,
		defaultBold: fallbackFont,
	}
}

// loadFonts attempts to load fonts and sets the fontsLoaded flag
func (g *PDFGenerator) loadFonts(pdf *gopdf.GoPdf) error {
	if g.fontsLoaded {
		return nil // Fonts already loaded
	}

	if g.fontDirPath == "" {
		return fmt.Errorf("fonts directory not found")
	}

	// Try to load regular Arial font
	arialPath := filepath.Join(g.fontDirPath, arialRegularFile)
	if _, err := os.Stat(arialPath); err == nil {
		err := pdf.AddTTFFont(arialRegular, arialPath)
		if err != nil {
			log.Printf("Warning: Could not load Arial font: %v", err)
		} else {
			g.defaultFont = arialRegular
		}
	} else {
		log.Printf("Warning: Arial font file not found at %s", arialPath)
	}

	// Try to load bold Arial font
	arialBoldPath := filepath.Join(g.fontDirPath, arialBoldFile)
	if _, err := os.Stat(arialBoldPath); err == nil {
		err := pdf.AddTTFFont(arialBold, arialBoldPath)
		if err != nil {
			log.Printf("Warning: Could not load Arial Bold font: %v", err)
		} else {
			g.defaultBold = arialBold
		}
	} else {
		log.Printf("Warning: Arial Bold font file not found at %s", arialBoldPath)
	}

	// Only mark fonts as loaded if at least one font was loaded
	g.fontsLoaded = (g.defaultFont != fallbackFont) || (g.defaultBold != fallbackFont)

	if !g.fontsLoaded {
		return fmt.Errorf("no fonts could be loaded")
	}

	return nil
}

// createBasicPDF creates a new PDF document and loads fonts
func (g *PDFGenerator) createBasicPDF(orientation string) (*gopdf.GoPdf, error) {
	var pdf *gopdf.GoPdf = new(gopdf.GoPdf)

	// Set page size based on orientation
	switch orientation {
	case "landscape":
		pdf.Start(gopdf.Config{PageSize: *gopdf.PageSizeA4Landscape})
	case "portrait":
		pdf.Start(gopdf.Config{PageSize: *gopdf.PageSizeA4})
	default:
		pdf.Start(gopdf.Config{PageSize: *gopdf.PageSizeA4})
	}

	pdf.AddPage()

	// Attempt to load fonts
	err := g.loadFonts(pdf)
	if err != nil {
		log.Printf("Warning: Could not load fonts: %v - PDF will use default font", err)
	}

	return pdf, nil
}

// GeneratePriceReportPDF generates a price report in PDF format
func (g *PDFGenerator) GeneratePriceReportPDF(ctx context.Context, startDate, endDate time.Time, minChangePercent float64) (string, string, error) {
	// Get all products
	products, err := db.GetAllProducts(ctx, g.db)
	if err != nil {
		return "", "", fmt.Errorf("error getting products: %w", err)
	}

	if len(products) == 0 {
		return "", "", fmt.Errorf("no products found")
	}

	// Create new PDF document (landscape format for wider tables)
	pdf, err := g.createBasicPDF("landscape")
	if err != nil {
		return "", "", fmt.Errorf("error creating PDF: %w", err)
	}

	// Add report title
	if g.defaultBold != fallbackFont {
		pdf.SetFont(g.defaultBold, "", 16)
	}
	title := fmt.Sprintf("Price Report for Period %s - %s",
		startDate.Format("02.01.2006"),
		endDate.Format("02.01.2006"))
	pdf.SetX(30)
	pdf.SetY(20)
	pdf.Cell(nil, title)
	pdf.Br(20)

	// Define table headers
	headers := []string{
		"Product", "Vendor Code", "Initial Price", "Final Price",
		"Change (₽)", "Change (%)", "Min Price", "Max Price", "Records",
	}

	// Configure table layout
	colWidths := []float64{120, 60, 50, 50, 50, 50, 50, 50, 50}
	headerHeight := 30.0
	rowHeight := 25.0

	// Draw table header
	if g.defaultBold != fallbackFont {
		pdf.SetFont(g.defaultBold, "", 10)
	}
	pdf.SetFillColor(221, 235, 247) // Light blue background
	x := 30.0
	y := pdf.GetY()

	for i, header := range headers {
		pdf.Rectangle(x, y, x+colWidths[i], y+headerHeight, "FD", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 10)
		pdf.Cell(nil, header)
		x += colWidths[i]
	}

	// Prepare for table data
	y += headerHeight
	if g.defaultFont != fallbackFont {
		pdf.SetFont(g.defaultFont, "", 9)
	}

	// Process each product
	productsWithSignificantChanges := 0

	for _, product := range products {
		// Get price history for this product
		prices, err := db.GetPricesForPeriod(ctx, g.db, product.ID, startDate, endDate)
		if err != nil {
			log.Printf("Error getting prices for product %d: %v", product.ID, err)
			continue
		}

		if len(prices) < 2 {
			continue // Need at least two price points to show changes
		}

		// Calculate price metrics
		firstPrice := prices[0].FinalPrice
		lastPrice := prices[len(prices)-1].FinalPrice
		priceChange := lastPrice - firstPrice

		// Skip products with minimal price changes if threshold specified
		priceChangePercent := 0.0
		if firstPrice > 0 {
			priceChangePercent = float64(priceChange) / float64(firstPrice) * 100
		}

		if math.Abs(priceChangePercent) < minChangePercent {
			continue
		}

		productsWithSignificantChanges++

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

		// Check if we need a new page
		if y > 500 { // Near the bottom of the page
			pdf.AddPage()
			y = 30

			// Redraw column headers on new page
			if g.defaultBold != fallbackFont {
				pdf.SetFont(g.defaultBold, "", 10)
			}
			pdf.SetFillColor(221, 235, 247)
			x = 30.0

			for i, header := range headers {
				pdf.Rectangle(x, y, x+colWidths[i], y+headerHeight, "FD", 0, 0)
				pdf.SetX(x + 2)
				pdf.SetY(y + 10)
				pdf.Cell(nil, header)
				x += colWidths[i]
			}

			y += headerHeight
			if g.defaultFont != fallbackFont {
				pdf.SetFont(g.defaultFont, "", 9)
			}
		}

		// Draw row for this product
		x = 30.0

		// Product name (truncate if too long)
		pdf.Rectangle(x, y, x+colWidths[0], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		name := product.Name
		if len(name) > 25 {
			name = name[:22] + "..."
		}
		pdf.Cell(nil, name)
		x += colWidths[0]

		// Vendor code
		pdf.Rectangle(x, y, x+colWidths[1], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		pdf.Cell(nil, product.VendorCode)
		x += colWidths[1]

		// Initial price
		pdf.Rectangle(x, y, x+colWidths[2], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		pdf.Cell(nil, fmt.Sprintf("%.2f", float64(firstPrice)/100))
		x += colWidths[2]

		// Final price
		pdf.Rectangle(x, y, x+colWidths[3], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		pdf.Cell(nil, fmt.Sprintf("%.2f", float64(lastPrice)/100))
		x += colWidths[3]

		// Price change
		pdf.Rectangle(x, y, x+colWidths[4], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		pdf.Cell(nil, fmt.Sprintf("%.2f", float64(priceChange)/100))
		x += colWidths[4]

		// Percentage change
		pdf.Rectangle(x, y, x+colWidths[5], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		pdf.Cell(nil, fmt.Sprintf("%.2f%%", priceChangePercent))
		x += colWidths[5]

		// Min price
		pdf.Rectangle(x, y, x+colWidths[6], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		pdf.Cell(nil, fmt.Sprintf("%.2f", float64(minPrice)/100))
		x += colWidths[6]

		// Max price
		pdf.Rectangle(x, y, x+colWidths[7], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		pdf.Cell(nil, fmt.Sprintf("%.2f", float64(maxPrice)/100))
		x += colWidths[7]

		// Record count
		pdf.Rectangle(x, y, x+colWidths[8], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		pdf.Cell(nil, fmt.Sprintf("%d", len(prices)))

		y += rowHeight
	}

	// If no products had significant changes, add a message
	if productsWithSignificantChanges == 0 {
		if g.defaultFont != fallbackFont {
			pdf.SetFont(g.defaultFont, "", 12)
		}
		pdf.SetX(30)
		pdf.SetY(y + 20)
		pdf.Cell(nil, fmt.Sprintf("No products found with price changes greater than %.1f%%", minChangePercent))
	}

	// Save the PDF to a temporary file
	filename := fmt.Sprintf("price_report_%s_%s.pdf",
		startDate.Format("02-01-2006"),
		endDate.Format("02-01-2006"))
	filePath := filepath.Join(os.TempDir(), filename)

	if err := pdf.WritePdf(filePath); err != nil {
		return "", "", fmt.Errorf("error saving PDF: %w", err)
	}

	return filePath, filename, nil
}

// GenerateStockReportPDF generates a stock report in PDF format
func (g *PDFGenerator) GenerateStockReportPDF(ctx context.Context, startDate, endDate time.Time, minChangePercent float64) (string, string, error) {
	// Get all products
	products, err := db.GetAllProducts(ctx, g.db)
	if err != nil {
		return "", "", fmt.Errorf("error getting products: %w", err)
	}

	// Get all warehouses
	warehouses, err := db.GetAllWarehouses(ctx, g.db)
	if err != nil {
		return "", "", fmt.Errorf("error getting warehouses: %w", err)
	}

	if len(products) == 0 {
		return "", "", fmt.Errorf("no products found")
	}

	// Create new PDF document (landscape format for wider tables)
	pdf, err := g.createBasicPDF("landscape")
	if err != nil {
		return "", "", fmt.Errorf("error creating PDF: %w", err)
	}

	// Add report title
	if g.defaultBold != fallbackFont {
		pdf.SetFont(g.defaultBold, "", 16)
	}
	title := fmt.Sprintf("Stock Report for Period %s - %s",
		startDate.Format("02.01.2006"),
		endDate.Format("02.01.2006"))
	pdf.SetX(30)
	pdf.SetY(20)
	pdf.Cell(nil, title)
	pdf.Br(20)

	// Define table headers
	headers := []string{"Product", "Vendor Code"}

	// Add warehouse names as headers
	// Limit number of warehouses to display to prevent too wide table
	maxWarehousesToDisplay := 5
	displayedWarehouses := warehouses
	if len(warehouses) > maxWarehousesToDisplay {
		displayedWarehouses = warehouses[:maxWarehousesToDisplay]
		headers = append(headers, "W1", "W2", "W3", "W4", "W5")
	} else {
		for _, wh := range warehouses {
			shortName := wh.Name
			if len(shortName) > 10 {
				shortName = shortName[:10] + "..."
			}
			headers = append(headers, shortName)
		}
	}

	headers = append(headers, "Total", "Change")

	// Configure table layout
	colWidths := []float64{120, 60}
	for range displayedWarehouses {
		colWidths = append(colWidths, 40)
	}
	colWidths = append(colWidths, 50, 50) // Total and Change columns

	headerHeight := 30.0
	rowHeight := 25.0

	// Draw table header
	if g.defaultBold != fallbackFont {
		pdf.SetFont(g.defaultBold, "", 10)
	}
	pdf.SetFillColor(221, 235, 247) // Light blue background
	x := 30.0
	y := pdf.GetY()

	for i, header := range headers {
		pdf.Rectangle(x, y, x+colWidths[i], y+headerHeight, "FD", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 10)
		pdf.Cell(nil, header)
		x += colWidths[i]
	}

	// Prepare for table data
	y += headerHeight
	if g.defaultFont != fallbackFont {
		pdf.SetFont(g.defaultFont, "", 9)
	}

	// Process each product
	productsWithSignificantChanges := 0

	for _, product := range products {
		// Collect stock data for all warehouses
		warehouseStocks := make(map[int64][]models.StockRecord)
		totalInitialStock := 0
		totalFinalStock := 0
		hasStockData := false

		for _, warehouse := range warehouses {
			stocks, err := db.GetStocksForPeriod(ctx, g.db, product.ID, warehouse.ID, startDate, endDate)
			if err != nil {
				log.Printf("Error getting stocks for product %d, warehouse %d: %v",
					product.ID, warehouse.ID, err)
				continue
			}

			if len(stocks) > 0 {
				warehouseStocks[warehouse.ID] = stocks
				totalInitialStock += stocks[0].Amount
				totalFinalStock += stocks[len(stocks)-1].Amount
				hasStockData = true
			}
		}

		if !hasStockData {
			continue // Skip if no stock data for any warehouse
		}

		// Calculate total change percentage
		totalChangePercent := 0.0
		if totalInitialStock > 0 {
			totalChangePercent = float64(totalFinalStock-totalInitialStock) / float64(totalInitialStock) * 100
		} else if totalFinalStock > 0 {
			totalChangePercent = 100.0 // If starting from zero, that's a 100% increase
		}

		// Skip products with minimal stock changes if threshold specified
		if math.Abs(totalChangePercent) < minChangePercent {
			continue
		}

		productsWithSignificantChanges++

		// Check if we need a new page
		if y > 500 { // Near the bottom of the page
			pdf.AddPage()
			y = 30

			// Redraw column headers on new page
			if g.defaultBold != fallbackFont {
				pdf.SetFont(g.defaultBold, "", 10)
			}
			pdf.SetFillColor(221, 235, 247)
			x = 30.0

			for i, header := range headers {
				pdf.Rectangle(x, y, x+colWidths[i], y+headerHeight, "FD", 0, 0)
				pdf.SetX(x + 2)
				pdf.SetY(y + 10)
				pdf.Cell(nil, header)
				x += colWidths[i]
			}

			y += headerHeight
			if g.defaultFont != fallbackFont {
				pdf.SetFont(g.defaultFont, "", 9)
			}
		}

		// Draw row for this product
		x = 30.0

		// Product name (truncate if too long)
		pdf.Rectangle(x, y, x+colWidths[0], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		name := product.Name
		if len(name) > 25 {
			name = name[:22] + "..."
		}
		pdf.Cell(nil, name)
		x += colWidths[0]

		// Vendor code
		pdf.Rectangle(x, y, x+colWidths[1], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		pdf.Cell(nil, product.VendorCode)
		x += colWidths[1]

		// Stock for each warehouse
		for i, warehouse := range displayedWarehouses {
			pdf.Rectangle(x, y, x+colWidths[2+i], y+rowHeight, "D", 0, 0)
			pdf.SetX(x + 2)
			pdf.SetY(y + 8)

			if stocks, ok := warehouseStocks[warehouse.ID]; ok && len(stocks) > 0 {
				finalStock := stocks[len(stocks)-1].Amount
				pdf.Cell(nil, fmt.Sprintf("%d", finalStock))
			} else {
				pdf.Cell(nil, "0")
			}

			x += colWidths[2+i]
		}

		// Total stock
		pdf.Rectangle(x, y, x+colWidths[len(colWidths)-2], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		pdf.Cell(nil, fmt.Sprintf("%d", totalFinalStock))
		x += colWidths[len(colWidths)-2]

		// Total change
		pdf.Rectangle(x, y, x+colWidths[len(colWidths)-1], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		pdf.Cell(nil, fmt.Sprintf("%+d (%.1f%%)", totalFinalStock-totalInitialStock, totalChangePercent))

		y += rowHeight
	}

	// If no products had significant changes, add a message
	if productsWithSignificantChanges == 0 {
		if g.defaultFont != fallbackFont {
			pdf.SetFont(g.defaultFont, "", 12)
		}
		pdf.SetX(30)
		pdf.SetY(y + 20)
		pdf.Cell(nil, fmt.Sprintf("No products found with stock changes greater than %.1f%%", minChangePercent))
	}

	// Add stock trend charts for products with significant changes
	if productsWithSignificantChanges > 0 {
		// Create a second page for trends
		pdf.AddPage()
		if g.defaultBold != fallbackFont {
			pdf.SetFont(g.defaultBold, "", 14)
		}
		pdf.SetX(30)
		pdf.SetY(20)
		pdf.Cell(nil, "Stock Trend Analysis")
		pdf.Br(20)

		// Add trend charts for top products (most significant changes)
		type ProductWithChange struct {
			Product       models.ProductRecord
			InitialStock  int
			FinalStock    int
			AbsChangePerc float64
			WarehouseData map[int64][]models.StockRecord
		}

		var productsToChart []ProductWithChange

		// Collect data for products with significant changes
		for _, product := range products {
			warehouseStocks := make(map[int64][]models.StockRecord)
			totalInitialStock := 0
			totalFinalStock := 0
			hasStockData := false

			for _, warehouse := range warehouses {
				stocks, err := db.GetStocksForPeriod(ctx, g.db, product.ID, warehouse.ID, startDate, endDate)
				if err != nil {
					continue
				}

				if len(stocks) > 0 {
					warehouseStocks[warehouse.ID] = stocks
					totalInitialStock += stocks[0].Amount
					totalFinalStock += stocks[len(stocks)-1].Amount
					hasStockData = true
				}
			}

			if !hasStockData || totalInitialStock == 0 && totalFinalStock == 0 {
				continue
			}

			changePerc := 0.0
			if totalInitialStock > 0 {
				changePerc = float64(totalFinalStock-totalInitialStock) / float64(totalInitialStock) * 100
			} else if totalFinalStock > 0 {
				changePerc = 100.0
			}

			if math.Abs(changePerc) < minChangePercent {
				continue
			}

			productsToChart = append(productsToChart, ProductWithChange{
				Product:       product,
				InitialStock:  totalInitialStock,
				FinalStock:    totalFinalStock,
				AbsChangePerc: math.Abs(changePerc),
				WarehouseData: warehouseStocks,
			})
		}

		// Sort products by absolute change percentage (descending)
		sort.Slice(productsToChart, func(i, j int) bool {
			return productsToChart[i].AbsChangePerc > productsToChart[j].AbsChangePerc
		})

		// Add trend charts for top products (limit to 5)
		maxChartsToShow := 5
		if len(productsToChart) > maxChartsToShow {
			productsToChart = productsToChart[:maxChartsToShow]
		}

		chartY := pdf.GetY()
		chartHeight := 120.0
		chartGap := 20.0

		for _, productData := range productsToChart {
			// Check if we need a new page
			if chartY > 650 {
				pdf.AddPage()
				if g.defaultBold != fallbackFont {
					pdf.SetFont(g.defaultBold, "", 14)
				}
				pdf.SetX(30)
				pdf.SetY(20)
				pdf.Cell(nil, "Stock Trend Analysis (continued)")
				chartY = 50
			}

			// Add product name and basic info
			if g.defaultBold != fallbackFont {
				pdf.SetFont(g.defaultBold, "", 12)
			}
			pdf.SetX(30)
			pdf.SetY(chartY)

			// Calculate overall change
			changePerc := 0.0
			if productData.InitialStock > 0 {
				changePerc = float64(productData.FinalStock-productData.InitialStock) / float64(productData.InitialStock) * 100
			} else if productData.FinalStock > 0 {
				changePerc = 100.0
			}

			pdf.Cell(nil, fmt.Sprintf("%s (%s) - Change: %+d (%.1f%%)",
				productData.Product.Name,
				productData.Product.VendorCode,
				productData.FinalStock-productData.InitialStock,
				changePerc))

			chartY += 20

			// Prepare data for chart
			type DayData struct {
				Date  time.Time
				Stock int
			}

			var dailyTotals []DayData

			// Aggregate data by day
			dailyMap := make(map[string]int)
			dateFormat := "2006-01-02"

			for _, stocks := range productData.WarehouseData {
				for _, stock := range stocks {
					dateKey := stock.RecordedAt.Format(dateFormat)
					dailyMap[dateKey] += stock.Amount
				}
			}

			// Convert to sorted daily data
			for dateStr, total := range dailyMap {
				date, _ := time.Parse(dateFormat, dateStr)
				dailyTotals = append(dailyTotals, DayData{Date: date, Stock: total})
			}

			// Sort by date
			sort.Slice(dailyTotals, func(i, j int) bool {
				return dailyTotals[i].Date.Before(dailyTotals[j].Date)
			})

			// Draw a simplified time series chart
			if len(dailyTotals) > 1 {
				// Chart dimensions
				chartWidth := 700.0
				marginLeft := 50.0
				marginBottom := 30.0
				chartY += 10 // Space before chart

				// Calculate min and max values for Y-axis
				minStock, maxStock := dailyTotals[0].Stock, dailyTotals[0].Stock
				for _, data := range dailyTotals {
					if data.Stock < minStock {
						minStock = data.Stock
					}
					if data.Stock > maxStock {
						maxStock = data.Stock
					}
				}

				// Add padding to min/max for better visualization
				yPadding := int(float64(maxStock-minStock) * 0.1)
				if yPadding < 1 {
					yPadding = 1
				}
				minStock = max(0, minStock-yPadding)
				maxStock += yPadding

				// Draw axes
				pdf.SetStrokeColor(0, 0, 0)

				// X-axis
				pdf.Line(30+marginLeft, chartY+chartHeight-marginBottom,
					30+marginLeft+chartWidth-marginLeft, chartY+chartHeight-marginBottom)

				// Y-axis
				pdf.Line(30+marginLeft, chartY, 30+marginLeft, chartY+chartHeight-marginBottom)

				// Add Y-axis labels
				if g.defaultFont != fallbackFont {
					pdf.SetFont(g.defaultFont, "", 8)
				}

				numYTicks := 5
				for i := 0; i <= numYTicks; i++ {
					yPos := chartY + chartHeight - marginBottom -
						((float64(i) / float64(numYTicks)) * (chartHeight - marginBottom))
					yValue := minStock + ((maxStock - minStock) * i / numYTicks)

					pdf.SetX(15)
					pdf.SetY(yPos - 3)
					pdf.Cell(nil, fmt.Sprintf("%d", yValue))

					// Optional: Add horizontal grid lines
					pdf.SetStrokeColor(200, 200, 200) // Light gray
					pdf.Line(30+marginLeft, yPos, 30+marginLeft+chartWidth-marginLeft, yPos)
					pdf.SetStrokeColor(0, 0, 0) // Reset to black
				}

				// Add X-axis date labels
				numXTicks := min(len(dailyTotals), 6)
				for i := 0; i < numXTicks; i++ {
					idx := i
					if numXTicks > 1 {
						idx = i * (len(dailyTotals) - 1) / (numXTicks - 1)
					}

					xPos := 30 + marginLeft + (float64(idx) * (chartWidth - marginLeft) / float64(len(dailyTotals)-1))
					pdf.SetX(xPos - 10)
					pdf.SetY(chartY + chartHeight - marginBottom + 5)
					pdf.Cell(nil, dailyTotals[idx].Date.Format("02.01"))
				}

				// Draw the line chart
				pdf.SetStrokeColor(0, 0, 255) // Blue for the line
				pdf.SetLineWidth(2)

				for i := 0; i < len(dailyTotals)-1; i++ {
					x1 := 30 + marginLeft + (float64(i) * (chartWidth - marginLeft) / float64(len(dailyTotals)-1))
					y1 := chartY + chartHeight - marginBottom -
						((float64(dailyTotals[i].Stock-minStock) / float64(maxStock-minStock)) *
							(chartHeight - marginBottom))

					x2 := 30 + marginLeft + (float64(i+1) * (chartWidth - marginLeft) / float64(len(dailyTotals)-1))
					y2 := chartY + chartHeight - marginBottom -
						((float64(dailyTotals[i+1].Stock-minStock) / float64(maxStock-minStock)) *
							(chartHeight - marginBottom))

					pdf.Line(x1, y1, x2, y2)
				}

				pdf.SetLineWidth(1) // Reset line width
			} else {
				// Not enough data points for a chart
				if g.defaultFont != fallbackFont {
					pdf.SetFont(g.defaultFont, "", 10)
				}
				pdf.SetX(50)
				pdf.SetY(chartY + 20)
				pdf.Cell(nil, "Insufficient data for trend chart")
				chartY += 20
			}

			chartY += chartHeight + chartGap
		}
	}

	// Save the PDF to a temporary file
	filename := fmt.Sprintf("stock_report_%s_%s.pdf",
		startDate.Format("02-01-2006"),
		endDate.Format("02-01-2006"))
	filePath := filepath.Join(os.TempDir(), filename)

	if err := pdf.WritePdf(filePath); err != nil {
		return "", "", fmt.Errorf("error saving PDF: %w", err)
	}

	return filePath, filename, nil
}

// Helper function for determining min value
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Helper function for determining max value
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
