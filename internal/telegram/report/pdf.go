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
	fontsDir = "fonts"

	arialRegularFile = "arial.ttf"
	arialBoldFile    = "arialbd.ttf"

	arialRegular = "arial"
	arialBold    = "arial-bold"

	fallbackFont = ""
)

type PDFGenerator struct {
	db          *sqlx.DB
	fontsLoaded bool
	fontDirPath string
	defaultFont string
	defaultBold string
}

func NewPDFGenerator(db *sqlx.DB) *PDFGenerator {

	fontPaths := []string{}

	// 1. Try relative to executable
	execPath, err := os.Executable()
	if err == nil {
		appDir := filepath.Dir(execPath)
		fontPaths = append(fontPaths, filepath.Join(appDir, fontsDir))
	}

	// 2. Try current directory
	cwd, err := os.Getwd()
	if err == nil {
		fontPaths = append(fontPaths, filepath.Join(cwd, fontsDir))
	}

	// 3. Try parent directory of executable (for cases where exe is in a subdirectory)
	if err == nil {
		appParentDir := filepath.Dir(filepath.Dir(execPath))
		fontPaths = append(fontPaths, filepath.Join(appParentDir, fontsDir))
	}

	// 4. Try cmd/wbmonitoring/fonts path (based on project structure)
	for _, baseDir := range []string{cwd, filepath.Dir(execPath)} {

		projectRoot := baseDir
		for i := 0; i < 3; i++ {
			fontPaths = append(fontPaths, filepath.Join(projectRoot, "cmd", "wbmonitoring", "fonts"))
			projectRoot = filepath.Dir(projectRoot)
		}
	}

	var validFontPath string
	for _, path := range fontPaths {
		if _, err := os.Stat(path); err == nil {

			arialPath := filepath.Join(path, arialRegularFile)
			if _, err := os.Stat(arialPath); err == nil {
				validFontPath = path
				log.Printf("Found fonts directory at: %s", validFontPath)
				break
			}
		}
	}

	if validFontPath == "" {
		log.Printf("Warning: Could not find fonts directory. Tried paths: %v", fontPaths)
	}

	return &PDFGenerator{
		db:          db,
		fontsLoaded: false,
		fontDirPath: validFontPath,
		defaultFont: fallbackFont,
		defaultBold: fallbackFont,
	}
}

func (g *PDFGenerator) loadFonts(pdf *gopdf.GoPdf) error {
	if g.fontsLoaded {
		return nil
	}

	if g.fontDirPath == "" {
		return fmt.Errorf("fonts directory not found")
	}

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

	g.fontsLoaded = (g.defaultFont != fallbackFont) || (g.defaultBold != fallbackFont)

	if !g.fontsLoaded {
		return fmt.Errorf("no fonts could be loaded")
	}

	return nil
}

func (g *PDFGenerator) createBasicPDF(orientation string) (*gopdf.GoPdf, error) {
	var pdf *gopdf.GoPdf = new(gopdf.GoPdf)

	switch orientation {
	case "landscape":
		pdf.Start(gopdf.Config{PageSize: *gopdf.PageSizeA4Landscape})
	case "portrait":
		pdf.Start(gopdf.Config{PageSize: *gopdf.PageSizeA4})
	default:
		pdf.Start(gopdf.Config{PageSize: *gopdf.PageSizeA4})
	}

	pdf.AddPage()

	err := g.loadFonts(pdf)
	if err != nil {
		log.Printf("Warning: Could not load fonts: %v - PDF will use default font", err)
	}

	return pdf, nil
}

func (g *PDFGenerator) GeneratePriceReportPDF(ctx context.Context, startDate, endDate time.Time, minChangePercent float64) (string, string, error) {
	products, err := db.GetAllProducts(ctx, g.db)
	if err != nil {
		return "", "", fmt.Errorf("error getting products: %w", err)
	}

	if len(products) == 0 {
		return "", "", fmt.Errorf("no products found")
	}

	pdf, err := g.createBasicPDF("landscape")
	if err != nil {
		return "", "", fmt.Errorf("error creating PDF: %w", err)
	}

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

	headers := []string{
		"Product", "Vendor Code", "Initial Price (₽)", "Final Price (₽)",
		"Change (₽)", "Change (%)", "Min Price (₽)", "Max Price (₽)", "Records",
	}

	colWidths := []float64{120, 60, 70, 70, 70, 50, 70, 70, 50}
	headerHeight := 30.0
	rowHeight := 25.0

	if g.defaultBold != fallbackFont {
		pdf.SetFont(g.defaultBold, "", 10)
	}
	pdf.SetFillColor(221, 235, 247)
	x := 30.0
	y := pdf.GetY()

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

	productsWithSignificantChanges := 0

	for _, product := range products {
		// Use price snapshots instead of prices
		prices, err := db.GetPriceSnapshotsForPeriod(ctx, g.db, product.ID, startDate, endDate)
		if err != nil {
			log.Printf("Error getting price snapshots for product %d: %v", product.ID, err)
			continue
		}

		if len(prices) < 2 {
			continue
		}

		firstPrice := prices[0].FinalPrice
		lastPrice := prices[len(prices)-1].FinalPrice
		priceChange := lastPrice - firstPrice

		priceChangePercent := 0.0
		if firstPrice > 0 {
			priceChangePercent = float64(priceChange) / float64(firstPrice) * 100
		}

		if math.Abs(priceChangePercent) < minChangePercent {
			continue
		}

		productsWithSignificantChanges++

		minPrice, maxPrice := firstPrice, firstPrice
		for _, price := range prices {
			if price.FinalPrice < minPrice {
				minPrice = price.FinalPrice
			}
			if price.FinalPrice > maxPrice {
				maxPrice = price.FinalPrice
			}
		}

		if y > 500 {
			pdf.AddPage()
			y = 30

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

		x = 30.0

		pdf.Rectangle(x, y, x+colWidths[0], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		name := product.Name
		if len(name) > 25 {
			name = name[:22] + "..."
		}
		pdf.Cell(nil, name)
		x += colWidths[0]

		pdf.Rectangle(x, y, x+colWidths[1], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		pdf.Cell(nil, product.VendorCode)
		x += colWidths[1]

		pdf.Rectangle(x, y, x+colWidths[2], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		pdf.Cell(nil, fmt.Sprintf("%d", firstPrice))
		x += colWidths[2]

		pdf.Rectangle(x, y, x+colWidths[3], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		pdf.Cell(nil, fmt.Sprintf("%d", lastPrice))
		x += colWidths[3]

		pdf.Rectangle(x, y, x+colWidths[4], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		pdf.Cell(nil, fmt.Sprintf("%d", priceChange))
		x += colWidths[4]

		pdf.Rectangle(x, y, x+colWidths[5], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		pdf.Cell(nil, fmt.Sprintf("%.2f%%", priceChangePercent))
		x += colWidths[5]

		pdf.Rectangle(x, y, x+colWidths[6], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		pdf.Cell(nil, fmt.Sprintf("%d", minPrice))
		x += colWidths[6]

		pdf.Rectangle(x, y, x+colWidths[7], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		pdf.Cell(nil, fmt.Sprintf("%d", maxPrice))
		x += colWidths[7]

		pdf.Rectangle(x, y, x+colWidths[8], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		pdf.Cell(nil, fmt.Sprintf("%d", len(prices)))

		y += rowHeight
	}

	if productsWithSignificantChanges == 0 {
		if g.defaultFont != fallbackFont {
			pdf.SetFont(g.defaultFont, "", 12)
		}
		pdf.SetX(30)
		pdf.SetY(y + 20)
		pdf.Cell(nil, fmt.Sprintf("No products found with price changes greater than %.1f%%", minChangePercent))
	}

	filename := fmt.Sprintf("price_report_%s_%s.pdf",
		startDate.Format("02-01-2006"),
		endDate.Format("02-01-2006"))
	filePath := filepath.Join(os.TempDir(), filename)

	if err := pdf.WritePdf(filePath); err != nil {
		return "", "", fmt.Errorf("error saving PDF: %w", err)
	}

	return filePath, filename, nil
}

func (g *PDFGenerator) GenerateStockReportPDF(ctx context.Context, startDate, endDate time.Time, minChangePercent float64) (string, string, error) {
	products, err := db.GetAllProducts(ctx, g.db)
	if err != nil {
		return "", "", fmt.Errorf("error getting products: %w", err)
	}

	warehouses, err := db.GetAllWarehouses(ctx, g.db)
	if err != nil {
		return "", "", fmt.Errorf("error getting warehouses: %w", err)
	}

	if len(products) == 0 {
		return "", "", fmt.Errorf("no products found")
	}

	pdf, err := g.createBasicPDF("landscape")
	if err != nil {
		return "", "", fmt.Errorf("error creating PDF: %w", err)
	}

	if g.defaultBold != fallbackFont {
		pdf.SetFont(g.defaultBold, "", 16)
	}
	title := fmt.Sprintf("Stock Report for Period %s - %s",
		startDate.Format("02.01.2006"),
		endDate.Format("02-01-2006"))
	pdf.SetX(30)
	pdf.SetY(20)
	pdf.Cell(nil, title)
	pdf.Br(20)

	headers := []string{"Product", "Vendor Code"}

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

	colWidths := []float64{120, 60}
	for range displayedWarehouses {
		colWidths = append(colWidths, 40)
	}
	colWidths = append(colWidths, 50, 50)

	headerHeight := 30.0
	rowHeight := 25.0

	if g.defaultBold != fallbackFont {
		pdf.SetFont(g.defaultBold, "", 10)
	}
	pdf.SetFillColor(221, 235, 247)
	x := 30.0
	y := pdf.GetY()

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

	productsWithSignificantChanges := 0

	for _, product := range products {
		warehouseStocks := make(map[int64][]models.StockRecord)
		totalInitialStock := 0
		totalFinalStock := 0
		hasStockData := false

		for _, warehouse := range warehouses {
			// Use stock snapshots instead of stocks
			stocks, err := db.GetStockSnapshotsForPeriod(ctx, g.db, product.ID, warehouse.ID, startDate, endDate)
			if err != nil {
				log.Printf("Error getting stock snapshots for product %d, warehouse %d: %v",
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
			continue
		}

		totalChangePercent := 0.0
		if totalInitialStock > 0 {
			totalChangePercent = float64(totalFinalStock-totalInitialStock) / float64(totalInitialStock) * 100
		} else if totalFinalStock > 0 {
			totalChangePercent = 100.0
		}

		if math.Abs(totalChangePercent) < minChangePercent {
			continue
		}

		productsWithSignificantChanges++

		if y > 500 {
			pdf.AddPage()
			y = 30

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

		x = 30.0

		pdf.Rectangle(x, y, x+colWidths[0], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		name := product.Name
		if len(name) > 25 {
			name = name[:22] + "..."
		}
		pdf.Cell(nil, name)
		x += colWidths[0]

		pdf.Rectangle(x, y, x+colWidths[1], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		pdf.Cell(nil, product.VendorCode)
		x += colWidths[1]

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

		pdf.Rectangle(x, y, x+colWidths[len(colWidths)-2], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		pdf.Cell(nil, fmt.Sprintf("%d", totalFinalStock))
		x += colWidths[len(colWidths)-2]

		pdf.Rectangle(x, y, x+colWidths[len(colWidths)-1], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		pdf.Cell(nil, fmt.Sprintf("%+d (%.1f%%)", totalFinalStock-totalInitialStock, totalChangePercent))

		y += rowHeight
	}

	if productsWithSignificantChanges == 0 {
		if g.defaultFont != fallbackFont {
			pdf.SetFont(g.defaultFont, "", 12)
		}
		pdf.SetX(30)
		pdf.SetY(y + 20)
		pdf.Cell(nil, fmt.Sprintf("No products found with stock changes greater than %.1f%%", minChangePercent))
	}

	if productsWithSignificantChanges > 0 {
		pdf.AddPage()
		if g.defaultBold != fallbackFont {
			pdf.SetFont(g.defaultBold, "", 14)
		}
		pdf.SetX(30)
		pdf.SetY(20)
		pdf.Cell(nil, "Stock Trend Analysis")
		pdf.Br(20)

		type ProductWithChange struct {
			Product       models.ProductRecord
			InitialStock  int
			FinalStock    int
			AbsChangePerc float64
			WarehouseData map[int64][]models.StockRecord
		}

		var productsToChart []ProductWithChange

		for _, product := range products {
			warehouseStocks := make(map[int64][]models.StockRecord)
			totalInitialStock := 0
			totalFinalStock := 0
			hasStockData := false

			for _, warehouse := range warehouses {
				// Use stock snapshots instead of stocks
				stocks, err := db.GetStockSnapshotsForPeriod(ctx, g.db, product.ID, warehouse.ID, startDate, endDate)
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

		sort.Slice(productsToChart, func(i, j int) bool {
			return productsToChart[i].AbsChangePerc > productsToChart[j].AbsChangePerc
		})

		maxChartsToShow := 5
		if len(productsToChart) > maxChartsToShow {
			productsToChart = productsToChart[:maxChartsToShow]
		}

		chartY := pdf.GetY()
		chartHeight := 120.0
		chartGap := 20.0

		for _, productData := range productsToChart {
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

			if g.defaultBold != fallbackFont {
				pdf.SetFont(g.defaultBold, "", 12)
			}
			pdf.SetX(30)
			pdf.SetY(chartY)

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

			type DayData struct {
				Date  time.Time
				Stock int
			}

			var dailyTotals []DayData

			dailyMap := make(map[string]int)
			dateFormat := "2006-01-02"

			for _, stocks := range productData.WarehouseData {
				for _, stock := range stocks {
					dateKey := stock.RecordedAt.Format(dateFormat)
					dailyMap[dateKey] += stock.Amount
				}
			}

			for dateStr, total := range dailyMap {
				date, _ := time.Parse(dateFormat, dateStr)
				dailyTotals = append(dailyTotals, DayData{Date: date, Stock: total})
			}

			sort.Slice(dailyTotals, func(i, j int) bool {
				return dailyTotals[i].Date.Before(dailyTotals[j].Date)
			})

			if len(dailyTotals) > 1 {
				chartWidth := 700.0
				marginLeft := 50.0
				marginBottom := 30.0
				chartY += 10

				minStock, maxStock := dailyTotals[0].Stock, dailyTotals[0].Stock
				for _, data := range dailyTotals {
					if data.Stock < minStock {
						minStock = data.Stock
					}
					if data.Stock > maxStock {
						maxStock = data.Stock
					}
				}

				yPadding := int(float64(maxStock-minStock) * 0.1)
				if yPadding < 1 {
					yPadding = 1
				}
				minStock = maximum(0, minStock-yPadding)
				maxStock += yPadding

				pdf.SetStrokeColor(0, 0, 0)

				pdf.Line(30+marginLeft, chartY+chartHeight-marginBottom,
					30+marginLeft+chartWidth-marginLeft, chartY+chartHeight-marginBottom)

				pdf.Line(30+marginLeft, chartY, 30+marginLeft, chartY+chartHeight-marginBottom)

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

					pdf.SetStrokeColor(200, 200, 200)
					pdf.Line(30+marginLeft, yPos, 30+marginLeft+chartWidth-marginLeft, yPos)
					pdf.SetStrokeColor(0, 0, 0)
				}

				numXTicks := minimum(len(dailyTotals), 6)
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

				pdf.SetStrokeColor(0, 0, 255)
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

				pdf.SetLineWidth(1)
			} else {
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

	filename := fmt.Sprintf("stock_report_%s_%s.pdf",
		startDate.Format("02-01-2006"),
		endDate.Format("02-01-2006"))
	filePath := filepath.Join(os.TempDir(), filename)

	if err := pdf.WritePdf(filePath); err != nil {
		return "", "", fmt.Errorf("error saving PDF: %w", err)
	}

	return filePath, filename, nil
}

func minimum(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maximum(a, b int) int {
	if a > b {
		return a
	}
	return b
}
