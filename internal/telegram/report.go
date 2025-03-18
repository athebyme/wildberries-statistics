package telegram

import (
	"context"
	"fmt"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/xuri/excelize/v2"
	"log"
	"os"
	"path/filepath"
	"time"
	"wbmonitoring/monitoring/internal/db"
)

func (b *Bot) sendReportMenu(chatID int64) {
	msg := tgbotapi.NewMessage(chatID, "Выберите тип отчета:")

	keyboard := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("📈 Отчет по ценам", "report_prices"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("📦 Отчет по остаткам", "report_stocks"),
		),
	)

	msg.ReplyMarkup = keyboard
	sentMsg, err := b.api.Send(msg)
	if err == nil {
		b.trackReportMessage(chatID, sentMsg.MessageID)
	}
}

func (b *Bot) generateReport(chatID int64, reportType, period, format string) {

	progressMsg := tgbotapi.NewMessage(chatID, "Генерация отчета... Пожалуйста, подождите.")
	sentMsg, err := b.api.Send(progressMsg)
	if err == nil {
		b.trackReportMessage(chatID, sentMsg.MessageID)
	}

	filePath, reportName, err := b.generateReportFile(reportType, period, format)
	if err != nil {
		log.Printf("Ошибка при генерации отчета: %v", err)
		errorMsg := tgbotapi.NewMessage(chatID, fmt.Sprintf("Ошибка при генерации отчета: %v", err))
		b.api.Send(errorMsg)
		return
	}

	fileBytes, err := os.ReadFile(filePath)
	if err != nil {
		log.Printf("Ошибка при чтении файла отчета: %v", err)
		errorMsg := tgbotapi.NewMessage(chatID, "Ошибка при подготовке отчета к отправке")
		b.api.Send(errorMsg)
		return
	}

	b.cleanupReportMessages(chatID)

	var reportTypeName string
	if reportType == "prices" {
		reportTypeName = "ценам"
	} else {
		reportTypeName = "остаткам"
	}

	doc := tgbotapi.NewDocument(chatID, tgbotapi.FileBytes{
		Name:  reportName,
		Bytes: fileBytes,
	})
	doc.Caption = fmt.Sprintf("Отчет по %s за %s", reportTypeName, b.getPeriodName(period))

	b.api.Send(doc)

	os.Remove(filePath)
}

// Метод для генерации и отправки ежедневного отчета по ценам через ExcelGenerator
func (b *Bot) generateDailyPriceReport(ctx context.Context, startDate, endDate time.Time) error {
	log.Println("Генерация ежедневного отчета по ценам...")

	if b.excelGenerator != nil {
		log.Println("Используем улучшенный ExcelGenerator")
		filePath, _, err := b.excelGenerator.GeneratePriceReportExcel(ctx, startDate, endDate)
		if err != nil {
			return fmt.Errorf("ошибка при генерации отчета по ценам: %w", err)
		}

		// Отправляем файл в Telegram
		doc := tgbotapi.NewDocument(b.chatID, tgbotapi.FilePath(filePath))
		doc.Caption = fmt.Sprintf("📈 Ежедневный отчет по ценам за %s",
			startDate.Format("02.01.2006"))
		_, err = b.api.Send(doc)
		if err != nil {
			return fmt.Errorf("ошибка при отправке отчета: %w", err)
		}

		return nil
	}

	return fmt.Errorf("error")

}

func (b *Bot) generateDailyStockReport(ctx context.Context, startDate, endDate time.Time) error {
	products, err := db.GetAllProducts(ctx, b.db)
	if err != nil {
		return fmt.Errorf("error getting products: %w", err)
	}

	if len(products) == 0 {
		b.api.Send(tgbotapi.NewMessage(b.chatID, "Товары не найдены в базе данных."))
		return nil
	}

	warehouses, err := db.GetAllWarehouses(ctx, b.db)
	if err != nil {
		return fmt.Errorf("error getting warehouses: %w", err)
	}

	f := excelize.NewFile()
	sheetName := "Ежедневный отчет по остаткам"
	f.SetSheetName("Sheet1", sheetName)

	headers := []string{
		"Товар", "Артикул", "Склад", "Начальный остаток (шт.)", "Текущий остаток (шт.)",
		"Изменение (шт.)", "Изменение (%)",
	}
	for i, header := range headers {
		cell := fmt.Sprintf("%c%d", 'A'+i, 1)
		f.SetCellValue(sheetName, cell, header)
	}

	headerStyle, _ := f.NewStyle(&excelize.Style{
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
	f.SetCellStyle(sheetName, "A1", string(rune('A'+len(headers)-1))+"1", headerStyle)

	row := 2
	changesFound := false

	for _, product := range products {
		for _, warehouse := range warehouses {
			stocks, err := db.GetStocksForPeriod(ctx, b.db, product.ID, warehouse.ID, startDate, endDate)
			if err != nil {
				log.Printf("Error getting stocks for product %d on warehouse %d: %v",
					product.ID, warehouse.ID, err)
				continue
			}

			if len(stocks) <= 1 {
				// Нет изменений в остатках за сегодня на этом складе
				continue
			}

			// Первый и последний остаток за период
			firstStock := stocks[0].Amount
			lastStock := stocks[len(stocks)-1].Amount

			// Если остаток не изменился, пропускаем запись
			if firstStock == lastStock {
				continue
			}

			changesFound = true

			// Рассчитываем изменение остатка за период
			stockChange := lastStock - firstStock
			stockChangePercent := float64(0)
			if firstStock > 0 {
				stockChangePercent = float64(stockChange) / float64(firstStock) * 100
			}

			// Добавляем данные в Excel
			f.SetCellValue(sheetName, fmt.Sprintf("A%d", row), product.Name)
			f.SetCellValue(sheetName, fmt.Sprintf("B%d", row), product.VendorCode)
			f.SetCellValue(sheetName, fmt.Sprintf("C%d", row), warehouse.Name)
			f.SetCellValue(sheetName, fmt.Sprintf("D%d", row), firstStock)
			f.SetCellValue(sheetName, fmt.Sprintf("E%d", row), lastStock)
			f.SetCellValue(sheetName, fmt.Sprintf("F%d", row), stockChange)
			f.SetCellValue(sheetName, fmt.Sprintf("G%d", row), stockChangePercent)

			row++
		}
	}

	// Если нет изменений в остатках за сегодня, отправляем уведомление и выходим
	if !changesFound {
		b.api.Send(tgbotapi.NewMessage(b.chatID, "За сегодня не было изменений в остатках товаров."))
		return nil
	}

	// Автонастройка ширины столбцов
	for i := range headers {
		col := string(rune('A' + i))
		width, _ := f.GetColWidth(sheetName, col)
		if width < 15 {
			f.SetColWidth(sheetName, col, col, 15)
		}
	}

	// Устанавливаем стиль для чисел
	numberStyle, _ := f.NewStyle(&excelize.Style{
		NumFmt: 1, // Целое число
	})
	f.SetCellStyle(sheetName, "D2", fmt.Sprintf("F%d", row-1), numberStyle)

	//  стиль для процентов
	percentStyle, _ := f.NewStyle(&excelize.Style{
		NumFmt: 10,
	})
	f.SetCellStyle(sheetName, "G2", fmt.Sprintf("G%d", row-1), percentStyle)

	filename := fmt.Sprintf("daily_stock_report_%s.xlsx", startDate.Format("02-01-2006"))
	filepath := filepath.Join(os.TempDir(), filename)
	if err := f.SaveAs(filepath); err != nil {
		return fmt.Errorf("error saving Excel file: %w", err)
	}

	doc := tgbotapi.NewDocument(b.chatID, tgbotapi.FilePath(filepath))
	doc.Caption = fmt.Sprintf("📦 Ежедневный отчет по изменениям остатков за %s",
		startDate.Format("02.01.2006"))
	_, err = b.api.Send(doc)
	if err != nil {
		return fmt.Errorf("error sending Excel file: %w", err)
	}

	os.Remove(filepath)
	return nil
}
