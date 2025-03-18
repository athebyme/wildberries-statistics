package telegram

import (
	"context"
	"fmt"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/xuri/excelize/v2"
	"log"
	"os"
	"path/filepath"
	"strings"
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
	// Генерируем ID операции и создаем сообщение о начале генерации
	operationID := generateOperationID()

	var reportName string
	if reportType == "prices" {
		reportName = "отчета по ценам"
	} else {
		reportName = "отчета по остаткам"
	}

	periodName := b.getPeriodName(period)

	// Создаем операцию в трекере прогресса
	// Устанавливаем общее количество шагов (примерно) для отслеживания прогресса
	totalSteps := 100
	_ = b.progressTracker.StartOperation(operationID, fmt.Sprintf("Генерация %s за %s", reportName, periodName), totalSteps)

	// Отправляем сообщение о начале генерации с информацией о возможности отслеживания прогресса
	progressMsg := tgbotapi.NewMessage(chatID, fmt.Sprintf(
		"🔄 Начата генерация %s за %s в формате %s.\n\n"+
			"ID операции: `%s`\n\n"+
			"Вы можете отслеживать прогресс с помощью команды:\n"+
			"`/status %s`",
		reportName, periodName, strings.ToUpper(format), operationID, operationID))
	progressMsg.ParseMode = "Markdown"
	sentMsg, err := b.api.Send(progressMsg)
	if err == nil {
		b.trackReportMessage(chatID, sentMsg.MessageID)
	}

	// Запускаем горутину для периодического обновления сообщения
	go b.updateReportProgress(chatID, sentMsg.MessageID, operationID)

	// Обновляем прогресс: начинаем обработку
	b.progressTracker.UpdateProgress(operationID, 5, 0, 0, "Определение параметров отчета")

	// Определяем и рассчитываем даты для отчета
	startDate, endDate, err := b.calculateReportDates(period)
	if err != nil {
		errorMsg := fmt.Sprintf("Ошибка при определении дат для отчета: %v", err)
		b.progressTracker.CompleteOperation(operationID, errorMsg)
		b.api.Send(tgbotapi.NewMessage(chatID, "❌ "+errorMsg))
		return
	}

	b.progressTracker.UpdateProgress(operationID, 10, 0, 0,
		fmt.Sprintf("Период отчета: с %s по %s", startDate.Format("02.01.2006"), endDate.Format("02.01.2006")))

	// Запускаем генерацию отчета в отдельной горутине
	go func() {
		defer func() {
			if r := recover(); r != nil {
				errorMsg := fmt.Sprintf("Паника при генерации отчета: %v", r)
				b.progressTracker.CompleteOperation(operationID, errorMsg)
				b.api.Send(tgbotapi.NewMessage(chatID, "❌ "+errorMsg))
			}
		}()

		b.progressTracker.UpdateProgress(operationID, 15, 0, 0, "Начало генерации файла отчета")

		// Генерация файла отчета
		filePath, fileName, err := b.generateReportFile(reportType, period, format, operationID)
		if err != nil {
			errorMsg := fmt.Sprintf("Ошибка при генерации отчета: %v", err)
			b.progressTracker.CompleteOperation(operationID, errorMsg)
			b.api.Send(tgbotapi.NewMessage(chatID, "❌ "+errorMsg))
			return
		}

		b.progressTracker.UpdateProgress(operationID, 90, 0, 0, "Файл отчета создан, подготовка к отправке")

		// Чтение файла для отправки
		fileBytes, err := os.ReadFile(filePath)
		if err != nil {
			errorMsg := fmt.Sprintf("Ошибка при чтении файла отчета: %v", err)
			b.progressTracker.CompleteOperation(operationID, errorMsg)
			b.api.Send(tgbotapi.NewMessage(chatID, "❌ "+errorMsg))
			return
		}

		b.progressTracker.UpdateProgress(operationID, 95, 0, 0, "Отправка файла")

		// Очищаем предыдущие сообщения о прогрессе
		b.cleanupReportMessages(chatID)

		var reportTypeName string
		if reportType == "prices" {
			reportTypeName = "ценам"
		} else {
			reportTypeName = "остаткам"
		}

		// Отправляем финальный отчет
		doc := tgbotapi.NewDocument(chatID, tgbotapi.FileBytes{
			Name:  fileName,
			Bytes: fileBytes,
		})
		doc.Caption = fmt.Sprintf("📊 Отчет по %s за %s", reportTypeName, b.getPeriodName(period))

		_, err = b.api.Send(doc)
		if err != nil {
			errorMsg := fmt.Sprintf("Ошибка при отправке файла: %v", err)
			b.progressTracker.CompleteOperation(operationID, errorMsg)
			b.api.Send(tgbotapi.NewMessage(chatID, "❌ "+errorMsg))
			return
		}

		// Удаление временного файла
		os.Remove(filePath)

		// Завершение операции
		b.progressTracker.CompleteOperation(operationID, "")

		// Отправляем сообщение об успешном завершении
		b.api.Send(tgbotapi.NewMessage(chatID,
			fmt.Sprintf("✅ Отчет успешно сгенерирован и отправлен! ID операции: `%s`", operationID)))
	}()
}

// Функция для обновления сообщения о прогрессе
func (b *Bot) updateReportProgress(chatID int64, messageID int, operationID string) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	// Ограничиваем время обновления сообщений
	timeout := time.After(30 * time.Minute)

	for {
		select {
		case <-ticker.C:
			// Получаем текущий прогресс
			op := b.progressTracker.GetOperation(operationID)
			if op == nil {
				return // Операция не найдена
			}

			// Если операция завершена, останавливаем обновления
			if op.IsComplete {
				return
			}

			// Формируем сообщение о прогрессе
			percentComplete := 0
			if op.TotalItems > 0 {
				percentComplete = int((float64(op.ProcessedItems) / float64(op.TotalItems)) * 100)
			}

			// Создаем индикатор прогресса
			progressBar := ""
			barLength := 10
			filledChars := int(float64(barLength) * float64(percentComplete) / 100.0)

			for i := 0; i < barLength; i++ {
				if i < filledChars {
					progressBar += "▓"
				} else {
					progressBar += "░"
				}
			}

			msgText := fmt.Sprintf(
				"🔄 Генерация отчета (ID: `%s`)\n\n"+
					"*Прогресс*: %d%% %s\n\n"+
					"*Последние операции*:\n",
				op.ID, percentComplete, progressBar)

			// Добавляем последние 3 сообщения
			messagesCount := len(op.Messages)
			start := messagesCount - 3
			if start < 0 {
				start = 0
			}

			for i := start; i < messagesCount; i++ {
				msg := op.Messages[i]
				msgText += fmt.Sprintf("• %s\n", msg.Message)
			}

			// Добавляем информацию о времени выполнения
			elapsedTime := time.Since(op.StartTime).Round(time.Second)
			msgText += fmt.Sprintf("\n*Время выполнения*: %s", elapsedTime)

			// Если есть оценка времени завершения, добавляем ее
			if op.EstimatedEndTime.After(time.Now()) {
				remainingTime := op.EstimatedEndTime.Sub(time.Now()).Round(time.Second)
				msgText += fmt.Sprintf("\n*Осталось примерно*: %s", remainingTime)
			}

			// Обновляем сообщение
			editMsg := tgbotapi.NewEditMessageText(chatID, messageID, msgText)
			editMsg.ParseMode = "Markdown"
			_, err := b.api.Send(editMsg)
			if err != nil {
				// Если не удалось обновить сообщение, логируем ошибку и продолжаем
				log.Printf("Ошибка при обновлении сообщения о прогрессе: %v", err)
			}

		case <-timeout:
			// По истечении таймаута прекращаем обновления
			return
		}
	}
}

// Расчет дат для отчета на основе периода
func (b *Bot) calculateReportDates(period string) (startDate, endDate time.Time, err error) {
	now := time.Now()

	// Обработка произвольного периода
	if strings.HasPrefix(period, "custom_") {
		parts := strings.Split(period, "_")
		if len(parts) != 3 {
			return time.Time{}, time.Time{}, fmt.Errorf("некорректный формат произвольного периода")
		}

		startDateStr, endDateStr := parts[1], parts[2]
		startDate, err = time.ParseInLocation("02.01.2006", startDateStr, now.Location())
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("ошибка разбора начальной даты: %w", err)
		}

		endDate, err = time.ParseInLocation("02.01.2006", endDateStr, now.Location())
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("ошибка разбора конечной даты: %w", err)
		}

		// Устанавливаем конец дня для конечной даты
		endDate = time.Date(endDate.Year(), endDate.Month(), endDate.Day(), 23, 59, 59, 999999999, endDate.Location())
	} else {
		// Стандартные периоды
		switch period {
		case "day":
			startDate = time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
			endDate = now
		case "week":
			startDate = now.AddDate(0, 0, -7)
			endDate = now
		case "month":
			startDate = now.AddDate(0, -1, 0)
			endDate = now
		default:
			return time.Time{}, time.Time{}, fmt.Errorf("неизвестный период: %s", period)
		}
	}

	return startDate, endDate, nil
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
