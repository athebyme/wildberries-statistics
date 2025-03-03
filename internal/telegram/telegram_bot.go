package telegram

import (
	"context"
	"fmt"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/jmoiron/sqlx"
	"github.com/xuri/excelize/v2" // Add this package for Excel file creation
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"wbmonitoring/monitoring/internal/db"
	"wbmonitoring/monitoring/internal/models"
)

// Bot представляет Telegram бота с расширенным функционалом
type Bot struct {
	api          *tgbotapi.BotAPI
	chatID       int64
	db           *sqlx.DB
	allowedUsers map[int64]bool // Список разрешенных пользователей
	config       ReportConfig
}

// Структура конфигурации для отчетов
type ReportConfig struct {
	// Минимальный процент изменения цены для включения в отчет динамики
	MinPriceChangePercent float64 `json:"minPriceChangePercent"`
	// Минимальный процент изменения остатка для включения в отчет динамики
	MinStockChangePercent float64 `json:"minStockChangePercent"`
}

// NewBot создает нового Telegram бота
func NewBot(token string, chatID int64, db *sqlx.DB, allowedUserIDs []int64, config ReportConfig) (*Bot, error) {
	log.Printf("allowed users %v", allowedUserIDs)
	api, err := tgbotapi.NewBotAPI(token)
	if err != nil {
		return nil, fmt.Errorf("initializing telegram bot: %w", err)
	}

	// Создаем карту разрешенных пользователей для быстрого доступа
	allowedUsers := make(map[int64]bool)
	for _, userID := range allowedUserIDs {
		allowedUsers[userID] = true
	}

	return &Bot{
		api:          api,
		chatID:       chatID,
		db:           db,
		allowedUsers: allowedUsers,
		config:       config,
	}, nil
}

// SendTelegramAlert отправляет оповещение в Telegram
func (b *Bot) SendTelegramAlert(message string) error {
	msg := tgbotapi.NewMessage(b.chatID, message)
	_, err := b.api.Send(msg)
	return err
}

// SendTelegramAlertWithParseMode отправляет оповещение в Telegram с указанным режимом форматирования
func (b *Bot) SendTelegramAlertWithParseMode(message, parseMode string) error {
	msg := tgbotapi.NewMessage(b.chatID, message)
	msg.ParseMode = parseMode
	_, err := b.api.Send(msg)
	return err
}

// StartBot запускает обработку сообщений бота
func (b *Bot) StartBot(ctx context.Context) {
	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60

	updates := b.api.GetUpdatesChan(u)

	for {
		select {
		case update := <-updates:
			if update.Message != nil {
				// Проверяем, разрешен ли доступ пользователю
				if !b.allowedUsers[update.Message.From.ID] {
					log.Printf("Unauthorized access attempt from user ID: %d", update.Message.From.ID)
					b.api.Send(tgbotapi.NewMessage(update.Message.Chat.ID, "Извините, у вас нет доступа к этому боту."))
					continue
				}

				// Обработка команд
				b.handleMessage(update.Message)
			} else if update.CallbackQuery != nil {
				// Проверяем, разрешен ли доступ пользователю
				if !b.allowedUsers[update.CallbackQuery.From.ID] {
					log.Printf("Unauthorized callback query from user ID: %d", update.CallbackQuery.From.ID)
					continue
				}

				// Обработка callback запросов от inline кнопок
				b.handleCallbackQuery(update.CallbackQuery)
			}
		case <-ctx.Done():
			return
		}
	}
}

// handleMessage обрабатывает входящие сообщения
func (b *Bot) handleMessage(message *tgbotapi.Message) {
	// Обработка команд
	switch message.Command() {
	case "start":
		b.sendWelcomeMessage(message.Chat.ID)
	case "report":
		b.sendReportMenu(message.Chat.ID)
	case "help":
		b.sendHelpMessage(message.Chat.ID)
	default:
		// Если это не команда, просто отправляем меню отчетов
		if message.Text != "" {
			b.sendReportMenu(message.Chat.ID)
		}
	}
}

// sendWelcomeMessage отправляет приветственное сообщение
func (b *Bot) sendWelcomeMessage(chatID int64) {
	welcomeText := `Добро пожаловать в бот мониторинга Wildberries!

Бот отслеживает изменения цен и остатков товаров на Wildberries и отправляет уведомления при значительных изменениях.

Для получения отчетов используйте команду /report или нажмите на кнопки ниже.`

	msg := tgbotapi.NewMessage(chatID, welcomeText)
	msg.ReplyMarkup = b.getMainKeyboard()
	b.api.Send(msg)
}

// sendHelpMessage отправляет сообщение с помощью
func (b *Bot) sendHelpMessage(chatID int64) {
	helpText := `Доступные команды:

/start - Начать работу с ботом
/report - Получить отчет по ценам или остаткам
/help - Показать это сообщение

Для получения отчета за период нажмите соответствующую кнопку и выберите интересующий вас период времени.

Отчеты доступны в текстовом формате или в формате Excel. Выберите нужный формат в меню отчета.`

	msg := tgbotapi.NewMessage(chatID, helpText)
	b.api.Send(msg)
}

// getMainKeyboard возвращает основную клавиатуру бота
func (b *Bot) getMainKeyboard() tgbotapi.ReplyKeyboardMarkup {
	keyboard := tgbotapi.NewReplyKeyboard(
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton("📊 Получить отчет"),
		),
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton("❓ Помощь"),
		),
	)
	keyboard.ResizeKeyboard = true
	return keyboard
}

// sendReportMenu отправляет меню для выбора типа отчета
func (b *Bot) sendReportMenu(chatID int64) {
	msg := tgbotapi.NewMessage(chatID, "Выберите тип отчета:")

	// Создаем inline клавиатуру
	keyboard := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("📈 Отчет по ценам", "report_prices"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("📦 Отчет по остаткам", "report_stocks"),
		),
	)

	msg.ReplyMarkup = keyboard
	b.api.Send(msg)
}

// handleCallbackQuery обрабатывает callback запросы от inline кнопок
func (b *Bot) handleCallbackQuery(query *tgbotapi.CallbackQuery) {
	// Отправляем уведомление о получении запроса
	callback := tgbotapi.NewCallback(query.ID, "")
	b.api.Request(callback)

	parts := strings.Split(query.Data, "_")

	// Обработка выбора типа отчета
	if len(parts) >= 2 && parts[0] == "report" {
		reportType := parts[1]

		// Если пришло только два параметра, значит это выбор периода
		if len(parts) == 2 {
			b.sendPeriodSelection(query.Message.Chat.ID, reportType)
			return
		}

		// Если пришло три параметра, значит это запрос отчета за период
		if len(parts) == 3 {
			period := parts[2]
			b.sendFormatSelection(query.Message.Chat.ID, reportType, period)
			return
		}

		// Если пришло четыре параметра, значит это запрос отчета в определенном формате
		if len(parts) == 4 {
			period := parts[2]
			format := parts[3]
			b.generateReport(query.Message.Chat.ID, reportType, period, format)
			return
		}
	}
}

// sendPeriodSelection отправляет меню выбора периода для отчета
func (b *Bot) sendPeriodSelection(chatID int64, reportType string) {
	var msgText string
	if reportType == "prices" {
		msgText = "Выберите период для отчета по ценам:"
	} else {
		msgText = "Выберите период для отчета по остаткам:"
	}

	msg := tgbotapi.NewMessage(chatID, msgText)

	// Создаем inline клавиатуру с периодами
	keyboard := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("За день", fmt.Sprintf("report_%s_day", reportType)),
			tgbotapi.NewInlineKeyboardButtonData("За неделю", fmt.Sprintf("report_%s_week", reportType)),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("За месяц", fmt.Sprintf("report_%s_month", reportType)),
			tgbotapi.NewInlineKeyboardButtonData("Произвольный период", fmt.Sprintf("report_%s_custom", reportType)),
		),
	)

	msg.ReplyMarkup = keyboard
	b.api.Send(msg)
}

// sendFormatSelection отправляет меню выбора формата отчета
func (b *Bot) sendFormatSelection(chatID int64, reportType, period string) {
	msgText := "Выберите формат отчета:"

	msg := tgbotapi.NewMessage(chatID, msgText)

	// Создаем inline клавиатуру с форматами
	keyboard := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("Текстовый", fmt.Sprintf("report_%s_%s_text", reportType, period)),
			tgbotapi.NewInlineKeyboardButtonData("Excel", fmt.Sprintf("report_%s_%s_excel", reportType, period)),
		),
	)

	msg.ReplyMarkup = keyboard
	b.api.Send(msg)
}

// addDynamicChangesSheet добавляет лист с динамикой изменений во времени для товаров
// с изменениями больше порогового значения
func addDynamicChangesSheet(
	f *excelize.File,
	products []models.ProductRecord,
	ctx context.Context,
	database *sqlx.DB,
	startDate, endDate time.Time,
	isPriceReport bool,
	config ReportConfig,
	warehouses []models.Warehouse,
) error {
	// Название листа в зависимости от типа отчета
	sheetName := "Динамика цен"
	if !isPriceReport {
		sheetName = "Динамика остатков"
	}

	// Создаем новый лист
	_, err := f.NewSheet(sheetName)
	if err != nil {
		return fmt.Errorf("ошибка при создании листа динамики: %v", err)
	}

	// Устанавливаем заголовки
	var headers []string
	if isPriceReport {
		headers = []string{
			"Товар", "Артикул", "Дата", "Цена (₽)", "Изменение (₽)", "Изменение (%)",
		}
	} else {
		headers = []string{
			"Товар", "Артикул", "Склад", "Дата", "Остаток (шт.)", "Изменение (шт.)", "Изменение (%)",
		}
	}

	for i, header := range headers {
		cell := fmt.Sprintf("%c%d", 'A'+i, 1)
		f.SetCellValue(sheetName, cell, header)
	}

	// Устанавливаем стиль для заголовков
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

	// Заполняем данные
	row := 2
	productsAdded := 0

	// Для отчета по остаткам - обрабатываем каждый склад отдельно
	if isPriceReport {
		// Обрабатываем цены
		for _, product := range products {
			// Получаем все цены за период
			prices, err := db.GetPricesForPeriod(ctx, database, product.ID, startDate, endDate)
			if err != nil {
				log.Printf("Error getting prices for product %d: %v", product.ID, err)
				continue
			}

			if len(prices) < 2 {
				continue // Нужно минимум 2 записи для отслеживания изменений
			}

			// Проверяем, есть ли существенные изменения
			firstPrice := prices[0].FinalPrice
			lastPrice := prices[len(prices)-1].FinalPrice
			totalChangePercent := 0.0
			if firstPrice > 0 {
				totalChangePercent = float64(lastPrice-firstPrice) / float64(firstPrice) * 100
			}

			// Проверяем, превышает ли изменение пороговое значение
			if math.Abs(totalChangePercent) < config.MinPriceChangePercent {
				continue
			}

			// Добавляем товар в отчет динамики
			var prevPrice int
			var firstEntryForProduct bool = true

			for i, price := range prices {
				// Пропускаем первую запись для расчета изменений
				if i == 0 {
					prevPrice = price.FinalPrice
					continue
				}

				// Рассчитываем изменение по сравнению с предыдущей записью
				priceChange := price.FinalPrice - prevPrice
				changePercent := 0.0
				if prevPrice > 0 {
					changePercent = float64(priceChange) / float64(prevPrice) * 100
				}

				// Добавляем только если есть изменение цены
				if priceChange != 0 {
					// Если это первая запись для данного товара, добавляем имя и артикул
					if firstEntryForProduct {
						f.SetCellValue(sheetName, fmt.Sprintf("A%d", row), product.Name)
						f.SetCellValue(sheetName, fmt.Sprintf("B%d", row), product.VendorCode)
						firstEntryForProduct = false
						productsAdded++
					} else {
						// Для последующих записей оставляем пустыми ячейки имени и артикула
						f.SetCellValue(sheetName, fmt.Sprintf("A%d", row), "")
						f.SetCellValue(sheetName, fmt.Sprintf("B%d", row), "")
					}

					f.SetCellValue(sheetName, fmt.Sprintf("C%d", row), price.RecordedAt.Format("02.01.2006 15:04"))
					f.SetCellValue(sheetName, fmt.Sprintf("D%d", row), float64(price.FinalPrice)/100)
					f.SetCellValue(sheetName, fmt.Sprintf("E%d", row), float64(priceChange)/100)
					f.SetCellValue(sheetName, fmt.Sprintf("F%d", row), changePercent)

					row++
				}

				prevPrice = price.FinalPrice
			}
		}
	} else {
		// Обрабатываем остатки для каждого товара по каждому складу
		for _, product := range products {
			for _, warehouse := range warehouses {
				// Получаем историю остатков
				stocks, err := db.GetStocksForPeriod(ctx, database, product.ID, warehouse.ID, startDate, endDate)
				if err != nil {
					log.Printf("Error getting stocks for product %d on warehouse %d: %v",
						product.ID, warehouse.ID, err)
					continue
				}

				if len(stocks) < 2 {
					continue // Нужно минимум 2 записи для отслеживания изменений
				}

				// Проверяем, есть ли существенные изменения
				firstStock := stocks[0].Amount
				lastStock := stocks[len(stocks)-1].Amount
				totalChangePercent := 0.0
				if firstStock > 0 {
					totalChangePercent = float64(lastStock-firstStock) / float64(firstStock) * 100
				} else if firstStock == 0 && lastStock > 0 {
					// Если начальный остаток был 0, а теперь есть товары - это значительное изменение
					totalChangePercent = 100.0
				}

				// Проверяем, превышает ли изменение пороговое значение
				if math.Abs(totalChangePercent) < config.MinStockChangePercent {
					continue
				}

				// Добавляем товар в отчет динамики
				var prevStock int
				var firstEntryForProduct bool = true

				for i, stock := range stocks {
					// Пропускаем первую запись для расчета изменений
					if i == 0 {
						prevStock = stock.Amount
						continue
					}

					// Рассчитываем изменение по сравнению с предыдущей записью
					stockChange := stock.Amount - prevStock
					changePercent := 0.0
					if prevStock > 0 {
						changePercent = float64(stockChange) / float64(prevStock) * 100
					} else if prevStock == 0 && stock.Amount > 0 {
						changePercent = 100.0
					}

					// Добавляем только если есть изменение остатка
					if stockChange != 0 {
						// Если это первая запись для данного товара, добавляем имя и артикул
						if firstEntryForProduct {
							f.SetCellValue(sheetName, fmt.Sprintf("A%d", row), product.Name)
							f.SetCellValue(sheetName, fmt.Sprintf("B%d", row), product.VendorCode)
							f.SetCellValue(sheetName, fmt.Sprintf("C%d", row), warehouse.Name)
							firstEntryForProduct = false
							productsAdded++
						} else {
							// Для последующих записей оставляем пустыми ячейки имени и артикула
							f.SetCellValue(sheetName, fmt.Sprintf("A%d", row), "")
							f.SetCellValue(sheetName, fmt.Sprintf("B%d", row), "")
							f.SetCellValue(sheetName, fmt.Sprintf("C%d", row), "")
						}

						f.SetCellValue(sheetName, fmt.Sprintf("D%d", row), stock.RecordedAt.Format("02.01.2006 15:04"))
						f.SetCellValue(sheetName, fmt.Sprintf("E%d", row), stock.Amount)
						f.SetCellValue(sheetName, fmt.Sprintf("F%d", row), stockChange)
						f.SetCellValue(sheetName, fmt.Sprintf("G%d", row), changePercent)

						row++
					}

					prevStock = stock.Amount
				}
			}
		}
	}

	// Если товаров с существенными изменениями не найдено
	if productsAdded == 0 {
		emptyRow := 3
		if isPriceReport {
			f.SetCellValue(sheetName, fmt.Sprintf("A%d", emptyRow),
				fmt.Sprintf("Товары с изменением цены более %.1f%% не найдены", config.MinPriceChangePercent))
		} else {
			f.SetCellValue(sheetName, fmt.Sprintf("A%d", emptyRow),
				fmt.Sprintf("Товары с изменением остатка более %.1f%% не найдены", config.MinStockChangePercent))
		}
	}

	// Автонастройка ширины столбцов
	for i := range headers {
		col := string(rune('A' + i))
		width, _ := f.GetColWidth(sheetName, col)
		if width < 15 {
			f.SetColWidth(sheetName, col, col, 15)
		}
	}

	// Устанавливаем стиль для чисел и процентов
	if isPriceReport {
		// Стиль для цен с двумя десятичными знаками
		numberStyle, _ := f.NewStyle(&excelize.Style{
			NumFmt: 2, // Формат с двумя десятичными знаками
		})
		f.SetCellStyle(sheetName, "D2", fmt.Sprintf("E%d", row-1), numberStyle)

		// Стиль для процентов
		percentStyle, _ := f.NewStyle(&excelize.Style{
			NumFmt: 10, // Процентный формат
		})
		f.SetCellStyle(sheetName, "F2", fmt.Sprintf("F%d", row-1), percentStyle)
	} else {
		// Стиль для целых чисел
		numberStyle, _ := f.NewStyle(&excelize.Style{
			NumFmt: 1, // Целое число
		})
		f.SetCellStyle(sheetName, "E2", fmt.Sprintf("F%d", row-1), numberStyle)

		// Стиль для процентов
		percentStyle, _ := f.NewStyle(&excelize.Style{
			NumFmt: 10, // Процентный формат
		})
		f.SetCellStyle(sheetName, "G2", fmt.Sprintf("G%d", row-1), percentStyle)
	}

	// Добавим группировку по товарам (объединение строк одного товара визуально)
	currentProduct := ""
	for r := 2; r < row; r++ {
		productName, _ := f.GetCellValue(sheetName, fmt.Sprintf("A%d", r))
		if productName != "" {
			// Если начинается новый товар и текущий товар не пустой
			if currentProduct != "" && currentProduct != productName {
				// Применяем тонкий стиль границы для визуального разделения предыдущего товара
				borderStyle, _ := f.NewStyle(&excelize.Style{
					Border: []excelize.Border{
						{Type: "bottom", Color: "#CCCCCC", Style: 1},
					},
				})
				lastCol := string(rune('A' + len(headers) - 1))
				f.SetCellStyle(sheetName, "A"+strconv.Itoa(r-1), lastCol+strconv.Itoa(r-1), borderStyle)
			}
			currentProduct = productName

		}
	}

	return nil
}

// generateReport генерирует и отправляет отчет за выбранный период
func (b *Bot) generateReport(chatID int64, reportType, period, format string) {
	// Отправляем сообщение о начале генерации отчета
	statusMsg, _ := b.api.Send(tgbotapi.NewMessage(chatID, "Генерация отчета... Пожалуйста, подождите."))

	var startDate, endDate time.Time
	now := time.Now()

	// Определяем даты начала и конца периода
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
	case "custom":
		// Для произвольного периода отправляем сообщение с инструкциями
		b.api.Send(tgbotapi.NewMessage(chatID, "Функция выбора произвольного периода находится в разработке. Пожалуйста, используйте стандартные периоды."))
		return
	default:
		b.api.Send(tgbotapi.NewMessage(chatID, "Неизвестный период. Пожалуйста, выберите корректный период."))
		return
	}

	// Генерируем отчет в зависимости от типа и формата
	if reportType == "prices" {
		if format == "text" {
			b.generatePriceReport(chatID, startDate, endDate)
		} else if format == "excel" {
			b.generatePriceReportExcel(chatID, startDate, endDate, b.config)
		} else {
			b.api.Send(tgbotapi.NewMessage(chatID, "Неизвестный формат отчета. Пожалуйста, выберите корректный формат."))
		}
	} else if reportType == "stocks" {
		if format == "text" {
			b.generateStockReport(chatID, startDate, endDate)
		} else if format == "excel" {
			b.generateStockReportExcel(chatID, startDate, endDate, b.config)
		} else {
			b.api.Send(tgbotapi.NewMessage(chatID, "Неизвестный формат отчета. Пожалуйста, выберите корректный формат."))
		}
	} else {
		b.api.Send(tgbotapi.NewMessage(chatID, "Неизвестный тип отчета. Пожалуйста, выберите корректный тип."))
	}

	// Удаляем сообщение о генерации отчета
	deleteMsg := tgbotapi.NewDeleteMessage(chatID, statusMsg.MessageID)
	b.api.Request(deleteMsg)
}

// generatePriceReport генерирует отчет по ценам в текстовом формате
func (b *Bot) generatePriceReport(chatID int64, startDate, endDate time.Time) {
	ctx := context.Background()

	// Получаем все товары
	products, err := db.GetAllProducts(ctx, b.db)
	if err != nil {
		b.api.Send(tgbotapi.NewMessage(chatID, fmt.Sprintf("Ошибка при получении списка товаров: %v", err)))
		return
	}

	if len(products) == 0 {
		b.api.Send(tgbotapi.NewMessage(chatID, "Товары не найдены в базе данных."))
		return
	}

	// Формируем отчет
	reportText := fmt.Sprintf("📈 Отчет по ценам за период %s - %s\n\n",
		startDate.Format("02.01.2006"),
		endDate.Format("02.01.2006"))

	for _, product := range products {
		// Получаем историю цен для товара за период
		prices, err := db.GetPricesForPeriod(ctx, b.db, product.ID, startDate, endDate)
		if err != nil {
			log.Printf("Error getting prices for product %d: %v", product.ID, err)
			continue
		}

		if len(prices) == 0 {
			continue
		}

		// Находим максимальную и минимальную цену за период
		var minPrice, maxPrice, firstPrice, lastPrice int
		firstPrice = prices[0].FinalPrice
		lastPrice = prices[len(prices)-1].FinalPrice
		minPrice = firstPrice
		maxPrice = firstPrice

		for _, price := range prices {
			if price.FinalPrice < minPrice {
				minPrice = price.FinalPrice
			}
			if price.FinalPrice > maxPrice {
				maxPrice = price.FinalPrice
			}
		}

		// Рассчитываем изменение цены за период
		priceChange := lastPrice - firstPrice
		priceChangePercent := float64(0)
		if firstPrice > 0 {
			priceChangePercent = float64(priceChange) / float64(firstPrice) * 100
		}

		// Добавляем информацию о товаре в отчет
		reportText += fmt.Sprintf("Товар: %s (арт. %s)\n", product.Name, product.VendorCode)
		reportText += fmt.Sprintf("Начальная цена: %d₽\n", firstPrice)
		reportText += fmt.Sprintf("Конечная цена: %d₽\n", lastPrice)
		reportText += fmt.Sprintf("Изменение: %d₽ (%.2f%%)\n", priceChange, priceChangePercent)
		reportText += fmt.Sprintf("Мин. цена: %d₽, Макс. цена: %d₽\n", minPrice, maxPrice)
		reportText += fmt.Sprintf("Количество записей: %d\n\n", len(prices))
	}

	// Отправляем отчет
	if len(reportText) > 4096 {
		// Telegram имеет ограничение в 4096 символов на сообщение
		// Разбиваем длинный отчет на части
		for i := 0; i < len(reportText); i += 4000 {
			end := i + 4000
			if end > len(reportText) {
				end = len(reportText)
			}
			b.api.Send(tgbotapi.NewMessage(chatID, reportText[i:end]))
		}
	} else {
		b.api.Send(tgbotapi.NewMessage(chatID, reportText))
	}
}

// generatePriceReportExcel генерирует отчет по ценам в формате Excel
func (b *Bot) generatePriceReportExcel(chatID int64, startDate, endDate time.Time, config ReportConfig) {
	ctx := context.Background()

	// Код остается тот же, как был раньше, до создания Excel файла
	products, err := db.GetAllProducts(ctx, b.db)
	if err != nil {
		b.api.Send(tgbotapi.NewMessage(chatID, fmt.Sprintf("Ошибка при получении списка товаров: %v", err)))
		return
	}

	if len(products) == 0 {
		b.api.Send(tgbotapi.NewMessage(chatID, "Товары не найдены в базе данных."))
		return
	}

	// Создаем новый Excel файл
	f := excelize.NewFile()
	sheetName := "Отчет по ценам"
	f.SetSheetName("Sheet1", sheetName)

	// Устанавливаем заголовки
	headers := []string{
		"Товар", "Артикул", "Начальная цена (₽)", "Конечная цена (₽)",
		"Изменение (₽)", "Изменение (%)", "Мин. цена (₽)", "Макс. цена (₽)", "Количество записей",
	}
	for i, header := range headers {
		cell := fmt.Sprintf("%c%d", 'A'+i, 1)
		f.SetCellValue(sheetName, cell, header)
	}

	// Устанавливаем стиль для заголовков
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

	// Заполняем данные
	row := 2
	for _, product := range products {
		// Получаем историю цен для товара за период
		prices, err := db.GetPricesForPeriod(ctx, b.db, product.ID, startDate, endDate)
		if err != nil {
			log.Printf("Error getting prices for product %d: %v", product.ID, err)
			continue
		}

		if len(prices) == 0 {
			continue
		}

		// Находим максимальную и минимальную цену за период
		var minPrice, maxPrice, firstPrice, lastPrice int
		firstPrice = prices[0].FinalPrice
		lastPrice = prices[len(prices)-1].FinalPrice
		minPrice = firstPrice
		maxPrice = firstPrice

		for _, price := range prices {
			if price.FinalPrice < minPrice {
				minPrice = price.FinalPrice
			}
			if price.FinalPrice > maxPrice {
				maxPrice = price.FinalPrice
			}
		}

		// Рассчитываем изменение цены за период
		priceChange := lastPrice - firstPrice
		priceChangePercent := float64(0)
		if firstPrice > 0 {
			priceChangePercent = float64(priceChange) / float64(firstPrice) * 100
		}

		// Добавляем данные в Excel
		f.SetCellValue(sheetName, fmt.Sprintf("A%d", row), product.Name)
		f.SetCellValue(sheetName, fmt.Sprintf("B%d", row), product.VendorCode)
		f.SetCellValue(sheetName, fmt.Sprintf("C%d", row), firstPrice)
		f.SetCellValue(sheetName, fmt.Sprintf("D%d", row), lastPrice)
		f.SetCellValue(sheetName, fmt.Sprintf("E%d", row), float64(priceChange))
		f.SetCellValue(sheetName, fmt.Sprintf("F%d", row), priceChangePercent)
		f.SetCellValue(sheetName, fmt.Sprintf("G%d", row), minPrice)
		f.SetCellValue(sheetName, fmt.Sprintf("H%d", row), maxPrice)
		f.SetCellValue(sheetName, fmt.Sprintf("I%d", row), len(prices))

		row++
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
		NumFmt: 2, // Формат с двумя десятичными знаками
	})
	f.SetCellStyle(sheetName, "C2", fmt.Sprintf("H%d", row-1), numberStyle)

	warehouses, _ := db.GetAllWarehouses(ctx, b.db) // Получаем список складов для совместимости с функцией
	err = addDynamicChangesSheet(f, products, ctx, b.db, startDate, endDate, true, config, warehouses)
	if err != nil {
		log.Printf("Error adding dynamic changes sheet: %v", err)
	}

	// Сохраняем файл
	filename := fmt.Sprintf("price_report_%s_%s.xlsx",
		startDate.Format("02-01-2006"),
		endDate.Format("02-01-2006"))
	filepath := filepath.Join(os.TempDir(), filename)
	if err := f.SaveAs(filepath); err != nil {
		log.Printf("Error saving Excel file: %v", err)
		b.api.Send(tgbotapi.NewMessage(chatID, "Ошибка при создании Excel-файла."))
		return
	}

	// Отправляем файл в Telegram
	doc := tgbotapi.NewDocument(chatID, tgbotapi.FilePath(filepath))
	doc.Caption = fmt.Sprintf("📈 Отчет по ценам за период %s - %s",
		startDate.Format("02.01.2006"),
		endDate.Format("02.01.2006"))
	_, err = b.api.Send(doc)
	if err != nil {
		log.Printf("Error sending Excel file: %v", err)
		b.api.Send(tgbotapi.NewMessage(chatID, "Ошибка при отправке Excel-файла."))
	}

	// Удаляем временный файл
	os.Remove(filepath)
}

// generateStockReport генерирует отчет по остаткам в текстовом формате
func (b *Bot) generateStockReport(chatID int64, startDate, endDate time.Time) {
	ctx := context.Background()

	// Получаем все товары
	products, err := db.GetAllProducts(ctx, b.db)
	if err != nil {
		b.api.Send(tgbotapi.NewMessage(chatID, fmt.Sprintf("Ошибка при получении списка товаров: %v", err)))
		return
	}

	if len(products) == 0 {
		b.api.Send(tgbotapi.NewMessage(chatID, "Товары не найдены в базе данных."))
		return
	}

	// Получаем все склады
	warehouses, err := db.GetAllWarehouses(ctx, b.db)
	if err != nil {
		b.api.Send(tgbotapi.NewMessage(chatID, fmt.Sprintf("Ошибка при получении списка складов: %v", err)))
		return
	}

	// Формируем отчет
	reportText := fmt.Sprintf("📦 Отчет по остаткам за период %s - %s\n\n",
		startDate.Format("02.01.2006"),
		endDate.Format("02.01.2006"))

	for _, product := range products {
		totalFirstStock := 0
		totalLastStock := 0
		totalRecords := 0

		productText := fmt.Sprintf("Товар: %s (арт. %s)\n", product.Name, product.VendorCode)
		hasStocks := false

		for _, warehouse := range warehouses {
			// Получаем историю остатков для товара на конкретном складе за период
			stocks, err := db.GetStocksForPeriod(ctx, b.db, product.ID, warehouse.ID, startDate, endDate)
			if err != nil {
				log.Printf("Error getting stocks for product %d on warehouse %d: %v",
					product.ID, warehouse.ID, err)
				continue
			}

			if len(stocks) == 0 {
				continue
			}

			hasStocks = true
			totalRecords += len(stocks)

			// Первый и последний остаток за период
			firstStock := stocks[0].Amount
			lastStock := stocks[len(stocks)-1].Amount

			totalFirstStock += firstStock
			totalLastStock += lastStock

			// Находим максимальный и минимальный остаток за период
			minStock := firstStock
			maxStock := firstStock

			for _, stock := range stocks {
				if stock.Amount < minStock {
					minStock = stock.Amount
				}
				if stock.Amount > maxStock {
					maxStock = stock.Amount
				}
			}

			// Добавляем информацию о складе в отчет продукта
			productText += fmt.Sprintf("  Склад %s:\n", warehouse.Name)
			productText += fmt.Sprintf("    Начальный остаток: %d шт.\n", firstStock)
			productText += fmt.Sprintf("    Конечный остаток: %d шт.\n", lastStock)
			productText += fmt.Sprintf("    Изменение: %d шт.\n", lastStock-firstStock)
			productText += fmt.Sprintf("    Мин. остаток: %d шт., Макс. остаток: %d шт.\n", minStock, maxStock)
		}

		if hasStocks {
			// Добавляем общую информацию и данные о складах
			productText += fmt.Sprintf("  Общий начальный остаток: %d шт.\n", totalFirstStock)
			productText += fmt.Sprintf("  Общий конечный остаток: %d шт.\n", totalLastStock)
			productText += fmt.Sprintf("  Общее изменение: %d шт.\n", totalLastStock-totalFirstStock)
			productText += fmt.Sprintf("  Количество записей: %d\n\n", totalRecords)

			reportText += productText
		}
	}

	// Отправляем отчет
	if len(reportText) > 4096 {
		// Разбиваем длинный отчет на части
		for i := 0; i < len(reportText); i += 4000 {
			end := i + 4000
			if end > len(reportText) {
				end = len(reportText)
			}
			b.api.Send(tgbotapi.NewMessage(chatID, reportText[i:end]))
		}
	} else {
		b.api.Send(tgbotapi.NewMessage(chatID, reportText))
	}
}

// generateStockReportExcel генерирует отчет по остаткам в формате Excel
func (b *Bot) generateStockReportExcel(chatID int64, startDate, endDate time.Time, config ReportConfig) {
	ctx := context.Background()

	// Код остается тот же, как был раньше, до создания Excel файла
	products, err := db.GetAllProducts(ctx, b.db)
	if err != nil {
		b.api.Send(tgbotapi.NewMessage(chatID, fmt.Sprintf("Ошибка при получении списка товаров: %v", err)))
		return
	}

	if len(products) == 0 {
		b.api.Send(tgbotapi.NewMessage(chatID, "Товары не найдены в базе данных."))
		return
	}

	// Получаем все склады
	warehouses, err := db.GetAllWarehouses(ctx, b.db)
	if err != nil {
		b.api.Send(tgbotapi.NewMessage(chatID, fmt.Sprintf("Ошибка при получении списка складов: %v", err)))
		return
	}

	// Создаем новый Excel файл
	f := excelize.NewFile()
	sheetName := "Отчет по остаткам"
	f.SetSheetName("Sheet1", sheetName)

	// Устанавливаем заголовки для суммарного отчета
	headers := []string{
		"Товар", "Артикул", "Начальный остаток (шт.)", "Конечный остаток (шт.)",
		"Изменение (шт.)", "Изменение (%)", "Мин. остаток (шт.)", "Макс. остаток (шт.)", "Количество записей",
	}
	for i, header := range headers {
		cell := fmt.Sprintf("%c%d", 'A'+i, 1)
		f.SetCellValue(sheetName, cell, header)
	}

	// Устанавливаем стиль для заголовков
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

	// Создаем отдельный лист для подробной информации по складам
	detailSheetName := "Детализация по складам"
	index, err := f.NewSheet(detailSheetName)
	if err != nil {
		log.Printf("Error creating detail sheet: %v", err)
	}

	// Устанавливаем заголовки для детального отчета
	detailHeaders := []string{
		"Товар", "Артикул", "Склад", "Начальный остаток (шт.)", "Конечный остаток (шт.)",
		"Изменение (шт.)", "Изменение (%)", "Мин. остаток (шт.)", "Макс. остаток (шт.)", "Количество записей",
	}
	for i, header := range detailHeaders {
		cell := fmt.Sprintf("%c%d", 'A'+i, 1)
		f.SetCellValue(detailSheetName, cell, header)
	}
	f.SetCellStyle(detailSheetName, "A1", string(rune('A'+len(detailHeaders)-1))+"1", headerStyle)

	// Заполняем данные
	row := 2
	detailRow := 2

	for _, product := range products {
		totalFirstStock := 0
		totalLastStock := 0
		totalMinStock := 0
		totalMaxStock := 0
		totalRecords := 0
		hasStocks := false

		// Для каждого продукта собираем данные по каждому складу
		warehouseData := []struct {
			warehouseName string
			firstStock    int
			lastStock     int
			minStock      int
			maxStock      int
			records       int
		}{}

		for _, warehouse := range warehouses {
			// Получаем историю остатков для товара на конкретном складе за период
			stocks, err := db.GetStocksForPeriod(ctx, b.db, product.ID, warehouse.ID, startDate, endDate)
			if err != nil {
				log.Printf("Error getting stocks for product %d on warehouse %d: %v",
					product.ID, warehouse.ID, err)
				continue
			}

			if len(stocks) == 0 {
				continue
			}

			hasStocks = true

			// Первый и последний остаток за период
			firstStock := stocks[0].Amount
			lastStock := stocks[len(stocks)-1].Amount

			// Находим максимальный и минимальный остаток за период
			minStock := firstStock
			maxStock := firstStock

			for _, stock := range stocks {
				if stock.Amount < minStock {
					minStock = stock.Amount
				}
				if stock.Amount > maxStock {
					maxStock = stock.Amount
				}
			}

			// Суммируем для общего отчета
			totalFirstStock += firstStock
			totalLastStock += lastStock
			if totalMinStock == 0 || minStock < totalMinStock {
				totalMinStock = minStock
			}
			if maxStock > totalMaxStock {
				totalMaxStock = maxStock
			}
			totalRecords += len(stocks)

			// Добавляем данные в детальный лист
			f.SetCellValue(detailSheetName, fmt.Sprintf("A%d", detailRow), product.Name)
			f.SetCellValue(detailSheetName, fmt.Sprintf("B%d", detailRow), product.VendorCode)
			f.SetCellValue(detailSheetName, fmt.Sprintf("C%d", detailRow), warehouse.Name)
			f.SetCellValue(detailSheetName, fmt.Sprintf("D%d", detailRow), firstStock)
			f.SetCellValue(detailSheetName, fmt.Sprintf("E%d", detailRow), lastStock)
			f.SetCellValue(detailSheetName, fmt.Sprintf("F%d", detailRow), lastStock-firstStock)

			// Рассчитываем процент изменения
			changePercent := float64(0)
			if firstStock > 0 {
				changePercent = float64(lastStock-firstStock) / float64(firstStock) * 100
			}
			f.SetCellValue(detailSheetName, fmt.Sprintf("G%d", detailRow), changePercent)

			f.SetCellValue(detailSheetName, fmt.Sprintf("H%d", detailRow), minStock)
			f.SetCellValue(detailSheetName, fmt.Sprintf("I%d", detailRow), maxStock)
			f.SetCellValue(detailSheetName, fmt.Sprintf("J%d", detailRow), len(stocks))

			detailRow++

			// Сохраняем данные для сводного отчета
			warehouseData = append(warehouseData, struct {
				warehouseName string
				firstStock    int
				lastStock     int
				minStock      int
				maxStock      int
				records       int
			}{
				warehouseName: warehouse.Name,
				firstStock:    firstStock,
				lastStock:     lastStock,
				minStock:      minStock,
				maxStock:      maxStock,
				records:       len(stocks),
			})
		}

		if hasStocks {
			// Добавляем данные в суммарный лист
			f.SetCellValue(sheetName, fmt.Sprintf("A%d", row), product.Name)
			f.SetCellValue(sheetName, fmt.Sprintf("B%d", row), product.VendorCode)
			f.SetCellValue(sheetName, fmt.Sprintf("C%d", row), totalFirstStock)
			f.SetCellValue(sheetName, fmt.Sprintf("D%d", row), totalLastStock)
			f.SetCellValue(sheetName, fmt.Sprintf("E%d", row), totalLastStock-totalFirstStock)

			// Рассчитываем процент изменения
			changePercent := float64(0)
			if totalFirstStock > 0 {
				changePercent = float64(totalLastStock-totalFirstStock) / float64(totalFirstStock) * 100
			}
			f.SetCellValue(sheetName, fmt.Sprintf("F%d", row), changePercent)

			f.SetCellValue(sheetName, fmt.Sprintf("G%d", row), totalMinStock)
			f.SetCellValue(sheetName, fmt.Sprintf("H%d", row), totalMaxStock)
			f.SetCellValue(sheetName, fmt.Sprintf("I%d", row), totalRecords)

			row++
		}
	}

	// Автонастройка ширины столбцов для суммарного отчета
	for i := range headers {
		col := string(rune('A' + i))
		width, _ := f.GetColWidth(sheetName, col)
		if width < 15 {
			f.SetColWidth(sheetName, col, col, 15)
		}
	}

	// Автонастройка ширины столбцов для детального отчета
	for i := range detailHeaders {
		col := string(rune('A' + i))
		width, _ := f.GetColWidth(detailSheetName, col)
		if width < 15 {
			f.SetColWidth(detailSheetName, col, col, 15)
		}
	}

	// Устанавливаем стиль для чисел
	numberStyle, _ := f.NewStyle(&excelize.Style{
		NumFmt: 1, // Целое число
	})
	f.SetCellStyle(sheetName, "C2", fmt.Sprintf("E%d", row-1), numberStyle)
	f.SetCellStyle(sheetName, "G2", fmt.Sprintf("I%d", row-1), numberStyle)
	f.SetCellStyle(detailSheetName, "D2", fmt.Sprintf("F%d", detailRow-1), numberStyle)
	f.SetCellStyle(detailSheetName, "H2", fmt.Sprintf("J%d", detailRow-1), numberStyle)

	// Устанавливаем стиль для процентов
	percentStyle, _ := f.NewStyle(&excelize.Style{
		NumFmt: 10, // Процентный формат
	})
	f.SetCellStyle(sheetName, "F2", fmt.Sprintf("F%d", row-1), percentStyle)
	f.SetCellStyle(detailSheetName, "G2", fmt.Sprintf("G%d", detailRow-1), percentStyle)

	// Устанавливаем активный лист
	f.SetActiveSheet(index)

	// После заполнения основного отчета добавляем новый лист с динамикой изменений
	err = addDynamicChangesSheet(f, products, ctx, b.db, startDate, endDate, false, config, warehouses)
	if err != nil {
		log.Printf("Error adding dynamic changes sheet: %v", err)
	}

	// Сохраняем файл
	filename := fmt.Sprintf("stock_report_%s_%s.xlsx",
		startDate.Format("02-01-2006"),
		endDate.Format("02-01-2006"))
	filepath := filepath.Join(os.TempDir(), filename)
	if err := f.SaveAs(filepath); err != nil {
		log.Printf("Error saving Excel file: %v", err)
		b.api.Send(tgbotapi.NewMessage(chatID, "Ошибка при создании Excel-файла."))
		return
	}

	// Отправляем файл в Telegram
	doc := tgbotapi.NewDocument(chatID, tgbotapi.FilePath(filepath))
	doc.Caption = fmt.Sprintf("📦 Отчет по остаткам за период %s - %s",
		startDate.Format("02.01.2006"),
		endDate.Format("02.01.2006"))
	_, err = b.api.Send(doc)
	if err != nil {
		log.Printf("Error sending Excel file: %v", err)
		b.api.Send(tgbotapi.NewMessage(chatID, "Ошибка при отправке Excel-файла."))
	}

	// Удаляем временный файл
	os.Remove(filepath)
}

// SendDailyReport отправляет ежедневный отчет по ценам и остаткам в чат
func (b *Bot) SendDailyReport(ctx context.Context) error {
	// Определяем даты для отчета: сегодня с 00:00 до текущего момента
	now := time.Now()
	startDate := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	endDate := now

	// Отправляем сообщение о начале формирования отчета
	statusMsg, err := b.api.Send(tgbotapi.NewMessage(b.chatID, "Формирование ежедневного отчета... Пожалуйста, подождите."))
	if err != nil {
		return fmt.Errorf("error sending status message: %w", err)
	}

	// Генерируем отчет по ценам в Excel
	errPrice := b.generateDailyPriceReport(ctx, startDate, endDate)
	if errPrice != nil {
		log.Printf("Error generating daily price report: %v", errPrice)
		b.api.Send(tgbotapi.NewMessage(b.chatID, fmt.Sprintf("Ошибка при формировании отчета по ценам: %v", errPrice)))
	}

	// Генерируем отчет по остаткам в Excel
	errStock := b.generateDailyStockReport(ctx, startDate, endDate)
	if errStock != nil {
		log.Printf("Error generating daily stock report: %v", errStock)
		b.api.Send(tgbotapi.NewMessage(b.chatID, fmt.Sprintf("Ошибка при формировании отчета по остаткам: %v", errStock)))
	}

	// Удаляем статусное сообщение
	deleteMsg := tgbotapi.NewDeleteMessage(b.chatID, statusMsg.MessageID)
	b.api.Request(deleteMsg)

	// Если обе операции завершились с ошибкой, возвращаем общую ошибку
	if errPrice != nil && errStock != nil {
		return fmt.Errorf("failed to generate daily reports: price: %v, stock: %v", errPrice, errStock)
	}

	return nil
}

// generateDailyPriceReport генерирует и отправляет ежедневный отчет по ценам
func (b *Bot) generateDailyPriceReport(ctx context.Context, startDate, endDate time.Time) error {
	// Получаем все товары с изменившейся ценой за сегодня
	products, err := db.GetAllProducts(ctx, b.db)
	if err != nil {
		return fmt.Errorf("error getting products: %w", err)
	}

	if len(products) == 0 {
		b.api.Send(tgbotapi.NewMessage(b.chatID, "Товары не найдены в базе данных."))
		return nil
	}

	// Создаем новый Excel файл
	f := excelize.NewFile()
	sheetName := "Ежедневный отчет по ценам"
	f.SetSheetName("Sheet1", sheetName)

	// Устанавливаем заголовки
	headers := []string{
		"Товар", "Артикул", "Начальная цена (₽)", "Текущая цена (₽)",
		"Изменение (₽)", "Изменение (%)",
	}
	for i, header := range headers {
		cell := fmt.Sprintf("%c%d", 'A'+i, 1)
		f.SetCellValue(sheetName, cell, header)
	}

	// Устанавливаем стиль для заголовков
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

	// Заполняем данные
	row := 2
	productsAdded := 0

	for _, product := range products {
		// Получаем историю цен для товара за период
		prices, err := db.GetPricesForPeriod(ctx, b.db, product.ID, startDate, endDate)
		if err != nil {
			log.Printf("Error getting prices for product %d: %v", product.ID, err)
			continue
		}

		if len(prices) <= 1 {
			// Нет изменений цены за сегодня
			continue
		}

		// Первая и последняя цена за период
		firstPrice := prices[0].FinalPrice
		lastPrice := prices[len(prices)-1].FinalPrice

		// Если цена не изменилась, пропускаем товар
		if firstPrice == lastPrice {
			continue
		}

		// Рассчитываем изменение цены за период
		priceChange := lastPrice - firstPrice
		priceChangePercent := float64(0)
		if firstPrice > 0 {
			priceChangePercent = float64(priceChange) / float64(firstPrice) * 100
		}

		// Добавляем данные в Excel
		f.SetCellValue(sheetName, fmt.Sprintf("A%d", row), product.Name)
		f.SetCellValue(sheetName, fmt.Sprintf("B%d", row), product.VendorCode)
		f.SetCellValue(sheetName, fmt.Sprintf("C%d", row), float64(firstPrice)/100)
		f.SetCellValue(sheetName, fmt.Sprintf("D%d", row), float64(lastPrice)/100)
		f.SetCellValue(sheetName, fmt.Sprintf("E%d", row), float64(priceChange)/100)
		f.SetCellValue(sheetName, fmt.Sprintf("F%d", row), priceChangePercent)

		row++
		productsAdded++
	}

	// Если нет изменений в ценах за сегодня, отправляем уведомление и выходим
	if productsAdded == 0 {
		b.api.Send(tgbotapi.NewMessage(b.chatID, "За сегодня не было изменений в ценах товаров."))
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
		NumFmt: 2, // Формат с двумя десятичными знаками
	})
	f.SetCellStyle(sheetName, "C2", fmt.Sprintf("E%d", row-1), numberStyle)

	// Устанавливаем стиль для процентов
	percentStyle, _ := f.NewStyle(&excelize.Style{
		NumFmt: 10, // Процентный формат
	})
	f.SetCellStyle(sheetName, "F2", fmt.Sprintf("F%d", row-1), percentStyle)

	// Сохраняем файл
	filename := fmt.Sprintf("daily_price_report_%s.xlsx", startDate.Format("02-01-2006"))
	filepath := filepath.Join(os.TempDir(), filename)
	if err := f.SaveAs(filepath); err != nil {
		return fmt.Errorf("error saving Excel file: %w", err)
	}

	// Отправляем файл в Telegram
	doc := tgbotapi.NewDocument(b.chatID, tgbotapi.FilePath(filepath))
	doc.Caption = fmt.Sprintf("📈 Ежедневный отчет по изменениям цен за %s",
		startDate.Format("02.01.2006"))
	_, err = b.api.Send(doc)
	if err != nil {
		return fmt.Errorf("error sending Excel file: %w", err)
	}

	// Удаляем временный файл
	os.Remove(filepath)
	return nil
}

// generateDailyStockReport генерирует и отправляет ежедневный отчет по остаткам
func (b *Bot) generateDailyStockReport(ctx context.Context, startDate, endDate time.Time) error {
	// Получаем все товары
	products, err := db.GetAllProducts(ctx, b.db)
	if err != nil {
		return fmt.Errorf("error getting products: %w", err)
	}

	if len(products) == 0 {
		b.api.Send(tgbotapi.NewMessage(b.chatID, "Товары не найдены в базе данных."))
		return nil
	}

	// Получаем все склады
	warehouses, err := db.GetAllWarehouses(ctx, b.db)
	if err != nil {
		return fmt.Errorf("error getting warehouses: %w", err)
	}

	// Создаем новый Excel файл
	f := excelize.NewFile()
	sheetName := "Ежедневный отчет по остаткам"
	f.SetSheetName("Sheet1", sheetName)

	// Устанавливаем заголовки
	headers := []string{
		"Товар", "Артикул", "Склад", "Начальный остаток (шт.)", "Текущий остаток (шт.)",
		"Изменение (шт.)", "Изменение (%)",
	}
	for i, header := range headers {
		cell := fmt.Sprintf("%c%d", 'A'+i, 1)
		f.SetCellValue(sheetName, cell, header)
	}

	// Устанавливаем стиль для заголовков
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

	// Заполняем данные
	row := 2
	changesFound := false

	for _, product := range products {
		for _, warehouse := range warehouses {
			// Получаем историю остатков для товара на конкретном складе за период
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

	// Устанавливаем стиль для процентов
	percentStyle, _ := f.NewStyle(&excelize.Style{
		NumFmt: 10, // Процентный формат
	})
	f.SetCellStyle(sheetName, "G2", fmt.Sprintf("G%d", row-1), percentStyle)

	// Сохраняем файл
	filename := fmt.Sprintf("daily_stock_report_%s.xlsx", startDate.Format("02-01-2006"))
	filepath := filepath.Join(os.TempDir(), filename)
	if err := f.SaveAs(filepath); err != nil {
		return fmt.Errorf("error saving Excel file: %w", err)
	}

	// Отправляем файл в Telegram
	doc := tgbotapi.NewDocument(b.chatID, tgbotapi.FilePath(filepath))
	doc.Caption = fmt.Sprintf("📦 Ежедневный отчет по изменениям остатков за %s",
		startDate.Format("02.01.2006"))
	_, err = b.api.Send(doc)
	if err != nil {
		return fmt.Errorf("error sending Excel file: %w", err)
	}

	// Удаляем временный файл
	os.Remove(filepath)
	return nil
}
