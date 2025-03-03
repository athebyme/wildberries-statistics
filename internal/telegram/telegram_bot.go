package telegram

import (
	"context"
	"fmt"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/jmoiron/sqlx"
	"github.com/signintech/gopdf"
	"github.com/xuri/excelize/v2"
	"gopkg.in/gomail.v2"
	"log"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
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
	userStates   map[int64]string
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
		userStates:   make(map[int64]string),
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
	// Проверяем, находится ли пользователь в состоянии ожидания ввода произвольного периода
	state := b.getUserState(message.Chat.ID)
	if strings.HasPrefix(state, "waiting_custom_period_") {
		// Извлекаем тип отчета из состояния
		reportType := strings.TrimPrefix(state, "waiting_custom_period_")

		// Обрабатываем ввод периода
		startDate, endDate, err := b.parseCustomPeriod(message.Text)
		if err != nil {
			msg := tgbotapi.NewMessage(message.Chat.ID, fmt.Sprintf("Ошибка: %s\nПожалуйста, введите период в формате ДД.ММ.ГГГГ-ДД.ММ.ГГГГ", err.Error()))
			b.api.Send(msg)
			return
		}

		// Форматируем период для использования в callback data
		// Используем формат "startYYYYMMDD_endYYYYMMDD"
		periodCode := fmt.Sprintf("custom_%s_%s",
			startDate.Format("20060102"),
			endDate.Format("20060102"))

		// Очищаем состояние пользователя
		b.clearUserState(message.Chat.ID)

		// Отправляем сообщение о выбранном периоде
		confirmMsg := tgbotapi.NewMessage(message.Chat.ID,
			fmt.Sprintf("Выбран период с %s по %s",
				startDate.Format("02.01.2006"),
				endDate.Format("02.01.2006")))
		b.api.Send(confirmMsg)

		// Отправляем выбор формата для отчета
		b.sendFormatSelection(message.Chat.ID, reportType, periodCode)
	} else if strings.HasPrefix(state, "waiting_email_") {
		// Извлекаем параметры из состояния
		parts := strings.Split(state, "_")
		if len(parts) >= 3 {
			reportType := parts[2]
			period := parts[3]

			// Проверяем валидность введенного email
			if !isValidEmail(message.Text) {
				msg := tgbotapi.NewMessage(message.Chat.ID, "Пожалуйста, введите корректный адрес электронной почты.")
				b.api.Send(msg)
				return
			}

			// Отправляем отчет на введенный email
			b.sendReportToEmail(message.Chat.ID, message.From.ID, reportType, period, message.Text)
			return
		}
	} else {
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

// Обновленная функция handleCallbackQuery
func (b *Bot) handleCallbackQuery(query *tgbotapi.CallbackQuery) {
	// Отправляем уведомление о получении запроса
	callback := tgbotapi.NewCallback(query.ID, "")
	b.api.Request(callback)

	// Обработка отмены ввода
	if query.Data == "cancel_email_input" {
		b.clearUserState(query.Message.Chat.ID)
		b.api.Send(tgbotapi.NewMessage(query.Message.Chat.ID, "Отправка отчета на email отменена"))
		return
	}

	// Обработка сохранения/несохранения email
	if strings.HasPrefix(query.Data, "save_email_") {
		email := strings.TrimPrefix(query.Data, "save_email_")
		err := b.saveUserEmail(query.From.ID, email)
		if err != nil {
			log.Printf("Ошибка при сохранении email: %v", err)
			b.api.Send(tgbotapi.NewMessage(query.Message.Chat.ID, "Не удалось сохранить email. Попробуйте позже."))
			return
		}
		b.api.Send(tgbotapi.NewMessage(query.Message.Chat.ID, "Email успешно сохранен!"))
		return
	}

	if query.Data == "dont_save_email" {
		b.api.Send(tgbotapi.NewMessage(query.Message.Chat.ID, "Email не будет сохранен."))
		return
	}

	// Обработка использования сохраненного email
	if strings.HasPrefix(query.Data, "use_saved_email_") {
		parts := strings.Split(query.Data, "_")
		if len(parts) >= 3 {
			reportType := parts[3]
			period := parts[4]

			email, err := b.getUserEmail(query.From.ID)
			if err != nil || email == "" {
				b.api.Send(tgbotapi.NewMessage(query.Message.Chat.ID, "Не удалось получить сохраненный email. Введите новый адрес."))
				b.requestEmailInput(query.Message.Chat.ID, reportType, period)
				return
			}

			b.sendReportToEmail(query.Message.Chat.ID, query.From.ID, reportType, period, email)
		}
		return
	}

	// Обработка ввода нового email
	if strings.HasPrefix(query.Data, "enter_new_email_") {
		parts := strings.Split(query.Data, "_")
		if len(parts) >= 3 {
			reportType := parts[3]
			period := parts[4]
			b.requestEmailInput(query.Message.Chat.ID, reportType, period)
		}
		return
	}

	// Остальной ваш код обработки callback запросов...
	parts := strings.Split(query.Data, "_")

	// Обработка выбора типа отчета
	if len(parts) >= 2 && parts[0] == "report" {
		reportType := parts[1]

		// Если пришло только два параметра, значит это выбор периода
		if len(parts) == 2 {
			b.sendPeriodSelection(query.Message.Chat.ID, reportType)
			return
		}

		// Если пришло три параметра, это запрос отчета за период или выбор произвольного периода
		if len(parts) == 3 {
			period := parts[2]

			// Если выбран произвольный период, запрашиваем его ввод
			if period == "custom" {
				b.handleCustomPeriodSelection(query.Message.Chat.ID, reportType)
				return
			}

			// Иначе отправляем выбор формата
			b.sendFormatSelection(query.Message.Chat.ID, reportType, period)
			return
		}

		// Если пришло четыре параметра или больше
		if len(parts) >= 4 {
			period := parts[2]
			format := parts[3]

			// Если выбрана отправка на email
			if format == "email" {
				b.handleEmailReportOption(query.Message.Chat.ID, query.From.ID, reportType, period)
				return
			}

			// Если это кастомный период с датами
			if period == "custom" && len(parts) >= 6 {
				startDate := parts[3]
				endDate := parts[4]
				format := parts[5]

				// Если формат - email
				if format == "email" {
					customPeriod := fmt.Sprintf("custom_%s_%s", startDate, endDate)
					b.handleEmailReportOption(query.Message.Chat.ID, query.From.ID, reportType, customPeriod)
					return
				}

				customPeriod := fmt.Sprintf("custom_%s_%s", startDate, endDate)
				b.generateReport(query.Message.Chat.ID, reportType, customPeriod, format)
				return
			}

			// Обычный период и формат
			b.generateReport(query.Message.Chat.ID, reportType, period, format)
			return
		}
	}
}

// getPeriodName возвращает человекочитаемое название периода
func (b *Bot) getPeriodName(period string) string {
	switch period {
	case "day":
		return "день"
	case "week":
		return "неделю"
	case "month":
		return "месяц"
	}

	// Для custom периода
	if strings.HasPrefix(period, "custom_") {
		parts := strings.Split(period, "_")
		if len(parts) >= 3 {
			startStr := parts[1]
			endStr := parts[2]

			// Преобразуем формат дат
			layout := "20060102"
			start, err1 := time.Parse(layout, startStr)
			end, err2 := time.Parse(layout, endStr)

			if err1 == nil && err2 == nil {
				return fmt.Sprintf("период с %s по %s",
					start.Format("02.01.2006"),
					end.Format("02.01.2006"))
			}
		}
	}

	return period
}

// handleCustomPeriodSelection запрашивает у пользователя ввод произвольного периода
func (b *Bot) handleCustomPeriodSelection(chatID int64, reportType string) {
	msgText := "Пожалуйста, введите даты начала и конца периода в формате:\nДД.ММ.ГГГГ-ДД.ММ.ГГГГ\n\nНапример: 01.03.2025-15.03.2025"

	msg := tgbotapi.NewMessage(chatID, msgText)

	// Создаем клавиатуру с кнопкой отмены
	keyboard := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("Отмена", "cancel_custom_period"),
		),
	)

	msg.ReplyMarkup = keyboard
	b.api.Send(msg)

	// Сохраняем в состоянии бота, что пользователь сейчас вводит период для этого типа отчета
	b.setUserState(chatID, fmt.Sprintf("waiting_custom_period_%s", reportType))
}

// parseCustomPeriod проверяет и парсит произвольный период
func (b *Bot) parseCustomPeriod(periodStr string) (startDate, endDate time.Time, err error) {
	// Разделяем строку на две даты
	dates := strings.Split(periodStr, "-")
	if len(dates) != 2 {
		return time.Time{}, time.Time{}, fmt.Errorf("неверный формат периода")
	}

	// Парсим даты
	layout := "02.01.2006"
	startDate, err = time.Parse(layout, strings.TrimSpace(dates[0]))
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("неверный формат даты начала: %v", err)
	}

	endDate, err = time.Parse(layout, strings.TrimSpace(dates[1]))
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("неверный формат даты окончания: %v", err)
	}

	// Проверяем, что конечная дата не раньше начальной
	if endDate.Before(startDate) {
		return time.Time{}, time.Time{}, fmt.Errorf("дата окончания должна быть позже даты начала")
	}

	return startDate, endDate, nil
}

// setUserState сохраняет текущее состояние пользователя
func (b *Bot) setUserState(chatID int64, state string) {
	// Предполагается, что в боте есть карта для хранения состояний пользователей
	// Если ее нет, нужно добавить в структуру Bot поле userStates
	if b.userStates == nil {
		b.userStates = make(map[int64]string)
	}
	b.userStates[chatID] = state
}

// getUserState получает текущее состояние пользователя
func (b *Bot) getUserState(chatID int64) string {
	if state, ok := b.userStates[chatID]; ok {
		return state
	}
	return ""
}

// clearUserState очищает состояние пользователя
func (b *Bot) clearUserState(chatID int64) {
	delete(b.userStates, chatID)
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
func (b *Bot) sendFormatSelection(chatID int64, reportType string, period string) {
	var msgText string
	if reportType == "prices" {
		msgText = "Выберите формат отчета по ценам:"
	} else {
		msgText = "Выберите формат отчета по остаткам:"
	}

	msg := tgbotapi.NewMessage(chatID, msgText)

	// Создаем inline клавиатуру с форматами и опцией отправки на почту
	keyboard := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("Excel", fmt.Sprintf("report_%s_%s_excel", reportType, period)),
			tgbotapi.NewInlineKeyboardButtonData("PDF", fmt.Sprintf("report_%s_%s_pdf", reportType, period)),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("Отправить на почту", fmt.Sprintf("report_%s_%s_email", reportType, period)),
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
					changePercent = float64((priceChange / prevPrice) * 100)
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
					f.SetCellValue(sheetName, fmt.Sprintf("D%d", row), price.FinalPrice)
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
		if format == "pdf" {
			b.generatePriceReportPDF(chatID, startDate, endDate, b.config)
		} else if format == "excel" {
			b.generatePriceReportExcel(chatID, startDate, endDate, b.config)
		} else {
			b.api.Send(tgbotapi.NewMessage(chatID, "Неизвестный формат отчета. Пожалуйста, выберите корректный формат."))
		}
	} else if reportType == "stocks" {
		if format == "pdf" {
			b.generateStockReportPDF(chatID, startDate, endDate, b.config)
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

// generateStockReportPDFToFile генерирует отчет по остаткам в PDF и сохраняет его в файл
func (b *Bot) generateStockReportPDFToFile(startDate, endDate time.Time, config ReportConfig) (string, string, error) {
	ctx := context.Background()

	// Получаем товары и склады
	products, err := db.GetAllProducts(ctx, b.db)
	if err != nil {
		return "", "", fmt.Errorf("ошибка при получении списка товаров: %v", err)
	}

	warehouses, err := db.GetAllWarehouses(ctx, b.db)
	if err != nil {
		return "", "", fmt.Errorf("ошибка при получении списка складов: %v", err)
	}

	if len(products) == 0 {
		return "", "", fmt.Errorf("товары не найдены в базе данных")
	}

	// Инициализируем PDF-документ
	pdf := gopdf.GoPdf{}
	pdf.Start(gopdf.Config{PageSize: *gopdf.PageSizeA4Landscape})
	pdf.AddPage()
	// Загружаем шрифты
	if err := pdf.AddTTFFont("arial", "fonts/arial.ttf"); err != nil {
		log.Printf("Ошибка загрузки шрифта: %v", err)
		return "", "", fmt.Errorf("не удалось загрузить шрифт: %v", err)
	}
	if err := pdf.AddTTFFont("arial-bold", "fonts/arialbd.ttf"); err != nil {
		log.Printf("Ошибка загрузки жирного шрифта: %v", err)
		return "", "", fmt.Errorf("не удалось загрузить жирный шрифт: %v", err)
	}

	// Добавляем заголовок отчета
	pdf.SetFont("arial-bold", "", 16)
	pdf.SetX(30)
	pdf.SetY(20)
	title := fmt.Sprintf("Отчет по складским запасам за период %s - %s",
		startDate.Format("02.01.2006"), endDate.Format("02.01.2006"))
	pdf.Cell(nil, title)
	pdf.Br(25)

	// Заголовки таблицы
	headers := []string{"Товар", "Артикул"}
	for _, wh := range warehouses {
		headers = append(headers, wh.Name)
	}
	headers = append(headers, "Всего", "Изменение")

	// Ширины колонок
	colWidths := []float64{120, 60}
	for range warehouses {
		colWidths = append(colWidths, 40)
	}
	colWidths = append(colWidths, 40, 40)

	// Рисуем заголовок таблицы
	pdf.SetFont("arial-bold", "", 10)
	pdf.SetFillColor(221, 235, 247) // Светло-голубой фон
	x := 30.0
	y := pdf.GetY()
	headerHeight := 20.0
	for i, header := range headers {
		pdf.Rectangle(x, y, x+colWidths[i], y+headerHeight, "FD", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 6)
		pdf.Cell(nil, header)
		x += colWidths[i]
	}
	pdf.SetStrokeColor(0, 0, 0) // Сброс цвета

	// Данные таблицы
	y += headerHeight
	rowHeight := 20.0
	pdf.SetFont("arial", "", 9)

	type TimeSeriesData struct {
		Product   models.ProductRecord
		TotalData []struct {
			Date     time.Time
			Quantity int
		}
		WarehouseData map[int][]models.StockRecord
	}

	timeSeriesDataList := make([]TimeSeriesData, 0)

	for _, product := range products {
		warehouseStocks := make(map[int]int)
		warehouseInitial := make(map[int]int)
		warehouseRecords := make(map[int][]models.StockRecord)
		totalStock := 0
		totalChange := 0
		totalInitialStock := 0

		// Получаем данные по остаткам для каждого склада
		for _, warehouse := range warehouses {
			stocks, err := db.GetStocksForPeriod(ctx, b.db, product.ID, warehouse.ID, startDate, endDate)
			if err != nil {
				log.Printf("Ошибка при получении остатков для товара %d, склада %d: %v", product.ID, warehouse.ID, err)
				continue
			}
			if len(stocks) == 0 {
				continue
			}

			warehouseRecords[int(warehouse.ID)] = stocks
			warehouseInitial[int(warehouse.ID)] = stocks[0].Amount
			lastStock := stocks[len(stocks)-1].Amount
			warehouseStocks[int(warehouse.ID)] = lastStock
			totalStock += lastStock
			totalChange += lastStock - stocks[0].Amount
			totalInitialStock += stocks[0].Amount
		}

		if len(warehouseStocks) == 0 {
			continue
		}

		// Опциональная фильтрация по MinStockChangePercent из ReportConfig
		if config.MinStockChangePercent > 0 && totalInitialStock > 0 {
			stockChangePercent := (float64(totalChange) / float64(totalInitialStock)) * 100
			if stockChangePercent < config.MinStockChangePercent && stockChangePercent > -config.MinStockChangePercent {
				continue // Пропускаем товар, если изменение остатков меньше порога
			}
		}

		// Проверяем, нужно ли добавить новую страницу
		if y > 500 {
			pdf.AddPage()
			y = 30.0
			pdf.SetFont("arial-bold", "", 10)
			pdf.SetFillColor(221, 235, 247)
			x = 30.0
			for i, header := range headers {
				pdf.Rectangle(x, y, x+colWidths[i], y+headerHeight, "FD", 0, 0)
				pdf.SetX(x + 2)
				pdf.SetY(y + 6)
				pdf.Cell(nil, header)
				x += colWidths[i]
			}
			pdf.SetStrokeColor(0, 0, 0)
			y += headerHeight
			pdf.SetFont("arial", "", 9)
		}

		// Рисуем строку таблицы
		x = 30.0
		name := product.Name
		if len(name) > 25 {
			name = name[:22] + "..."
		}
		pdf.Rectangle(x, y, x+colWidths[0], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 6)
		pdf.Cell(nil, name)
		x += colWidths[0]

		pdf.Rectangle(x, y, x+colWidths[1], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 6)
		pdf.Cell(nil, product.VendorCode)
		x += colWidths[1]

		for _, wh := range warehouses {
			pdf.Rectangle(x, y, x+colWidths[2], y+rowHeight, "D", 0, 0)
			pdf.SetX(x + 2)
			pdf.SetY(y + 6)
			if qty, ok := warehouseStocks[int(wh.ID)]; ok {
				pdf.Cell(nil, fmt.Sprintf("%d", qty))
			} else {
				pdf.Cell(nil, "0")
			}
			x += colWidths[2]
		}

		pdf.Rectangle(x, y, x+colWidths[len(colWidths)-2], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 6)
		pdf.Cell(nil, fmt.Sprintf("%d", totalStock))
		x += colWidths[len(colWidths)-2]

		pdf.Rectangle(x, y, x+colWidths[len(colWidths)-1], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 6)
		pdf.Cell(nil, fmt.Sprintf("%+d", totalChange))

		y += rowHeight

		// Подготавливаем данные для графиков
		if len(warehouseRecords) > 0 {
			datesMap := make(map[time.Time]bool)
			for _, records := range warehouseRecords {
				for _, r := range records {
					date := r.RecordedAt.Truncate(24 * time.Hour)
					datesMap[date] = true
				}
			}

			dates := make([]time.Time, 0, len(datesMap))
			for date := range datesMap {
				dates = append(dates, date)
			}
			sort.Slice(dates, func(i, j int) bool { return dates[i].Before(dates[j]) })

			totalData := make([]struct {
				Date     time.Time
				Quantity int
			}, len(dates))
			for i, date := range dates {
				qty := 0
				for _, records := range warehouseRecords {
					for _, r := range records {
						if r.RecordedAt.Truncate(24 * time.Hour).Equal(date) {
							qty += r.Amount
							break
						}
					}
				}
				totalData[i] = struct {
					Date     time.Time
					Quantity int
				}{date, qty}
			}

			timeSeriesDataList = append(timeSeriesDataList, TimeSeriesData{
				Product:       product,
				TotalData:     totalData,
				WarehouseData: warehouseRecords,
			})
		}
	}

	// Добавляем графики динамики остатков
	if len(timeSeriesDataList) > 0 {
		pdf.AddPage()
		pdf.SetFont("arial-bold", "", 14)
		pdf.SetX(30)
		pdf.SetY(20)
		pdf.Cell(nil, "Динамика изменения складских запасов")
		y = 50.0

		for _, ts := range timeSeriesDataList {
			if len(ts.TotalData) < 2 {
				continue
			}

			if y > 500 {
				pdf.AddPage()
				y = 50.0
			}

			// Заголовок графика
			pdf.SetFont("arial-bold", "", 12)
			pdf.SetX(30)
			pdf.SetY(y)
			pdf.Cell(nil, fmt.Sprintf("%s (%s)", ts.Product.Name, ts.Product.VendorCode))
			y += 20

			// Рисуем график
			graphWidth := 700.0
			graphHeight := 120.0
			marginLeft := 50.0
			xAxisLength := graphWidth - marginLeft
			yAxisLength := graphHeight - 20

			minQty, maxQty := ts.TotalData[0].Quantity, ts.TotalData[0].Quantity
			for _, d := range ts.TotalData {
				if d.Quantity < minQty {
					minQty = d.Quantity
				}
				if d.Quantity > maxQty {
					maxQty = d.Quantity
				}
			}
			yBuffer := int(float64(maxQty-minQty)*0.1) + 1
			minQty = max(0, minQty-yBuffer)
			maxQty += yBuffer

			// Оси
			pdf.SetStrokeColor(0, 0, 0)
			pdf.Line(30+marginLeft, y+yAxisLength, 30+marginLeft+xAxisLength, y+yAxisLength) // X-ось
			pdf.Line(30+marginLeft, y, 30+marginLeft, y+yAxisLength)                         // Y-ось

			// Метки на оси Y
			pdf.SetFont("arial", "", 8)
			for i := 0; i <= 5; i++ {
				tickY := y + yAxisLength - (float64(i) * yAxisLength / 5)
				value := minQty + (maxQty-minQty)*i/5
				pdf.SetX(20)
				pdf.SetY(tickY - 3)
				pdf.Cell(nil, fmt.Sprintf("%d", value))
			}

			// Метки на оси X (до 10)
			numTicks := min(len(ts.TotalData), 10)
			for i := 0; i < numTicks; i++ {
				idx := i * (len(ts.TotalData) - 1) / max(numTicks-1, 1)
				xPos := 30 + marginLeft + float64(i)*xAxisLength/float64(numTicks-1)
				pdf.SetX(xPos - 10)
				pdf.SetY(y + yAxisLength + 5)
				pdf.Cell(nil, ts.TotalData[idx].Date.Format("02.01"))
			}

			// Рисуем линию графика
			pdf.SetStrokeColor(0, 0, 255)
			pdf.SetLineWidth(2)
			for i := 0; i < len(ts.TotalData)-1; i++ {
				x1 := 30 + marginLeft + float64(i)*xAxisLength/float64(len(ts.TotalData)-1)
				y1 := y + yAxisLength - (float64(ts.TotalData[i].Quantity-minQty) * yAxisLength / float64(maxQty-minQty))
				x2 := 30 + marginLeft + float64(i+1)*xAxisLength/float64(len(ts.TotalData)-1)
				y2 := y + yAxisLength - (float64(ts.TotalData[i+1].Quantity-minQty) * yAxisLength / float64(maxQty-minQty))
				pdf.Line(x1, y1, x2, y2)
			}
			pdf.SetLineWidth(1)

			y += graphHeight + 20
		}
	}

	// Сохраняем PDF в файл
	filename := fmt.Sprintf("stock_report_%s_%s.pdf", startDate.Format("02-01-2006"), endDate.Format("02-01-2006"))
	filePath := filepath.Join(os.TempDir(), filename)
	if err := pdf.WritePdf(filePath); err != nil {
		log.Printf("Ошибка при сохранении PDF: %v", err)
		return "", "", fmt.Errorf("не удалось сохранить PDF: %v", err)
	}

	return filePath, filename, nil
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

// Добавление модели для хранения email пользователя
type UserEmail struct {
	UserID    int64     `db:"user_id"`
	Email     string    `db:"email"`
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
}

// Инициализация таблицы для хранения email
func (b *Bot) initializeEmailStorage() error {
	// Создаем таблицу для хранения email адресов, если она не существует
	_, err := b.db.Exec(`
        CREATE TABLE IF NOT EXISTS user_emails (
            user_id BIGINT PRIMARY KEY,
            email TEXT NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )
    `)
	return err
}

// Функция для получения сохраненного адреса электронной почты пользователя
func (b *Bot) getUserEmail(userID int64) (string, error) {
	var email string
	err := b.db.Get(&email, "SELECT email FROM user_emails WHERE user_id = $1", userID)
	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			return "", nil // Пользователь не имеет сохраненного адреса
		}
		return "", err
	}
	return email, nil
}

// Функция для сохранения адреса электронной почты пользователя
func (b *Bot) saveUserEmail(userID int64, email string) error {
	// Сначала проверяем, есть ли уже запись для этого пользователя
	var count int
	err := b.db.Get(&count, "SELECT count(*) FROM user_emails WHERE user_id = $1", userID)
	if err != nil {
		return err
	}

	if count == 0 {
		// Если записи нет, создаем новую
		_, err = b.db.Exec(
			"INSERT INTO user_emails (user_id, email) VALUES ($1, $2)",
			userID, email)
		return err
	}

	// Если запись есть, обновляем ее
	_, err = b.db.Exec(
		"UPDATE user_emails SET email = $1, updated_at = NOW() WHERE user_id = $2",
		email, userID)
	return err
}

// Функция для проверки валидности email
func isValidEmail(email string) bool {
	// Регулярка для проверки формата email
	emailRegex := regexp.MustCompile(`^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$`)
	return emailRegex.MatchString(email)
}

// Обработка выбора отправки отчета на почту
func (b *Bot) handleEmailReportOption(chatID int64, userID int64, reportType string, period string) {
	// Проверяем, есть ли у пользователя сохраненный email
	email, err := b.getUserEmail(userID)
	if err != nil {
		log.Printf("Ошибка при получении email пользователя %d: %v", userID, err)
		b.api.Send(tgbotapi.NewMessage(chatID, "Произошла ошибка при получении данных. Попробуйте позже."))
		return
	}

	if email != "" {
		// Если email уже сохранен, предлагаем использовать его или ввести новый
		msg := tgbotapi.NewMessage(chatID, fmt.Sprintf("У вас уже есть сохраненный адрес: %s\nХотите использовать его или ввести новый?", email))

		keyboard := tgbotapi.NewInlineKeyboardMarkup(
			tgbotapi.NewInlineKeyboardRow(
				tgbotapi.NewInlineKeyboardButtonData("Использовать сохраненный",
					fmt.Sprintf("use_saved_email_%s_%s", reportType, period)),
				tgbotapi.NewInlineKeyboardButtonData("Ввести новый",
					fmt.Sprintf("enter_new_email_%s_%s", reportType, period)),
			),
		)

		msg.ReplyMarkup = keyboard
		b.api.Send(msg)
	} else {
		// Если email не сохранен, запрашиваем его ввод
		b.requestEmailInput(chatID, reportType, period)
	}
}

// Запрос на ввод email
func (b *Bot) requestEmailInput(chatID int64, reportType string, period string) {
	msgText := "Пожалуйста, введите адрес электронной почты для отправки отчета:"

	msg := tgbotapi.NewMessage(chatID, msgText)

	// Создаем клавиатуру с кнопкой отмены
	keyboard := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("Отмена", "cancel_email_input"),
		),
	)

	msg.ReplyMarkup = keyboard
	b.api.Send(msg)

	// Сохраняем в состоянии бота, что пользователь сейчас вводит email для этого отчета
	b.setUserState(chatID, fmt.Sprintf("waiting_email_%s_%s", reportType, period))
}

func (b *Bot) sendReportToEmail(chatID int64, userID int64, reportType string, period string, email string) {
	b.api.Send(tgbotapi.NewMessage(chatID, "Формирую отчет для отправки на почту..."))

	// Для примера здесь определяется период; можно переиспользовать общую логику расчета дат
	now := time.Now()
	var startDate, endDate time.Time
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
		b.api.Send(tgbotapi.NewMessage(chatID, "Неизвестный период."))
		return
	}

	// Генерируем отчет в формате Excel и получаем путь к файлу
	filePath, reportName, err := b.generatePriceReportExcelToFile(startDate, endDate, b.config)
	if err != nil {
		log.Printf("Ошибка при генерации отчета: %v", err)
		b.api.Send(tgbotapi.NewMessage(chatID, fmt.Sprintf("Ошибка при генерации отчета: %v", err)))
		return
	}

	// Отправляем email с вложением (реализация sendEmail зависит от вашего кода)
	err = b.sendEmail(email, reportType, period, filePath, reportName)
	if err != nil {
		log.Printf("Ошибка при отправке email: %v", err)
		b.api.Send(tgbotapi.NewMessage(chatID, fmt.Sprintf("Ошибка при отправке отчета на email: %v", err)))
		return
	}

	// Если email новый, спрашиваем пользователя, сохранить ли его для будущих отчетов
	state := b.getUserState(chatID)
	if strings.HasPrefix(state, "waiting_email_") {
		msg := tgbotapi.NewMessage(chatID,
			fmt.Sprintf("Отчет успешно отправлен на %s\nХотите сохранить этот адрес для будущих отчетов?", email))
		keyboard := tgbotapi.NewInlineKeyboardMarkup(
			tgbotapi.NewInlineKeyboardRow(
				tgbotapi.NewInlineKeyboardButtonData("Да", fmt.Sprintf("save_email_%s", email)),
				tgbotapi.NewInlineKeyboardButtonData("Нет", "dont_save_email"),
			),
		)
		msg.ReplyMarkup = keyboard
		b.api.Send(msg)
	} else {
		b.api.Send(tgbotapi.NewMessage(chatID, fmt.Sprintf("Отчет успешно отправлен на %s", email)))
	}

	b.clearUserState(chatID)
}

// generateReportFile генерирует отчёт за заданный период и сохраняет его в файл,
// возвращая путь к файлу, имя отчёта и ошибку, если она произошла.
func (b *Bot) generateReportFile(reportType, period, format string) (filePath, reportName string, err error) {
	var startDate, endDate time.Time
	now := time.Now()

	// Расчёт начала и конца периода
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
		return "", "", fmt.Errorf("неизвестный период")
	}

	// Генерация отчёта в зависимости от типа и формата.
	// Здесь предполагается, что функции generatePriceReportExcelToFile, generateStockReportExcelToFile,
	// generatePriceReportPDFToFile, generateStockReportPDFToFile реализованы и сохраняют отчёт в файл.
	switch reportType {
	case "prices":
		if format == "excel" {
			return b.generatePriceReportExcelToFile(startDate, endDate, b.config)
		} else if format == "pdf" {
			return b.generatePriceReportPDFToFile(startDate, endDate, b.config)
		}
	case "stocks":
		if format == "excel" {
			return b.generateStockReportExcelToFile(startDate, endDate, b.config)
		} else if format == "pdf" {
			return b.generateStockReportPDFToFile(startDate, endDate, b.config)
		}
	}

	return "", "", fmt.Errorf("неизвестный тип отчёта или формат")
}

// generatePriceReportExcelToFile генерирует отчет по ценам в формате Excel и сохраняет его во временный файл.
// Возвращает путь к файлу, имя отчета и ошибку (если возникнет).
func (b *Bot) generatePriceReportExcelToFile(startDate, endDate time.Time, config ReportConfig) (string, string, error) {
	ctx := context.Background()

	products, err := db.GetAllProducts(ctx, b.db)
	if err != nil {
		return "", "", fmt.Errorf("ошибка при получении списка товаров: %v", err)
	}

	if len(products) == 0 {
		return "", "", fmt.Errorf("товары не найдены в базе данных")
	}

	// Создаем новый Excel файл
	f := excelize.NewFile()
	sheetName := "Отчет по ценам"
	f.SetSheetName("Sheet1", sheetName)

	// Заголовки таблицы
	headers := []string{
		"Товар", "Артикул", "Начальная цена (₽)", "Конечная цена (₽)",
		"Изменение (₽)", "Изменение (%)", "Мин. цена (₽)", "Макс. цена (₽)", "Количество записей",
	}
	for i, header := range headers {
		cell := fmt.Sprintf("%c%d", 'A'+i, 1)
		f.SetCellValue(sheetName, cell, header)
	}

	// Применяем стиль для заголовков
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
	lastHeaderCol := string(rune('A' + len(headers) - 1))
	f.SetCellStyle(sheetName, "A1", lastHeaderCol+"1", headerStyle)

	// Заполнение данными
	row := 2
	for _, product := range products {
		prices, err := db.GetPricesForPeriod(ctx, b.db, product.ID, startDate, endDate)
		if err != nil {
			log.Printf("Ошибка получения цен для товара %d: %v", product.ID, err)
			continue
		}
		if len(prices) == 0 {
			continue
		}

		firstPrice := prices[0].FinalPrice
		lastPrice := prices[len(prices)-1].FinalPrice
		minPrice, maxPrice := firstPrice, firstPrice

		for _, price := range prices {
			if price.FinalPrice < minPrice {
				minPrice = price.FinalPrice
			}
			if price.FinalPrice > maxPrice {
				maxPrice = price.FinalPrice
			}
		}

		priceChange := lastPrice - firstPrice
		var priceChangePercent float64
		if firstPrice > 0 {
			priceChangePercent = float64(priceChange) / float64(firstPrice) * 100
		}

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

	// Применяем числовой формат с двумя знаками после запятой
	numberStyle, _ := f.NewStyle(&excelize.Style{
		NumFmt: 2,
	})
	f.SetCellStyle(sheetName, "C2", fmt.Sprintf("H%d", row-1), numberStyle)

	// Для совместимости можно добавить динамический лист, если требуется
	warehouses, _ := db.GetAllWarehouses(ctx, b.db)
	err = addDynamicChangesSheet(f, products, ctx, b.db, startDate, endDate, true, config, warehouses)
	if err != nil {
		log.Printf("Ошибка добавления динамического листа: %v", err)
	}

	// Сохраняем файл во временной директории
	filename := fmt.Sprintf("price_report_%s_%s.xlsx",
		startDate.Format("02-01-2006"),
		endDate.Format("02-01-2006"))
	filePath := filepath.Join(os.TempDir(), filename)
	if err := f.SaveAs(filePath); err != nil {
		return "", "", fmt.Errorf("ошибка при сохранении файла: %v", err)
	}

	return filePath, filename, nil
}

func (b *Bot) generateStockReportExcelToFile(startDate, endDate time.Time, config ReportConfig) (string, string, error) {
	ctx := context.Background()

	// Получаем все товары из базы данных
	products, err := db.GetAllProducts(ctx, b.db)
	if err != nil {
		return "", "", fmt.Errorf("ошибка при получении списка товаров: %v", err)
	}

	// Получаем все склады из базы данных
	warehouses, err := db.GetAllWarehouses(ctx, b.db)
	if err != nil {
		return "", "", fmt.Errorf("ошибка при получении списка складов: %v", err)
	}

	// Создаем новый Excel файл
	f := excelize.NewFile()

	// Настраиваем лист "Сводка"
	summarySheet := "Сводка"
	f.SetSheetName("Sheet1", summarySheet)

	// Создаем лист "Детали"
	detailSheet := "Детали"
	_, err = f.NewSheet(detailSheet)
	if err != nil {
		return "", "", fmt.Errorf("ошибка при создании листа деталей: %v", err)
	}

	// Заголовки для листа "Сводка"
	summaryHeaders := []string{
		"Товар", "Артикул", "Начальные остатки", "Конечные остатки", "Изменение", "Изменение %", "Записей",
	}
	for i, header := range summaryHeaders {
		cell := fmt.Sprintf("%c1", 'A'+i)
		f.SetCellValue(summarySheet, cell, header)
	}

	// Заголовки для листа "Детали"
	detailHeaders := []string{
		"Товар", "Артикул", "Склад", "Начальные остатки", "Конечные остатки", "Изменение", "Изменение %", "Мин. остатки", "Макс. остатки", "Записей",
	}
	for i, header := range detailHeaders {
		cell := fmt.Sprintf("%c1", 'A'+i)
		f.SetCellValue(detailSheet, cell, header)
	}

	// Стиль для заголовков
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
		return "", "", fmt.Errorf("ошибка при создании стиля заголовков: %v", err)
	}

	// Применяем стиль к заголовкам
	f.SetCellStyle(summarySheet, "A1", fmt.Sprintf("%c1", 'A'+len(summaryHeaders)-1), headerStyle)
	f.SetCellStyle(detailSheet, "A1", fmt.Sprintf("%c1", 'A'+len(detailHeaders)-1), headerStyle)

	// Счетчики строк
	summaryRow := 2
	detailRow := 2

	// Обрабатываем каждый товар
	for _, product := range products {
		totalInitialStock := 0
		totalFinalStock := 0
		totalRecords := 0
		hasData := false

		for _, warehouse := range warehouses {
			// Получаем записи об остатках за период
			stocks, err := db.GetStocksForPeriod(ctx, b.db, product.ID, warehouse.ID, startDate, endDate)
			if err != nil {
				log.Printf("Ошибка получения остатков для товара %d, склада %d: %v", product.ID, warehouse.ID, err)
				continue
			}
			if len(stocks) == 0 {
				continue
			}

			hasData = true

			// Вычисляем начальные и конечные остатки
			initialStock := stocks[0].Amount
			finalStock := stocks[len(stocks)-1].Amount
			change := finalStock - initialStock

			// Вычисляем процент изменения
			var changePercent float64
			if initialStock != 0 {
				changePercent = (float64(change) / float64(initialStock)) * 100
			} else if change > 0 {
				changePercent = 100
			} else {
				changePercent = 0
			}

			// Находим минимальные и максимальные остатки
			minStock := initialStock
			maxStock := initialStock
			for _, stock := range stocks {
				if stock.Amount < minStock {
					minStock = stock.Amount
				}
				if stock.Amount > maxStock {
					maxStock = stock.Amount
				}
			}

			// Обновляем итоги для сводки
			totalInitialStock += initialStock
			totalFinalStock += finalStock
			totalRecords += len(stocks)

			// Добавляем данные в лист "Детали"
			f.SetCellValue(detailSheet, fmt.Sprintf("A%d", detailRow), product.Name)
			f.SetCellValue(detailSheet, fmt.Sprintf("B%d", detailRow), product.VendorCode)
			f.SetCellValue(detailSheet, fmt.Sprintf("C%d", detailRow), warehouse.Name)
			f.SetCellValue(detailSheet, fmt.Sprintf("D%d", detailRow), initialStock)
			f.SetCellValue(detailSheet, fmt.Sprintf("E%d", detailRow), finalStock)
			f.SetCellValue(detailSheet, fmt.Sprintf("F%d", detailRow), change)
			f.SetCellValue(detailSheet, fmt.Sprintf("G%d", detailRow), changePercent)
			f.SetCellValue(detailSheet, fmt.Sprintf("H%d", detailRow), minStock)
			f.SetCellValue(detailSheet, fmt.Sprintf("I%d", detailRow), maxStock)
			f.SetCellValue(detailSheet, fmt.Sprintf("J%d", detailRow), len(stocks))
			detailRow++
		}

		if hasData {
			// Вычисляем итоговое изменение и процент
			totalChange := totalFinalStock - totalInitialStock
			var totalChangePercent float64
			if totalInitialStock != 0 {
				totalChangePercent = (float64(totalChange) / float64(totalInitialStock)) * 100
			} else if totalChange > 0 {
				totalChangePercent = 100
			} else {
				totalChangePercent = 0
			}

			// Добавляем данные в лист "Сводка"
			f.SetCellValue(summarySheet, fmt.Sprintf("A%d", summaryRow), product.Name)
			f.SetCellValue(summarySheet, fmt.Sprintf("B%d", summaryRow), product.VendorCode)
			f.SetCellValue(summarySheet, fmt.Sprintf("C%d", summaryRow), totalInitialStock)
			f.SetCellValue(summarySheet, fmt.Sprintf("D%d", summaryRow), totalFinalStock)
			f.SetCellValue(summarySheet, fmt.Sprintf("E%d", summaryRow), totalChange)
			f.SetCellValue(summarySheet, fmt.Sprintf("F%d", summaryRow), totalChangePercent)
			f.SetCellValue(summarySheet, fmt.Sprintf("G%d", summaryRow), totalRecords)
			summaryRow++
		}
	}

	// Стили для чисел и процентов
	numberStyle, err := f.NewStyle(&excelize.Style{NumFmt: 0})
	if err != nil {
		return "", "", fmt.Errorf("ошибка при создании стиля чисел: %v", err)
	}
	percentStyle, err := f.NewStyle(&excelize.Style{NumFmt: 10})
	if err != nil {
		return "", "", fmt.Errorf("ошибка при создании стиля процентов: %v", err)
	}

	// Применяем стили к листу "Сводка"
	f.SetCellStyle(summarySheet, "C2", fmt.Sprintf("E%d", summaryRow-1), numberStyle)
	f.SetCellStyle(summarySheet, "G2", fmt.Sprintf("G%d", summaryRow-1), numberStyle)
	f.SetCellStyle(summarySheet, "F2", fmt.Sprintf("F%d", summaryRow-1), percentStyle)

	// Применяем стили к листу "Детали"
	f.SetCellStyle(detailSheet, "D2", fmt.Sprintf("F%d", detailRow-1), numberStyle)
	f.SetCellValue(detailSheet, "H2", fmt.Sprintf("J%d", detailRow-1))
	f.SetCellStyle(detailSheet, "G2", fmt.Sprintf("G%d", detailRow-1), percentStyle)

	err = addDynamicChangesSheet(f, products, ctx, b.db, startDate, endDate, false, config, warehouses)
	if err != nil {
		log.Printf("Ошибка при добавлении листа с динамикой: %v", err)
	}

	// Сохраняем файл
	filename := fmt.Sprintf("stock_report_%s_%s.xlsx", startDate.Format("02-01-2006"), endDate.Format("02-01-2006"))
	filePath := filepath.Join(os.TempDir(), filename)
	if err := f.SaveAs(filePath); err != nil {
		return "", "", fmt.Errorf("ошибка при сохранении Excel файла: %v", err)
	}

	return filePath, filename, nil
}

func (b *Bot) generatePriceReportPDFToFile(startDate, endDate time.Time, config ReportConfig) (string, string, error) {
	ctx := context.Background()

	// Получаем список всех товаров из базы данных
	products, err := db.GetAllProducts(ctx, b.db)
	if err != nil {
		return "", "", fmt.Errorf("ошибка при получении списка товаров: %v", err)
	}

	if len(products) == 0 {
		return "", "", fmt.Errorf("товары не найдены в базе данных")
	}

	// Создаем новый PDF-документ
	pdf := gopdf.GoPdf{}
	pdf.Start(gopdf.Config{PageSize: *gopdf.PageSizeA4})
	pdf.AddPage()

	// Загружаем шрифты (предполагается, что файлы шрифтов доступны)
	err = pdf.AddTTFFont("arial", "fonts/arial.ttf")
	if err != nil {
		return "", "", fmt.Errorf("ошибка загрузки шрифта arial: %v", err)
	}
	err = pdf.AddTTFFont("arial-bold", "fonts/arialbd.ttf")
	if err != nil {
		return "", "", fmt.Errorf("ошибка загрузки шрифта arial-bold: %v", err)
	}

	// Заголовок отчета
	pdf.SetFont("arial-bold", "", 16)
	title := fmt.Sprintf("Отчет по ценам за период %s - %s", startDate.Format("02.01.2006"), endDate.Format("02.01.2006"))
	pdf.SetX(30)
	pdf.Cell(nil, title)
	pdf.Br(20)

	// Заголовки таблицы
	headers := []string{
		"Товар", "Артикул", "Начальная цена (₽)", "Конечная цена (₽)",
		"Изменение (₽)", "Изменение (%)", "Мин. цена (₽)", "Макс. цена (₽)", "Кол-во записей",
	}
	colWidths := []float64{120, 50, 50, 50, 50, 50, 50, 50, 60}
	headerHeight := 30.0
	rowHeight := 25.0

	// Рисуем шапку таблицы
	pdf.SetFont("arial-bold", "", 10)
	pdf.SetFillColor(221, 235, 247) // Светло-голубой фон
	x := 30.0
	y := pdf.GetY()

	for i, header := range headers {
		pdf.Rectangle(x, y, x+colWidths[i], y+headerHeight, "FD", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 10)
		pdf.Cell(nil, header)
		x += colWidths[i]
	}

	// Данные таблицы
	y += headerHeight
	pdf.SetFont("arial", "", 9)
	pdf.SetFillColor(255, 255, 255) // Белый фон для данных

	for _, product := range products {
		prices, err := db.GetPricesForPeriod(ctx, b.db, product.ID, startDate, endDate)
		if err != nil {
			log.Printf("Ошибка получения цен для товара %d: %v", product.ID, err)
			continue
		}
		if len(prices) == 0 {
			continue
		}

		// Проверка на конец страницы
		if y > 760 {
			pdf.AddPage()
			y = 30.0
			pdf.SetFont("arial-bold", "", 10)
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
			pdf.SetFont("arial", "", 9)
			pdf.SetFillColor(255, 255, 255)
		}

		// Расчет цен (в копейках переводим в рубли)
		firstPrice := float64(prices[0].FinalPrice) / 100
		lastPrice := float64(prices[len(prices)-1].FinalPrice) / 100
		minPrice := firstPrice
		maxPrice := firstPrice

		for _, price := range prices {
			p := float64(price.FinalPrice) / 100
			if p < minPrice {
				minPrice = p
			}
			if p > maxPrice {
				maxPrice = p
			}
		}

		priceChange := lastPrice - firstPrice
		var priceChangePercent float64
		if firstPrice > 0 {
			priceChangePercent = (priceChange / firstPrice) * 100
		}

		x = 30.0
		// Товар
		pdf.Rectangle(x, y, x+colWidths[0], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		name := product.Name
		if len(name) > 25 {
			name = name[:22] + "..."
		}
		pdf.Cell(nil, name)
		x += colWidths[0]

		// Артикул
		pdf.Rectangle(x, y, x+colWidths[1], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		pdf.Cell(nil, product.VendorCode)
		x += colWidths[1]

		// Начальная цена
		pdf.Rectangle(x, y, x+colWidths[2], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		pdf.Cell(nil, fmt.Sprintf("%.2f", firstPrice))
		x += colWidths[2]

		// Конечная цена
		pdf.Rectangle(x, y, x+colWidths[3], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		pdf.Cell(nil, fmt.Sprintf("%.2f", lastPrice))
		x += colWidths[3]

		// Изменение
		pdf.Rectangle(x, y, x+colWidths[4], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		pdf.Cell(nil, fmt.Sprintf("%.2f", priceChange))
		x += colWidths[4]

		// Изменение в процентах
		pdf.Rectangle(x, y, x+colWidths[5], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		pdf.Cell(nil, fmt.Sprintf("%.2f%%", priceChangePercent))
		x += colWidths[5]

		// Мин. цена
		pdf.Rectangle(x, y, x+colWidths[6], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		pdf.Cell(nil, fmt.Sprintf("%.2f", minPrice))
		x += colWidths[6]

		// Макс. цена
		pdf.Rectangle(x, y, x+colWidths[7], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		pdf.Cell(nil, fmt.Sprintf("%.2f", maxPrice))
		x += colWidths[7]

		// Количество записей
		pdf.Rectangle(x, y, x+colWidths[8], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		pdf.Cell(nil, fmt.Sprintf("%d", len(prices)))

		y += rowHeight
	}

	// Сохранение файла
	filename := fmt.Sprintf("price_report_%s_%s.pdf", startDate.Format("02-01-2006"), endDate.Format("02-01-2006"))
	filePath := filepath.Join(os.TempDir(), filename)
	err = pdf.WritePdf(filePath)
	if err != nil {
		return "", "", fmt.Errorf("ошибка при сохранении PDF: %v", err)
	}

	return filePath, filename, nil
}

// Функция для фактической отправки email
func (b *Bot) sendEmail(to string, reportType string, period string, filePath string, reportName string) error {
	// Настройки SMTP сервера (должны быть заданы в конфигурации)
	smtpHost := os.Getenv("SMTP_HOST")
	smtpPort := 587 // или из конфигурации
	smtpUser := os.Getenv("SMTP_USER")
	smtpPassword := os.Getenv("SMTP_PASSWORD")

	// Создаем новое сообщение
	m := gomail.NewMessage()
	m.SetHeader("From", smtpUser)
	m.SetHeader("To", to)

	// Формируем тему в зависимости от типа отчета
	var subject string
	if reportType == "prices" {
		subject = "Отчет по ценам"
	} else {
		subject = "Отчет по остаткам"
	}
	m.SetHeader("Subject", subject)

	// Добавляем текст сообщения
	m.SetBody("text/plain", fmt.Sprintf("Отчет %s за период %s", subject, period))

	// Прикрепляем файл отчета
	m.Attach(filePath, gomail.Rename(reportName))

	// Отправляем сообщение
	d := gomail.NewDialer(smtpHost, smtpPort, smtpUser, smtpPassword)
	return d.DialAndSend(m)
}

func (b *Bot) Initialize() error {
	// Инициализация таблицы для хранения email адресов
	if err := b.initializeEmailStorage(); err != nil {
		return fmt.Errorf("ошибка инициализации хранилища email: %w", err)
	}

	return nil
}

// generatePriceReportPDF генерирует отчет по ценам в формате PDF
func (b *Bot) generatePriceReportPDF(chatID int64, startDate, endDate time.Time, config ReportConfig) {
	ctx := context.Background()

	// Получаем данные товаров из БД
	products, err := db.GetAllProducts(ctx, b.db)
	if err != nil {
		b.api.Send(tgbotapi.NewMessage(chatID, fmt.Sprintf("Ошибка при получении списка товаров: %v", err)))
		return
	}

	if len(products) == 0 {
		b.api.Send(tgbotapi.NewMessage(chatID, "Товары не найдены в базе данных."))
		return
	}

	// Создаем новый PDF документ
	pdf := gopdf.GoPdf{}
	pdf.Start(gopdf.Config{PageSize: *gopdf.PageSizeA4})
	pdf.AddPage()

	// Устанавливаем шрифты
	err = pdf.AddTTFFont("arial", "fonts/arial.ttf")
	if err != nil {
		log.Printf("Error loading font: %v", err)
		b.api.Send(tgbotapi.NewMessage(chatID, "Ошибка при создании PDF-файла: не удалось загрузить шрифт"))
		return
	}
	err = pdf.AddTTFFont("arial-bold", "fonts/arialbd.ttf")
	if err != nil {
		log.Printf("Error loading bold font: %v", err)
		b.api.Send(tgbotapi.NewMessage(chatID, "Ошибка при создании PDF-файла: не удалось загрузить шрифт"))
		return
	}

	// Добавляем заголовок отчета
	pdf.SetFont("arial-bold", "", 16)
	title := fmt.Sprintf("Отчет по ценам за период %s - %s",
		startDate.Format("02.01.2006"),
		endDate.Format("02.01.2006"))
	pdf.Cell(nil, title)
	pdf.Br(20)

	// Устанавливаем заголовки таблицы
	headers := []string{
		"Товар", "Артикул", "Начальная цена (₽)", "Конечная цена (₽)",
		"Изменение (₽)", "Изменение (%)", "Мин. цена (₽)", "Макс. цена (₽)", "Кол-во записей",
	}

	// Настройка таблицы
	headerHeight := 30.0
	rowHeight := 25.0
	colWidths := []float64{120, 50, 50, 50, 50, 50, 50, 50, 60} // Ширина каждой колонки

	// Рисуем шапку таблицы
	pdf.SetFont("arial-bold", "", 10)
	pdf.SetFillColor(221, 235, 247) // Светло-голубой фон для заголовков

	x := 30.0
	y := pdf.GetY()

	// Рисуем заголовки
	for i, header := range headers {
		// Рисуем прямоугольник с заливкой
		pdf.SetFillColor(221, 235, 247)
		pdf.Rectangle(x, y, x+colWidths[i], y+headerHeight, "F", 0, 0)
		// Рисуем границы прямоугольника
		pdf.Rectangle(x, y, x+colWidths[i], y+headerHeight, "D", 0, 0)
		pdf.SetX(x + 2)  // Отступ для текста
		pdf.SetY(y + 10) // Центрирование по вертикали
		pdf.Cell(nil, header)
		x += colWidths[i]
	}

	// Задаем координаты для первой строки данных
	y += headerHeight
	pdf.SetFont("arial", "", 9)

	// Заполняем данные
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

		// Проверяем, нужно ли добавить новую страницу
		if y > 760 { // Если мы близко к концу страницы
			pdf.AddPage()
			y = 30 // Сбрасываем Y в начало новой страницы
		}

		// Добавляем строку в таблицу
		x = 30.0

		// Ячейка с названием товара
		pdf.Rectangle(x, y, x+colWidths[0], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		nameToDisplay := product.Name
		if len(nameToDisplay) > 25 { // Ограничиваем длину названия
			nameToDisplay = nameToDisplay[:22] + "..."
		}
		pdf.Cell(nil, nameToDisplay)
		x += colWidths[0]

		// Артикул
		pdf.Rectangle(x, y, x+colWidths[1], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		pdf.Cell(nil, product.VendorCode)
		x += colWidths[1]

		// Начальная цена
		pdf.Rectangle(x, y, x+colWidths[2], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		pdf.Cell(nil, fmt.Sprintf("%d", firstPrice))
		x += colWidths[2]

		// Конечная цена
		pdf.Rectangle(x, y, x+colWidths[3], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		pdf.Cell(nil, fmt.Sprintf("%d", lastPrice))
		x += colWidths[3]

		// Изменение цены
		pdf.Rectangle(x, y, x+colWidths[4], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		pdf.Cell(nil, fmt.Sprintf("%d", priceChange))
		x += colWidths[4]

		// Изменение в процентах
		pdf.Rectangle(x, y, x+colWidths[5], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		pdf.Cell(nil, fmt.Sprintf("%.2f%%", priceChangePercent))
		x += colWidths[5]

		// Минимальная цена
		pdf.Rectangle(x, y, x+colWidths[6], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		pdf.Cell(nil, fmt.Sprintf("%d", minPrice))
		x += colWidths[6]

		// Максимальная цена
		pdf.Rectangle(x, y, x+colWidths[7], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		pdf.Cell(nil, fmt.Sprintf("%d", maxPrice))
		x += colWidths[7]

		// Количество записей
		pdf.Rectangle(x, y, x+colWidths[8], y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		pdf.Cell(nil, fmt.Sprintf("%d", len(prices)))

		// Переходим к следующей строке
		y += rowHeight
	}

	// Добавляем информацию о динамических изменениях если нужно
	// Для PDF это может быть отдельная страница или секция
	//if config.ShowDynamicChanges {}
	pdf.AddPage()
	pdf.SetFont("arial-bold", "", 14)
	pdf.Cell(nil, "Динамика изменения цен")
	pdf.Br(15)

	// Упрощенный вариант - таблица с историей изменений
	pdf.SetFont("arial-bold", "", 10)
	for _, product := range products {
		prices, err := db.GetPricesForPeriod(ctx, b.db, product.ID, startDate, endDate)
		if err != nil || len(prices) == 0 {
			continue
		}

		pdf.Br(10)
		pdf.Cell(nil, product.Name)
		pdf.Br(5)

		// Заголовки
		x = 30.0
		y = pdf.GetY()
		headers := []string{"Дата", "Цена (₽)", "Изменение (₽)"}
		colWidths := []float64{100, 80, 80}

		for i, header := range headers {
			// Рисуем прямоугольник с заливкой
			pdf.SetFillColor(221, 235, 247)
			pdf.Rectangle(x, y, x+colWidths[i], y+headerHeight*0.8, "F", 0, 0)
			// Рисуем границы прямоугольника
			pdf.Rectangle(x, y, x+colWidths[i], y+headerHeight*0.8, "D", 0, 0)
			pdf.SetX(x + 2)
			pdf.SetY(y + 8)
			pdf.Cell(nil, header)
			x += colWidths[i]
		}

		// Данные
		y += headerHeight * 0.8
		pdf.SetFont("arial", "", 9)

		var prevPrice int
		for i, price := range prices {
			if y > 760 {
				pdf.AddPage()
				y = 30

				// Повторяем заголовки на новой странице
				x = 30.0
				for j, header := range headers {
					// Рисуем прямоугольник с заливкой
					pdf.SetFillColor(221, 235, 247)
					pdf.Rectangle(x, y, x+colWidths[j], y+headerHeight*0.8, "F", 0, 0)
					// Рисуем границы прямоугольника
					pdf.Rectangle(x, y, x+colWidths[j], y+headerHeight*0.8, "D", 0, 0)
					pdf.SetX(x + 2)
					pdf.SetY(y + 8)
					pdf.Cell(nil, header)
					x += colWidths[j]
				}
				y += headerHeight * 0.8
			}

			x = 30.0

			// Дата
			pdf.Rectangle(x, y, x+colWidths[0], y+rowHeight*0.8, "D", 0, 0)
			pdf.SetX(x + 2)
			pdf.SetY(y + 6)
			pdf.Cell(nil, price.RecordedAt.Format("02.01.2006 15:04"))
			x += colWidths[0]

			// Цена
			pdf.Rectangle(x, y, x+colWidths[1], y+rowHeight*0.8, "D", 0, 0)
			pdf.SetX(x + 2)
			pdf.SetY(y + 6)
			pdf.Cell(nil, fmt.Sprintf("%d", price.FinalPrice))
			x += colWidths[1]

			// Изменение
			pdf.Rectangle(x, y, x+colWidths[2], y+rowHeight*0.8, "D", 0, 0)
			pdf.SetX(x + 2)
			pdf.SetY(y + 6)

			if i > 0 {
				change := price.FinalPrice - prevPrice
				changeStr := fmt.Sprintf("%+d", change)
				pdf.Cell(nil, changeStr)
			} else {
				pdf.Cell(nil, "-")
			}

			prevPrice = price.FinalPrice
			y += rowHeight * 0.8
		}
	}

	// Сохраняем файл
	filename := fmt.Sprintf("price_report_%s_%s.pdf",
		startDate.Format("02-01-2006"),
		endDate.Format("02-01-2006"))
	filepath := filepath.Join(os.TempDir(), filename)

	err = pdf.WritePdf(filepath)
	if err != nil {
		log.Printf("Error saving PDF file: %v", err)
		b.api.Send(tgbotapi.NewMessage(chatID, "Ошибка при создании PDF-файла."))
		return
	}

	// Отправляем файл в Telegram
	doc := tgbotapi.NewDocument(chatID, tgbotapi.FilePath(filepath))
	doc.Caption = fmt.Sprintf("📊 Отчет по ценам за период %s - %s",
		startDate.Format("02.01.2006"),
		endDate.Format("02.01.2006"))
	_, err = b.api.Send(doc)
	if err != nil {
		log.Printf("Error sending PDF file: %v", err)
		b.api.Send(tgbotapi.NewMessage(chatID, "Ошибка при отправке PDF-файла."))
	}

	// Удаляем временный файл
	os.Remove(filepath)
}

// generateStockReportPDF генерирует отчет по складским запасам в формате PDF
func (b *Bot) generateStockReportPDF(chatID int64, startDate, endDate time.Time, config ReportConfig) {
	ctx := context.Background()

	// Получаем данные о товарах и складах
	products, err := db.GetAllProducts(ctx, b.db)
	if err != nil {
		b.api.Send(tgbotapi.NewMessage(chatID, fmt.Sprintf("Ошибка при получении списка товаров: %v", err)))
		return
	}

	warehouses, err := db.GetAllWarehouses(ctx, b.db)
	if err != nil {
		b.api.Send(tgbotapi.NewMessage(chatID, fmt.Sprintf("Ошибка при получении списка складов: %v", err)))
		return
	}

	if len(products) == 0 {
		b.api.Send(tgbotapi.NewMessage(chatID, "Товары не найдены в базе данных."))
		return
	}

	// Создаем новый PDF документ
	pdf := gopdf.GoPdf{}
	pdf.Start(gopdf.Config{PageSize: *gopdf.PageSizeA4Landscape}) // Для отчета по складам лучше использовать альбомную ориентацию
	pdf.AddPage()

	// Устанавливаем шрифты
	err = pdf.AddTTFFont("arial", "fonts/arial.ttf")
	if err != nil {
		log.Printf("Error loading font: %v", err)
		b.api.Send(tgbotapi.NewMessage(chatID, "Ошибка при создании PDF-файла: не удалось загрузить шрифт"))
		return
	}
	err = pdf.AddTTFFont("arial-bold", "fonts/arialbd.ttf")
	if err != nil {
		log.Printf("Error loading bold font: %v", err)
		b.api.Send(tgbotapi.NewMessage(chatID, "Ошибка при создании PDF-файла: не удалось загрузить шрифт"))
		return
	}

	// Добавляем заголовок отчета
	pdf.SetFont("arial-bold", "", 16)
	title := fmt.Sprintf("Отчет по складским запасам за период %s - %s",
		startDate.Format("02.01.2006"),
		endDate.Format("02.01.2006"))
	pdf.Cell(nil, title)
	pdf.Br(20)

	// Данные для шапки таблицы
	headers := []string{"Товар", "Артикул"}
	for _, wh := range warehouses {
		headers = append(headers, wh.Name)
	}
	headers = append(headers, "Всего", "Изменение")

	// Настройка таблицы
	headerHeight := 30.0
	rowHeight := 25.0
	// Расчет ширины колонок в зависимости от количества складов
	baseColWidth := 40.0
	nameColWidth := 120.0
	artColWidth := 60.0

	// Рисуем шапку таблицы
	pdf.SetFont("arial-bold", "", 10)
	pdf.SetFillColor(221, 235, 247) // Светло-голубой фон для заголовков

	x := 30.0
	y := pdf.GetY()

	// Рисуем заголовки
	for i, header := range headers {
		var colWidth float64
		if i == 0 {
			colWidth = nameColWidth
		} else if i == 1 {
			colWidth = artColWidth
		} else {
			colWidth = baseColWidth
		}

		// Рисуем прямоугольник с заливкой
		pdf.SetFillColor(221, 235, 247)
		pdf.Rectangle(x, y, x+colWidth, y+headerHeight, "F", 0, 0)
		// Рисуем границы прямоугольника
		pdf.Rectangle(x, y, x+colWidth, y+headerHeight, "D", 0, 0)
		pdf.SetX(x + 2)  // Отступ для текста
		pdf.SetY(y + 10) // Центрирование по вертикали
		pdf.Cell(nil, header)
		x += colWidth
	}

	// Задаем координаты для первой строки данных
	y += headerHeight
	pdf.SetFont("arial", "", 9)

	// Структура для хранения данных для визуализации
	type TimeSeriesData struct {
		Product    models.ProductRecord
		Warehouses map[int][]models.StockRecord // Ключ - ID склада, значение - записи запасов
		TotalData  []struct {
			Date     time.Time
			Quantity int
		}
	}

	// Массив для хранения данных временных рядов для визуализации
	timeSeriesDataList := make([]TimeSeriesData, 0)

	// Для каждого товара собираем данные по складам
	for _, product := range products {
		// Собираем данные о запасах по всем складам
		stockDataByWarehouse := make(map[int][]models.StockRecord)
		initialStocks := make(map[int]int)
		finalStocks := make(map[int]int)

		for _, warehouse := range warehouses {
			whId := int(warehouse.ID)
			// Получаем историю запасов для товара и конкретного склада за период
			stockData, err := db.GetStocksForPeriod(ctx, b.db, product.ID, warehouse.ID, startDate, endDate)
			if err != nil {
				log.Printf("Error getting stocks for product %d on warehouse %d: %v", product.ID, warehouse.ID, err)
				continue
			}

			if len(stockData) > 0 {
				stockDataByWarehouse[whId] = stockData

				// Устанавливаем начальные и конечные значения запасов для склада
				initialStocks[whId] = stockData[0].Amount
				finalStocks[whId] = stockData[len(stockData)-1].Amount
			} else {
				// Если нет данных, устанавливаем нули
				initialStocks[whId] = 0
				finalStocks[whId] = 0
			}
		}

		// Если нет данных ни по одному складу, пропускаем товар
		if len(stockDataByWarehouse) == 0 {
			continue
		}

		// Считаем общее количество и изменение
		var initialTotal, finalTotal int
		for _, qty := range initialStocks {
			initialTotal += qty
		}
		for _, qty := range finalStocks {
			finalTotal += qty
		}
		totalChange := finalTotal - initialTotal

		// Проверяем, нужно ли добавить новую страницу
		if y > 500 { // Для альбомной ориентации
			pdf.AddPage()
			y = 30 // Сбрасываем Y в начало новой страницы

			// Повторяем заголовки на новой странице
			x = 30.0
			pdf.SetFont("arial-bold", "", 10)
			for i, header := range headers {
				var colWidth float64
				if i == 0 {
					colWidth = nameColWidth
				} else if i == 1 {
					colWidth = artColWidth
				} else {
					colWidth = baseColWidth
				}

				// Рисуем прямоугольник с заливкой
				pdf.SetFillColor(221, 235, 247)
				pdf.Rectangle(x, y, x+colWidth, y+headerHeight, "F", 0, 0)
				// Рисуем границы прямоугольника
				pdf.Rectangle(x, y, x+colWidth, y+headerHeight, "D", 0, 0)
				pdf.SetX(x + 2)
				pdf.SetY(y + 10)
				pdf.Cell(nil, header)
				x += colWidth
			}

			y += headerHeight
			pdf.SetFont("arial", "", 9)
		}

		// Добавляем строку с данными товара
		x = 30.0

		// Название товара
		pdf.Rectangle(x, y, x+nameColWidth, y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		nameToDisplay := product.Name
		if len(nameToDisplay) > 25 {
			nameToDisplay = nameToDisplay[:22] + "..."
		}
		pdf.Cell(nil, nameToDisplay)
		x += nameColWidth

		// Артикул
		pdf.Rectangle(x, y, x+artColWidth, y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		pdf.Cell(nil, product.VendorCode)
		x += artColWidth

		// Данные по каждому складу
		for _, wh := range warehouses {
			pdf.Rectangle(x, y, x+baseColWidth, y+rowHeight, "D", 0, 0)
			pdf.SetX(x + 2)
			pdf.SetY(y + 8)

			// Получаем конечное количество для данного склада
			qty := finalStocks[int(wh.ID)]
			pdf.Cell(nil, fmt.Sprintf("%d", qty))
			x += baseColWidth
		}

		// Общее количество
		pdf.Rectangle(x, y, x+baseColWidth, y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		pdf.Cell(nil, fmt.Sprintf("%d", finalTotal))
		x += baseColWidth

		// Изменение
		pdf.Rectangle(x, y, x+baseColWidth, y+rowHeight, "D", 0, 0)
		pdf.SetX(x + 2)
		pdf.SetY(y + 8)
		changeStr := fmt.Sprintf("%+d", totalChange)
		pdf.Cell(nil, changeStr)

		// Сохраняем данные для визуализации если есть какие-либо данные
		if len(stockDataByWarehouse) > 0 {
			// Создаем данные для общего графика
			// Объединяем данные со всех складов по датам
			// Соберем все уникальные даты из всех записей
			allDates := make(map[time.Time]bool)
			for _, stockRecords := range stockDataByWarehouse {
				for _, record := range stockRecords {
					// Округляем время до дня
					dateOnly := time.Date(record.RecordedAt.Year(), record.RecordedAt.Month(), record.RecordedAt.Day(), 0, 0, 0, 0, record.RecordedAt.Location())
					allDates[dateOnly] = true
				}
			}

			// Преобразуем в срез для сортировки
			dates := make([]time.Time, 0, len(allDates))
			for date := range allDates {
				dates = append(dates, date)
			}

			// Сортируем даты
			sort.Slice(dates, func(i, j int) bool {
				return dates[i].Before(dates[j])
			})

			// Для каждой даты собираем общее количество со всех складов
			totalTimeData := make([]struct {
				Date     time.Time
				Quantity int
			}, 0, len(dates))

			for _, date := range dates {
				// Для этой даты считаем общее количество
				totalQty := 0
				for _, stockRecords := range stockDataByWarehouse {
					// Ищем запись, ближайшую к текущей дате (не позже её)
					var latestRecord *models.StockRecord
					for i := len(stockRecords) - 1; i >= 0; i-- {
						recordDate := time.Date(stockRecords[i].RecordedAt.Year(), stockRecords[i].RecordedAt.Month(), stockRecords[i].RecordedAt.Day(), 0, 0, 0, 0, stockRecords[i].RecordedAt.Location())
						if !recordDate.After(date) {
							latestRecord = &stockRecords[i]
							break
						}
					}

					if latestRecord != nil {
						totalQty += latestRecord.Amount
					} else if len(stockRecords) > 0 {
						// Если нет записи до этой даты, используем первую запись
						totalQty += stockRecords[0].Amount
					}
				}

				totalTimeData = append(totalTimeData, struct {
					Date     time.Time
					Quantity int
				}{Date: date, Quantity: totalQty})
			}

			timeSeriesDataList = append(timeSeriesDataList, TimeSeriesData{
				Product:    product,
				Warehouses: stockDataByWarehouse,
				TotalData:  totalTimeData,
			})
		}

		// Переходим к следующей строке
		y += rowHeight
	}

	// Добавляем страницы с графиками и детальной информацией
	if len(timeSeriesDataList) > 0 {
		// Добавляем страницу с графиками
		pdf.AddPage()
		pdf.SetFont("arial-bold", "", 14)
		pdf.SetY(20)
		pdf.Cell(nil, "Динамика изменения складских запасов")
		pdf.Br(20)

		// Для каждого товара рисуем график
		y = 50
		graphHeight := 120.0
		graphWidth := 700.0

		for _, tsData := range timeSeriesDataList {
			if y > 500 {
				pdf.AddPage()
				y = 50
			}

			// Заголовок графика - название товара и артикул
			pdf.SetFont("arial-bold", "", 12)
			pdf.SetY(y)
			pdf.SetX(30)
			pdf.Cell(nil, fmt.Sprintf("Товар: %s (Артикул: %s)", tsData.Product.Name, tsData.Product.VendorCode))
			y += 20

			if len(tsData.TotalData) > 1 {
				// Параметры графика
				marginLeft := 50.0
				marginBottom := 30.0
				xAxisLength := graphWidth - marginLeft
				yAxisLength := graphHeight - marginBottom

				// Находим минимальное и максимальное значение для шкалы Y
				var minQty, maxQty int
				for i, data := range tsData.TotalData {
					if i == 0 || data.Quantity < minQty {
						minQty = data.Quantity
					}
					if i == 0 || data.Quantity > maxQty {
						maxQty = data.Quantity
					}
				}

				// Добавим небольшой буфер сверху и снизу
				yBuffer := int(float64(maxQty-minQty) * 0.1)
				if yBuffer < 5 {
					yBuffer = 5
				}
				minQty = max(0, minQty-yBuffer)
				maxQty = maxQty + yBuffer

				// Рисуем оси
				pdf.SetStrokeColor(0, 0, 0) // Черный цвет для осей

				// Ось X
				pdf.Line(30+marginLeft, y+yAxisLength, 30+marginLeft+xAxisLength, y+yAxisLength)

				// Ось Y
				pdf.Line(30+marginLeft, y, 30+marginLeft, y+yAxisLength)

				// Шкала для оси Y
				pdf.SetFont("arial", "", 8)
				numYTicks := 5
				for i := 0; i <= numYTicks; i++ {
					tickY := y + yAxisLength - (float64(i) * yAxisLength / float64(numYTicks))
					tickValue := minQty + (maxQty-minQty)*i/numYTicks

					// Горизонтальная линия сетки
					pdf.SetStrokeColor(200, 200, 200) // Светло-серый для сетки
					pdf.Line(30+marginLeft, tickY, 30+marginLeft+xAxisLength, tickY)

					// Подпись значения
					pdf.SetStrokeColor(0, 0, 0) // Черный для текста
					pdf.SetX(20)
					pdf.SetY(tickY - 3)
					pdf.Cell(nil, fmt.Sprintf("%d", tickValue))
				}

				// Шкала для оси X
				numXTicks := min(len(tsData.TotalData), 10) // Не больше 10 делений
				for i := 0; i < numXTicks; i++ {
					idx := i * (len(tsData.TotalData) - 1) / (numXTicks - 1)
					if idx >= len(tsData.TotalData) {
						idx = len(tsData.TotalData) - 1
					}

					tickX := 30 + marginLeft + (float64(i) * xAxisLength / float64(numXTicks-1))

					// Вертикальная линия сетки
					pdf.SetStrokeColor(200, 200, 200) // Светло-серый для сетки
					pdf.Line(tickX, y, tickX, y+yAxisLength)

					// Подпись даты
					pdf.SetStrokeColor(0, 0, 0) // Черный для текста
					pdf.SetX(tickX - 15)
					pdf.SetY(y + yAxisLength + 5)
					pdf.Cell(nil, tsData.TotalData[idx].Date.Format("02.01"))
				}

				// Рисуем линию графика
				pdf.SetStrokeColor(0, 0, 255) // Синий цвет для линии графика
				pdf.SetLineWidth(2)

				for i := 0; i < len(tsData.TotalData)-1; i++ {
					x1 := 30 + marginLeft + (float64(i) * xAxisLength / float64(len(tsData.TotalData)-1))
					y1 := y + yAxisLength - ((float64(tsData.TotalData[i].Quantity) - float64(minQty)) * yAxisLength / float64(maxQty-minQty))

					x2 := 30 + marginLeft + (float64(i+1) * xAxisLength / float64(len(tsData.TotalData)-1))
					y2 := y + yAxisLength - ((float64(tsData.TotalData[i+1].Quantity) - float64(minQty)) * yAxisLength / float64(maxQty-minQty))

					pdf.Line(x1, y1, x2, y2)
				}

				// Рисуем точки на графике
				//for i := 0; i < len(tsData.TotalData); i++ {
				//	x := 30 + marginLeft + (float64(i) * xAxisLength / float64(len(tsData.TotalData)-1))
				//	y1 := y + yAxisLength - ((float64(tsData.TotalData[i].Quantity) - float64(minQty)) * yAxisLength / float64(maxQty-minQty))
				//
				//	pdf.SetFillColor(0, 0, 255) // Синий цвет для точек
				//	pdf.Circle(x, y1, 3, "F")
				//}

				pdf.SetLineWidth(1) // Возвращаем стандартную толщину линии

				// Легенда
				pdf.SetFont("arial-bold", "", 10)
				pdf.SetY(y + yAxisLength + 20)
				pdf.SetX(30)
				pdf.Cell(nil, "Даты: ")

				pdf.SetFont("arial", "", 8)
				for i, data := range tsData.TotalData {
					pdf.SetX(70 + float64(i*80))
					pdf.SetY(y + yAxisLength + 20)
					pdf.Cell(nil, fmt.Sprintf("%s: %d", data.Date.Format("02.01.2006"), data.Quantity))
				}

				y += graphHeight + 40 // Пространство для следующего графика
			} else {
				// Если нет достаточно данных для графика
				pdf.SetFont("arial", "", 10)
				pdf.SetY(y)
				pdf.SetX(50)
				pdf.Cell(nil, "Недостаточно данных для построения графика")
				y += 30
			}

			// Если есть данные по отдельным складам, добавляем мини-графики для каждого склада
			if len(tsData.Warehouses) > 1 {
				pdf.SetFont("arial-bold", "", 10)
				pdf.SetY(y)
				pdf.SetX(30)
				pdf.Cell(nil, "Распределение по складам:")
				y += 15

				// Отдельный мини-график по каждому складу
				miniGraphHeight := 80.0
				miniGraphWidth := 320.0
				x = 30.0

				// Счетчик для отслеживания количества графиков в строке
				graphCounter := 0

				for whID, stockRecords := range tsData.Warehouses {
					// Если больше 2 графиков в строке, переходим на новую строку
					if graphCounter >= 2 {
						graphCounter = 0
						x = 30.0
						y += miniGraphHeight + 30
					}

					// Если данных меньше 2, пропускаем
					if len(stockRecords) < 2 {
						continue
					}

					// Находим название склада
					warehouseName := fmt.Sprintf("Склад #%d", whID)
					for _, wh := range warehouses {
						if int(wh.ID) == whID {
							warehouseName = wh.Name
							break
						}
					}

					// Заголовок мини-графика
					pdf.SetFont("arial-bold", "", 8)
					pdf.SetY(y)
					pdf.SetX(x)
					pdf.Cell(nil, warehouseName)

					// Сортируем записи по времени
					sort.Slice(stockRecords, func(i, j int) bool {
						return stockRecords[i].RecordedAt.Before(stockRecords[j].RecordedAt)
					})

					// Преобразуем их в временной ряд
					timeData := make([]struct {
						Date     time.Time
						Quantity int
					}, 0, len(stockRecords))

					for _, record := range stockRecords {
						// Округляем время до дня
						dateOnly := time.Date(record.RecordedAt.Year(), record.RecordedAt.Month(), record.RecordedAt.Day(), 0, 0, 0, 0, record.RecordedAt.Location())

						// Проверяем, есть ли уже запись с такой датой
						foundIdx := -1
						for i, td := range timeData {
							if td.Date.Equal(dateOnly) {
								foundIdx = i
								break
							}
						}

						if foundIdx >= 0 {
							// Если запись с такой датой уже есть, обновляем количество
							timeData[foundIdx].Quantity = record.Amount
						} else {
							// Иначе добавляем новую запись
							timeData = append(timeData, struct {
								Date     time.Time
								Quantity int
							}{Date: dateOnly, Quantity: record.Amount})
						}
					}

					// Рисуем мини-график
					marginLeft := 30.0
					marginBottom := 20.0
					xAxisLength := miniGraphWidth - marginLeft
					yAxisLength := miniGraphHeight - marginBottom

					// Находим минимальное и максимальное значение для шкалы Y
					var minQty, maxQty int
					for i, data := range timeData {
						if i == 0 || data.Quantity < minQty {
							minQty = data.Quantity
						}
						if i == 0 || data.Quantity > maxQty {
							maxQty = data.Quantity
						}
					}

					// Добавим небольшой буфер сверху и снизу
					yBuffer := int(float64(maxQty-minQty) * 0.1)
					if yBuffer < 5 {
						yBuffer = 5
					}
					minQty = max(0, minQty-yBuffer)
					maxQty = maxQty + yBuffer

					y += 15 // Отступ от заголовка

					// Рисуем оси
					pdf.SetStrokeColor(0, 0, 0) // Черный цвет для осей

					// Ось X
					pdf.Line(x+marginLeft, y+yAxisLength, x+marginLeft+xAxisLength, y+yAxisLength)

					// Ось Y
					pdf.Line(x+marginLeft, y, x+marginLeft, y+yAxisLength)

					// Шкала для оси Y - только несколько значений
					pdf.SetFont("arial", "", 6)
					numYTicks := 3
					for i := 0; i <= numYTicks; i++ {
						tickY := y + yAxisLength - (float64(i) * yAxisLength / float64(numYTicks))
						tickValue := minQty + (maxQty-minQty)*i/numYTicks

						// Подпись значения
						pdf.SetX(x)
						pdf.SetY(tickY - 3)
						pdf.Cell(nil, fmt.Sprintf("%d", tickValue))
					}

					// Шкала для оси X - только начало и конец
					if len(timeData) > 1 {
						// Начало
						pdf.SetX(x + marginLeft - 15)
						pdf.SetY(y + yAxisLength + 5)
						pdf.Cell(nil, timeData[0].Date.Format("02.01"))

						// Конец
						pdf.SetX(x + marginLeft + xAxisLength - 15)
						pdf.SetY(y + yAxisLength + 5)
						pdf.Cell(nil, timeData[len(timeData)-1].Date.Format("02.01"))
					}

					// Рисуем линию графика
					pdf.SetStrokeColor(0, 0, 255) // Синий цвет для линии графика
					pdf.SetLineWidth(1)

					for i := 0; i < len(timeData)-1; i++ {
						x1 := x + marginLeft + (float64(i) * xAxisLength / float64(len(timeData)-1))
						y1 := y + yAxisLength - ((float64(timeData[i].Quantity) - float64(minQty)) * yAxisLength / float64(maxQty-minQty))

						x2 := x + marginLeft + (float64(i+1) * xAxisLength / float64(len(timeData)-1))
						y2 := y + yAxisLength - ((float64(timeData[i+1].Quantity) - float64(minQty)) * yAxisLength / float64(maxQty-minQty))

						pdf.Line(x1, y1, x2, y2)
					}

					// Рисуем точки на графике
					//for i := 0; i < len(timeData); i++ {
					//	xPoint := x + marginLeft + (float64(i) * xAxisLength / float64(len(timeData)-1))
					//	yPoint := y + yAxisLength - ((float64(timeData[i].Quantity) - float64(minQty)) * yAxisLength / float64(maxQty-minQty))
					//
					//	pdf.SetFillColor(0, 0, 255) // Синий цвет для точек
					//	pdf.Circle(xPoint, yPoint, 2, "F")
					//}

					// Переходим к следующему графику
					x += miniGraphWidth + 20
					graphCounter++
				}

				// После всех мини-графиков
				y += miniGraphHeight + 40
			}
		}
	}

	// Сохраняем файл
	filename := fmt.Sprintf("stock_report_%s_%s.pdf",
		startDate.Format("02-01-2006"),
		endDate.Format("02-01-2006"))
	filepath := filepath.Join(os.TempDir(), filename)

	err = pdf.WritePdf(filepath)
	if err != nil {
		log.Printf("Error saving PDF file: %v", err)
		b.api.Send(tgbotapi.NewMessage(chatID, "Ошибка при создании PDF-файла."))
		return
	}

	// Отправляем файл в Telegram
	doc := tgbotapi.NewDocument(chatID, tgbotapi.FilePath(filepath))
	doc.Caption = fmt.Sprintf("📦 Отчет по складским запасам за период %s - %s",
		startDate.Format("02.01.2006"),
		endDate.Format("02.01.2006"))
	_, err = b.api.Send(doc)
	if err != nil {
		log.Printf("Error sending PDF file: %v", err)
		b.api.Send(tgbotapi.NewMessage(chatID, "Ошибка при отправке PDF-файла."))
	}

	// Удаляем временный файл
	os.Remove(filepath)
}

// Вспомогательная функция для вычисления модуля числа
func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

// Вспомогательная функция для определения максимального значения
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Вспомогательная функция для определения минимального значения
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
