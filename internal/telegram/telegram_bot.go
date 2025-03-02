package telegram

import (
	"context"
	"fmt"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/jmoiron/sqlx"
	"log"
	"strings"
	"time"
	"wbmonitoring/monitoring/internal/db"
)

// Bot представляет Telegram бота с расширенным функционалом
type Bot struct {
	api          *tgbotapi.BotAPI
	chatID       int64
	db           *sqlx.DB
	allowedUsers map[int64]bool // Список разрешенных пользователей
}

// NewBot создает нового Telegram бота
func NewBot(token string, chatID int64, db *sqlx.DB, allowedUserIDs []int64) (*Bot, error) {
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

Для получения отчета за период нажмите соответствующую кнопку и выберите интересующий вас период времени.`

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
			b.generateReport(query.Message.Chat.ID, reportType, period)
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

// generateReport генерирует и отправляет отчет за выбранный период
func (b *Bot) generateReport(chatID int64, reportType, period string) {
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

	// Генерируем отчет в зависимости от типа
	if reportType == "prices" {
		b.generatePriceReport(chatID, startDate, endDate)
	} else if reportType == "stocks" {
		b.generateStockReport(chatID, startDate, endDate)
	} else {
		b.api.Send(tgbotapi.NewMessage(chatID, "Неизвестный тип отчета. Пожалуйста, выберите корректный тип."))
	}

	// Удаляем сообщение о генерации отчета
	deleteMsg := tgbotapi.NewDeleteMessage(chatID, statusMsg.MessageID)
	b.api.Request(deleteMsg)
}

// generatePriceReport генерирует отчет по ценам
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
		priceChangePercent := (priceChange / firstPrice) * 100

		// Добавляем информацию о товаре в отчет
		reportText += fmt.Sprintf("Товар: %s (арт. %s)\n", product.Name, product.VendorCode)
		reportText += fmt.Sprintf("Начальная цена: %.2f₽\n", firstPrice)
		reportText += fmt.Sprintf("Конечная цена: %.2f₽\n", lastPrice)
		reportText += fmt.Sprintf("Изменение: %.2f₽ (%.2f%%)\n", priceChange, priceChangePercent)
		reportText += fmt.Sprintf("Мин. цена: %.2f₽, Макс. цена: %.2f₽\n", minPrice, maxPrice)
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

// generateStockReport генерирует отчет по остаткам
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

// SendDailyReport отправляет ежедневный отчет по всем товарам
func (b *Bot) SendDailyReport(ctx context.Context) error {
	now := time.Now()
	yesterday := now.AddDate(0, 0, -1)

	// Устанавливаем даты для вчерашнего дня
	startDate := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 0, 0, 0, 0, now.Location())
	endDate := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 23, 59, 59, 0, now.Location())

	// Формируем отчет по ценам
	message := fmt.Sprintf("📊 Ежедневный отчет за %s\n\n", yesterday.Format("02.01.2006"))

	// Получаем все товары
	products, err := db.GetAllProducts(ctx, b.db)
	if err != nil {
		return fmt.Errorf("getting products for daily report: %w", err)
	}

	// Формируем краткий отчет по ценам
	priceChanges := 0
	for _, product := range products {
		prices, err := db.GetPricesForPeriod(ctx, b.db, product.ID, startDate, endDate)
		if err != nil {
			log.Printf("Error getting prices for product %d: %v", product.ID, err)
			continue
		}

		if len(prices) <= 1 {
			continue
		}

		// Если было больше одной записи о цене, значит были изменения
		priceChanges++
	}

	// Формируем краткий отчет по остаткам
	stockChanges := 0
	for _, product := range products {
		warehouses, err := db.GetAllWarehouses(ctx, b.db)
		if err != nil {
			log.Printf("Error getting warehouses: %v", err)
			continue
		}

		for _, warehouse := range warehouses {
			stocks, err := db.GetStocksForPeriod(ctx, b.db, product.ID, warehouse.ID, startDate, endDate)
			if err != nil {
				log.Printf("Error getting stocks for product %d on warehouse %d: %v",
					product.ID, warehouse.ID, err)
				continue
			}

			if len(stocks) <= 1 {
				continue
			}

			// Если было больше одной записи об остатках, значит были изменения
			stockChanges++
			break
		}
	}

	message += fmt.Sprintf("Всего товаров: %d\n", len(products))
	message += fmt.Sprintf("Товаров с изменением цен: %d\n", priceChanges)
	message += fmt.Sprintf("Товаров с изменением остатков: %d\n\n", stockChanges)

	message += "Для получения подробного отчета используйте команду /report или кнопку 'Получить отчет'."

	return b.SendTelegramAlert(message)
}

// Вспомогательные функции для работы с базой данных
