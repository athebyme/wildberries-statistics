package telegram

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/jmoiron/sqlx"
	"gopkg.in/mail.v2"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
	"wbmonitoring/monitoring/internal/progress"
	"wbmonitoring/monitoring/internal/telegram/report"
)

type Bot struct {
	api              *tgbotapi.BotAPI
	chatID           int64
	db               *sqlx.DB
	allowedUsers     map[int64]bool
	config           report.ReportConfig
	userStates       map[int64]string
	reportMessageIDs map[int64][]int

	emailService    *EmailService
	pdfGenerator    *report.PDFGenerator
	excelGenerator  *report.ExcelGenerator
	progressTracker *progress.Tracker
}

func NewBot(token string, chatID int64, db *sqlx.DB, allowedUserIDs []int64, config report.ReportConfig) (*Bot, error) {
	api, err := tgbotapi.NewBotAPI(token)
	if err != nil {
		return nil, fmt.Errorf("initializing telegram bot: %w", err)
	}

	allowedUsers := make(map[int64]bool)
	for _, userID := range allowedUserIDs {
		allowedUsers[userID] = true
	}

	bot := &Bot{
		api:              api,
		chatID:           chatID,
		db:               db,
		allowedUsers:     allowedUsers,
		config:           config,
		userStates:       make(map[int64]string),
		reportMessageIDs: make(map[int64][]int),

		emailService:   nil,
		pdfGenerator:   nil,
		excelGenerator: nil,

		progressTracker: progress.NewProgressTracker(100),
	}

	if err := bot.Initialize(); err != nil {
		return nil, err
	}

	go bot.runProgressCleanup()

	return bot, nil
}

func (b *Bot) UpdateReportServices(emailService *EmailService, pdfGenerator *report.PDFGenerator, excelGenerator *report.ExcelGenerator) error {
	b.emailService = emailService
	b.pdfGenerator = pdfGenerator
	b.excelGenerator = excelGenerator
	return nil
}

func (b *Bot) StartBot(ctx context.Context) {
	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60
	updates := b.api.GetUpdatesChan(u)

	for {
		select {
		case update := <-updates:
			if update.Message != nil {
				if !b.allowedUsers[update.Message.From.ID] {
					log.Printf("Unauthorized access attempt from user ID: %d", update.Message.From.ID)
					b.api.Send(tgbotapi.NewMessage(update.Message.Chat.ID, "Извините, у вас нет доступа к этому боту."))
					continue
				}
				b.handleMessage(update.Message)
			} else if update.CallbackQuery != nil {
				if !b.allowedUsers[update.CallbackQuery.From.ID] {
					log.Printf("Unauthorized callback query from user ID: %d", update.CallbackQuery.From.ID)
					continue
				}
				b.handleCallbackQuery(update.CallbackQuery)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (b *Bot) SendTelegramAlert(message string) error {
	msg := tgbotapi.NewMessage(b.chatID, message)
	_, err := b.api.Send(msg)
	return err
}

func (b *Bot) SendTelegramAlertWithParseMode(message, parseMode string) error {
	msg := tgbotapi.NewMessage(b.chatID, message)
	msg.ParseMode = parseMode
	_, err := b.api.Send(msg)
	return err
}

// Функция для запуска периодической очистки завершенных операций
func (b *Bot) runProgressCleanup() {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for range ticker.C {
		// Удаляем завершенные операции старше 24 часов
		b.progressTracker.CleanupCompletedOperations(24 * time.Hour)
	}
}

// Генерация уникального ID для операции
func generateOperationID() string {
	bytes := make([]byte, 8)
	if _, err := rand.Read(bytes); err != nil {
		// В случае ошибки используем timestamp
		return fmt.Sprintf("op-%d", time.Now().UnixNano())
	}

	return hex.EncodeToString(bytes)
}

// Добавляем метод для проверки статуса операции
func (b *Bot) handleStatusCommand(message *tgbotapi.Message) {
	args := strings.Fields(message.Text)

	// Если передан ID операции, показываем конкретную операцию
	if len(args) > 1 {
		operationID := args[1]
		b.sendOperationStatus(message.Chat.ID, operationID)
		return
	}

	// Иначе показываем список активных операций
	b.sendActiveOperationsList(message.Chat.ID)
}

// Отправляем статус конкретной операции
func (b *Bot) sendOperationStatus(chatID int64, operationID string) {
	op := b.progressTracker.GetOperation(operationID)
	if op == nil {
		b.api.Send(tgbotapi.NewMessage(chatID, fmt.Sprintf("Операция с ID '%s' не найдена.", operationID)))
		return
	}

	// Формируем сообщение с информацией о прогрессе
	var status string
	if op.IsComplete {
		if op.Error != "" {
			status = "завершена с ошибкой"
		} else {
			status = "завершена"
		}
	} else {
		status = "в процессе"
	}

	// Вычисляем процент выполнения
	percentComplete := 0
	if op.TotalItems > 0 {
		percentComplete = int((float64(op.ProcessedItems) / float64(op.TotalItems)) * 100)
	}

	// Создаем сообщение
	msgText := fmt.Sprintf("📊 *Статус операции*: `%s`\n\n", op.ID)
	msgText += fmt.Sprintf("*Название*: %s\n", op.Name)
	msgText += fmt.Sprintf("*Статус*: %s\n", status)
	msgText += fmt.Sprintf("*Начало*: %s\n", op.StartTime.Format("02.01.2006 15:04:05"))
	msgText += fmt.Sprintf("*Прогресс*: %d%% (%d из %d)\n", percentComplete, op.ProcessedItems, op.TotalItems)

	if op.CompletedItems > 0 || op.FailedItems > 0 {
		msgText += fmt.Sprintf("*Успешно*: %d\n", op.CompletedItems)
		msgText += fmt.Sprintf("*С ошибками*: %d\n", op.FailedItems)
	}

	if !op.IsComplete && op.EstimatedEndTime.After(time.Now()) {
		msgText += fmt.Sprintf("*Ожидаемое время завершения*: %s\n", op.EstimatedEndTime.Format("15:04:05"))
	}

	if op.Error != "" {
		msgText += fmt.Sprintf("\n⚠️ *Ошибка*: %s\n", op.Error)
	}

	// Добавляем последние сообщения (не более 5)
	if len(op.Messages) > 0 {
		msgText += "\n*Последние сообщения*:\n"

		start := 0
		if len(op.Messages) > 5 {
			start = len(op.Messages) - 5
		}

		for i := start; i < len(op.Messages); i++ {
			msg := op.Messages[i]

			// Выбираем эмодзи для уровня сообщения
			var emoji string
			switch msg.Level {
			case "error":
				emoji = "❌"
			case "warning":
				emoji = "⚠️"
			default:
				emoji = "ℹ️"
			}

			// Добавляем сообщение с временем
			msgText += fmt.Sprintf("%s `%s`: %s\n",
				emoji,
				msg.Time.Format("15:04:05"),
				msg.Message)
		}
	}

	// Отправляем сообщение
	message := tgbotapi.NewMessage(chatID, msgText)
	message.ParseMode = "Markdown"

	b.api.Send(message)
}

// Отправляем список активных операций
func (b *Bot) sendActiveOperationsList(chatID int64) {
	operations := b.progressTracker.GetAllOperations()

	if len(operations) == 0 {
		b.api.Send(tgbotapi.NewMessage(chatID, "Нет активных операций."))
		return
	}

	// Создаем список операций с клавиатурой для выбора
	var activeOps, completedOps []*progress.OperationProgress

	for _, op := range operations {
		if op.IsComplete {
			completedOps = append(completedOps, op)
		} else {
			activeOps = append(activeOps, op)
		}
	}

	msgText := "*Операции*:\n\n"

	// Сначала выводим активные операции
	if len(activeOps) > 0 {
		msgText += "*Активные операции*:\n"
		for _, op := range activeOps {
			percentComplete := 0
			if op.TotalItems > 0 {
				percentComplete = int((float64(op.ProcessedItems) / float64(op.TotalItems)) * 100)
			}

			msgText += fmt.Sprintf("• `%s` - %s: %d%% (%d из %d)\n",
				op.ID, op.Name, percentComplete, op.ProcessedItems, op.TotalItems)
		}
		msgText += "\n"
	}

	// Затем выводим последние завершенные операции (не более 5)
	if len(completedOps) > 0 {
		msgText += "*Завершенные операции*:\n"

		// Сортируем по времени завершения (от новых к старым)
		sort.Slice(completedOps, func(i, j int) bool {
			return completedOps[i].LastUpdateTime.After(completedOps[j].LastUpdateTime)
		})

		// Показываем только последние 5
		showCount := len(completedOps)
		if showCount > 5 {
			showCount = 5
		}

		for i := 0; i < showCount; i++ {
			op := completedOps[i]

			status := "✅ успешно"
			if op.Error != "" {
				status = "❌ с ошибкой"
			}

			msgText += fmt.Sprintf("• `%s` - %s: %s\n",
				op.ID, op.Name, status)
		}
	}

	msgText += "\nДля просмотра статуса операции отправьте команду `/status ID_операции`"

	message := tgbotapi.NewMessage(chatID, msgText)
	message.ParseMode = "Markdown"

	b.api.Send(message)
}

func (b *Bot) sendWelcomeMessage(chatID int64) {
	welcomeText := `Добро пожаловать в бот мониторинга Wildberries!

Бот отслеживает изменения цен и остатков товаров на Wildberries и отправляет уведомления при значительных изменениях.

Для получения отчетов используйте команду /report или нажмите на кнопки ниже.`

	msg := tgbotapi.NewMessage(chatID, welcomeText)
	msg.ReplyMarkup = b.getMainKeyboard()
	b.api.Send(msg)
}

func (b *Bot) trackReportMessage(chatID int64, messageID int) {
	if _, exists := b.reportMessageIDs[chatID]; !exists {
		b.reportMessageIDs[chatID] = make([]int, 0)
	}
	b.reportMessageIDs[chatID] = append(b.reportMessageIDs[chatID], messageID)
}

func (b *Bot) cleanupReportMessages(chatID int64) {

	messageIDs, exists := b.reportMessageIDs[chatID]
	if !exists || len(messageIDs) == 0 {
		return
	}

	for _, msgID := range messageIDs {
		deleteMsg := tgbotapi.NewDeleteMessage(chatID, msgID)
		_, err := b.api.Request(deleteMsg)
		if err != nil {
			log.Printf("Ошибка при удалении сообщения %d: %v", msgID, err)
		}
	}

	b.reportMessageIDs[chatID] = make([]int, 0)
}

func (b *Bot) sendAndTrackMessage(msg tgbotapi.MessageConfig) (tgbotapi.Message, error) {
	sentMsg, err := b.api.Send(msg)
	if err == nil {
		b.trackReportMessage(msg.ChatID, sentMsg.MessageID)
	}
	return sentMsg, err
}

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

// Обновленная функция handleCallbackQuery
func (b *Bot) handleCallbackQuery(query *tgbotapi.CallbackQuery) {
	// Отправляем уведомление о получении запроса
	callback := tgbotapi.NewCallback(query.ID, "")
	b.api.Request(callback)

	if query.Message != nil {
		b.trackReportMessage(query.Message.Chat.ID, query.Message.MessageID)
	}

	// Обработка отмены ввода
	if query.Data == "cancel_email_input" {
		b.clearUserState(query.Message.Chat.ID)
		b.api.Send(tgbotapi.NewMessage(query.Message.Chat.ID, "Отправка отчета на email отменена"))
		return
	}

	// Обработка сохранения/несохранения email
	if strings.HasPrefix(query.Data, "save_email_") {
		email := strings.TrimPrefix(query.Data, "save_email_")

		// Используем EmailService для сохранения, если он доступен
		var err error
		if b.emailService != nil {
			err = b.emailService.SaveUserEmail(query.From.ID, email)
		} else {
			// Обратная совместимость - используем старый метод
			err = b.saveUserEmailLegacy(query.From.ID, email)
		}

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

			// Получаем email через EmailService, если он доступен
			var email string
			var err error
			if b.emailService != nil {
				email, err = b.emailService.GetUserEmail(query.From.ID)
			} else {
				// Обратная совместимость - используем старый метод
				email, err = b.getUserEmailLegacy(query.From.ID)
			}

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

	// Остальная обработка callback запросов...
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

		// Если пришло четыре параметра
		if len(parts) == 4 {
			period := parts[2]
			format := parts[3]

			// Обработка email формата
			if format == "email" {
				b.handleEmailReportOption(query.Message.Chat.ID, query.From.ID, reportType, period)
				return
			}

			// Обработка обычного формата (PDF или Excel)
			b.generateReport(query.Message.Chat.ID, reportType, period, format)
			return
		}

		// Для кастомного периода
		if parts[2] == "custom" {
			// Собираем полный период в правильном формате
			if len(parts) >= 5 {
				customPeriod := fmt.Sprintf("custom_%s_%s", parts[3], parts[4])

				// Если это запрос на email
				if len(parts) >= 6 && parts[5] == "email" {
					b.handleEmailReportOption(query.Message.Chat.ID, query.From.ID, reportType, customPeriod)
					return
				}

				// Иначе генерируем отчет
				if len(parts) >= 6 {
					format := parts[5]
					b.generateReport(query.Message.Chat.ID, reportType, customPeriod, format)
					return
				}
			}
		}
	}

	// Если дошли сюда, значит не смогли обработать callback
	b.api.Send(tgbotapi.NewMessage(query.Message.Chat.ID, "Произошла ошибка при обработке запроса. Пожалуйста, начните сначала /report"))
}

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
			layout := "02.01.2006"
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

func (b *Bot) handleCustomPeriodSelection(chatID int64, reportType string) {
	// Устанавливаем состояние пользователя
	b.setUserState(chatID, "waiting_custom_period_"+reportType)

	// Отправляем сообщение с просьбой ввести период
	msg := tgbotapi.NewMessage(chatID,
		"Пожалуйста, введите период в формате ДД.ММ.ГГГГ-ДД.ММ.ГГГГ\n"+
			"Например: 01.01.2023-31.01.2023")
	sentMsg, err := b.api.Send(msg)
	if err == nil {
		b.trackReportMessage(chatID, sentMsg.MessageID)
	}
}

func (b *Bot) parseCustomPeriod(periodStr string) (startDate, endDate time.Time, err error) {
	// Разделяем строку на две даты
	dates := strings.Split(periodStr, "-")
	if len(dates) != 2 {
		return time.Time{}, time.Time{}, fmt.Errorf("неверный формат периода")
	}

	// Парсим даты
	// Используем "02.01.2006" вместо "02.01.2006"
	layout := "02.01.2006" // Изменено здесь!
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

func (b *Bot) setUserState(chatID int64, state string) {
	// Предполагается, что в боте есть карта для хранения состояний пользователей
	// Если ее нет, нужно добавить в структуру Bot поле userStates
	if b.userStates == nil {
		b.userStates = make(map[int64]string)
	}
	b.userStates[chatID] = state
}

func (b *Bot) getUserState(chatID int64) string {
	if state, ok := b.userStates[chatID]; ok {
		return state
	}
	return ""
}

func (b *Bot) clearUserState(chatID int64) {
	delete(b.userStates, chatID)
}

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
	sentMsg, err := b.api.Send(msg)
	if err == nil {
		b.trackReportMessage(chatID, sentMsg.MessageID)
	}
}

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
	sentMsg, err := b.api.Send(msg)
	if err == nil {
		b.trackReportMessage(chatID, sentMsg.MessageID)
	}
}

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

// Обработка выбора отправки отчета на почту
func (b *Bot) handleEmailReportOption(chatID int64, userID int64, reportType, period string) {
	// Проверяем, есть ли у пользователя сохраненный email
	savedEmail, err := b.getUserEmail(userID)
	if err != nil {
		log.Printf("Ошибка при получении email: %v", err)
	}

	var keyboard tgbotapi.InlineKeyboardMarkup

	if savedEmail != "" {
		// Если email сохранен, предлагаем использовать его или ввести новый
		keyboard = tgbotapi.NewInlineKeyboardMarkup(
			tgbotapi.NewInlineKeyboardRow(
				tgbotapi.NewInlineKeyboardButtonData(
					fmt.Sprintf("Использовать %s", savedEmail),
					fmt.Sprintf("use_saved_email_%s_%s", reportType, period),
				),
			),
			tgbotapi.NewInlineKeyboardRow(
				tgbotapi.NewInlineKeyboardButtonData(
					"Ввести новый email",
					fmt.Sprintf("enter_new_email_%s_%s", reportType, period),
				),
			),
		)
		msg := tgbotapi.NewMessage(chatID, "У вас уже есть сохраненный email. Хотите использовать его или ввести новый?")
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

	keyboard := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("Отмена", "cancel_email_input"),
		),
	)

	msg.ReplyMarkup = keyboard
	sentMsg, err := b.api.Send(msg)
	if err == nil {
		b.trackReportMessage(chatID, sentMsg.MessageID)
	}

	b.setUserState(chatID, fmt.Sprintf("waiting_email_%s_%s", reportType, period))
}

// Обновленный метод для отправки отчета на email
func (b *Bot) sendReportToEmail(chatID int64, userID int64, reportType, period, email string) {
	// Генерируем ID операции
	operationID := generateOperationID()

	// Определяем название отчета
	var reportName string
	if reportType == "prices" {
		reportName = "отчета по ценам"
	} else {
		reportName = "отчета по остаткам"
	}

	periodName := b.getPeriodName(period)

	// Создаем операцию в трекере прогресса
	totalSteps := 100
	b.progressTracker.StartOperation(operationID, fmt.Sprintf("Отправка %s за %s на email", reportName, periodName), totalSteps)

	// Отправляем сообщение о начале генерации отчета
	msgText := fmt.Sprintf(
		"🔄 Начата генерация %s за %s для отправки на email.\n\n"+
			"ID операции: `%s`\n\n"+
			"Вы можете отслеживать прогресс с помощью команды:\n"+
			"`/status %s`",
		reportName, periodName, operationID, operationID)

	msg := tgbotapi.NewMessage(chatID, msgText)
	msg.ParseMode = "Markdown"
	sentMsg, err := b.api.Send(msg)
	if err == nil {
		b.trackReportMessage(chatID, sentMsg.MessageID)
	}

	// Запускаем горутину для периодического обновления сообщения
	go b.updateReportProgress(chatID, sentMsg.MessageID, operationID)

	// Обновляем прогресс: начинаем обработку
	b.progressTracker.UpdateProgress(operationID, 5, 0, 0, "Определение параметров отчета")

	// Форматируем период для отображения пользователю
	displayPeriod := period
	if strings.HasPrefix(period, "custom_") {
		parts := strings.Split(period, "_")
		if len(parts) == 3 {
			startDate, _ := time.ParseInLocation("02.01.2006", parts[1], time.Local)
			endDate, _ := time.ParseInLocation("02.01.2006", parts[2], time.Local)
			displayPeriod = fmt.Sprintf("%s - %s",
				startDate.Format("02.01.2006"),
				endDate.Format("02.01.2006"))
		}
	} else {
		switch period {
		case "day":
			displayPeriod = "сегодня"
		case "week":
			displayPeriod = "за неделю"
		case "month":
			displayPeriod = "за месяц"
		}
	}

	b.progressTracker.UpdateProgress(operationID, 10, 0, 0, "Начало генерации файла отчета")

	filePath, reportFileName, err := b.generateReportFile(reportType, period, "excel", operationID)
	if err != nil {
		errorMsg := fmt.Sprintf("Ошибка при генерации отчета: %v", err)
		log.Printf(errorMsg)
		b.progressTracker.CompleteOperation(operationID, errorMsg)
		b.api.Send(tgbotapi.NewEditMessageText(
			chatID, sentMsg.MessageID,
			fmt.Sprintf("❌ %s", errorMsg),
		))
		return
	}

	b.progressTracker.UpdateProgress(operationID, 70, 0, 0, "Файл отчета создан, подготовка к отправке на email")

	var sendErr error
	b.progressTracker.UpdateProgress(operationID, 80, 0, 0,
		fmt.Sprintf("Отправка отчета на email: %s", email))

	if b.emailService != nil {
		sendErr = b.emailService.SendReportEmail(email, reportType, displayPeriod, filePath, reportFileName)
	} else {
		b.progressTracker.AddWarning(operationID, "Email сервис не инициализирован, используем legacy метод")
		sendErr = b.sendEmailLegacy(email, reportType, displayPeriod, filePath, reportFileName)
	}

	if sendErr != nil {
		errorMsg := fmt.Sprintf("Ошибка при отправке email: %v", sendErr)
		log.Printf(errorMsg)
		b.progressTracker.CompleteOperation(operationID, errorMsg)
		b.api.Send(tgbotapi.NewEditMessageText(
			chatID, sentMsg.MessageID,
			fmt.Sprintf("❌ %s", errorMsg),
		))
		return
	}

	b.progressTracker.UpdateProgress(operationID, 90, 0, 0, "Email успешно отправлен, завершение операции")

	b.cleanupReportMessages(chatID)

	var savedEmail string
	if b.emailService != nil {
		savedEmail, err = b.emailService.GetUserEmail(userID)
	} else {
		savedEmail, err = b.getUserEmailLegacy(userID)
	}

	if (err == nil || err.Error() == "sql: no rows in result set") && savedEmail != email {
		keyboard := tgbotapi.NewInlineKeyboardMarkup(
			tgbotapi.NewInlineKeyboardRow(
				tgbotapi.NewInlineKeyboardButtonData("Да", fmt.Sprintf("save_email_%s", email)),
				tgbotapi.NewInlineKeyboardButtonData("Нет", "dont_save_email"),
			),
		)

		successMsg := tgbotapi.NewMessage(
			chatID,
			fmt.Sprintf("✅ Отчет успешно отправлен на %s.\n\nХотите сохранить этот адрес электронной почты для будущих отчетов? Сохраненный адрес будет использоваться по умолчанию при следующих отправках.", email),
		)
		b.api.Send(successMsg)

		successMsg.ReplyMarkup = keyboard
		b.api.Send(successMsg)
	} else {
		successMsg := tgbotapi.NewMessage(
			chatID,
			fmt.Sprintf("✅ Отчет успешно отправлен на %s.", email),
		)
		b.api.Send(successMsg)
	}

	b.progressTracker.CompleteOperation(operationID, "")

	defer os.Remove(filePath)
}

// Устаревший метод для сохранения email (для обратной совместимости)
func (b *Bot) saveUserEmailLegacy(userID int64, email string) error {
	// Проверяем, есть ли уже запись для этого пользователя
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

// Устаревший метод для получения email (для обратной совместимости)
func (b *Bot) getUserEmailLegacy(userID int64) (string, error) {
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

// Устаревший метод для отправки email (для обратной совместимости)
func (b *Bot) sendEmailLegacy(to string, reportType string, period string, filePath string, reportName string) error {
	// Настройки SMTP сервера
	smtpHost := os.Getenv("SMTP_HOST")
	if smtpHost == "" {
		return fmt.Errorf("SMTP_HOST не установлен")
	}

	smtpPortStr := os.Getenv("SMTP_PORT")
	if smtpPortStr == "" {
		smtpPortStr = "587" // Порт по умолчанию для большинства SMTP серверов
	}

	smtpPort, err := strconv.Atoi(smtpPortStr)
	if err != nil {
		return fmt.Errorf("неверный формат SMTP_PORT: %v", err)
	}

	// Создаем новое сообщение
	m := mail.NewMessage()
	m.SetHeader("From", "noreply@athebyme-market.ru")
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
	m.Attach(filePath, mail.Rename(reportName))

	// Проверяем существование файла
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return fmt.Errorf("файл отчета не существует: %s", filePath)
	}

	// Создаем dialer с пустыми учетными данными для MailHog
	d := mail.NewDialer(smtpHost, smtpPort, "", "")

	// Отключаем SSL и StartTLS для MailHog
	d.SSL = false
	d.TLSConfig = nil
	d.StartTLSPolicy = mail.MandatoryStartTLS

	// Добавляем логирование перед отправкой
	log.Printf("Отправка email на %s через SMTP сервер %s:%d", to, smtpHost, smtpPort)

	return d.DialAndSend(m)
}

// возвращая путь к файлу, имя отчёта и ошибку, если она произошла.
func (b *Bot) generateReportFile(reportType, period, format string, operationID string) (string, string, error) {
	// Расчет дат
	startDate, endDate, err := b.calculateReportDates(period)
	if err != nil {
		return "", "", fmt.Errorf("error calculating dates: %w", err)
	}

	b.progressTracker.UpdateProgress(operationID, 20, 0, 0,
		"Подготовка генерации отчета")

	// Вызов соответствующего метода генерации в зависимости от типа отчета
	if reportType == "prices" {
		if format == "excel" {
			return b.generatePriceExcelWithProgress(startDate, endDate, operationID)
		} else if format == "pdf" {
			return b.generatePricePDFWithProgress(startDate, endDate, operationID)
		}
	} else if reportType == "stocks" {
		if format == "excel" {
			return b.generateStockExcelWithProgress(startDate, endDate, operationID)
		} else if format == "pdf" {
			return b.generateStockPDFWithProgress(startDate, endDate, operationID)
		}
	}

	return "", "", fmt.Errorf("неподдерживаемый тип отчета или формат")
}

// Генерация Excel-отчета по ценам с отслеживанием прогресса
func (b *Bot) generatePriceExcelWithProgress(startDate, endDate time.Time, operationID string) (string, string, error) {
	b.progressTracker.UpdateProgress(operationID, 25, 0, 0, "Получение данных о товарах")

	// Проверяем существование генератора
	if b.excelGenerator == nil {
		return "", "", fmt.Errorf("Excel генератор не инициализирован")
	}

	// Вызываем обычный метод генерации, который у нас уже есть
	return b.excelGenerator.GeneratePriceReportExcel(context.Background(), startDate, endDate)
}

// Генерация PDF-отчета по ценам с отслеживанием прогресса
func (b *Bot) generatePricePDFWithProgress(startDate, endDate time.Time, operationID string) (string, string, error) {
	b.progressTracker.UpdateProgress(operationID, 25, 0, 0, "Получение данных о товарах")

	// Проверяем существование генератора
	if b.pdfGenerator == nil {
		return "", "", fmt.Errorf("PDF генератор не инициализирован")
	}

	// Вызываем обычный метод генерации, который у нас уже есть
	return b.pdfGenerator.GeneratePriceReportPDF(context.Background(), startDate, endDate, b.config.MinPriceChangePercent)
}

// Аналогично для отчетов по остаткам
func (b *Bot) generateStockExcelWithProgress(startDate, endDate time.Time, operationID string) (string, string, error) {
	b.progressTracker.UpdateProgress(operationID, 25, 0, 0, "Получение данных о товарах и складах")

	// Проверяем существование генератора
	if b.excelGenerator == nil {
		return "", "", fmt.Errorf("Excel генератор не инициализирован")
	}

	// Вызываем обычный метод генерации, который у нас уже есть
	return b.excelGenerator.GenerateStockReportExcel(context.Background(), startDate, endDate)
}

func (b *Bot) generateStockPDFWithProgress(startDate, endDate time.Time, operationID string) (string, string, error) {
	b.progressTracker.UpdateProgress(operationID, 25, 0, 0, "Получение данных о товарах и складах")

	// Проверяем существование генератора
	if b.pdfGenerator == nil {
		return "", "", fmt.Errorf("PDF генератор не инициализирован")
	}

	// Вызываем обычный метод генерации, который у нас уже есть
	return b.pdfGenerator.GenerateStockReportPDF(context.Background(), startDate, endDate, b.config.MinStockChangePercent)
}

// Обновленный метод handleMessage
func (b *Bot) handleMessage(message *tgbotapi.Message) {

	state := b.getUserState(message.Chat.ID)
	if strings.HasPrefix(state, "waiting_custom_period_") ||
		strings.HasPrefix(state, "waiting_email_") {
		b.trackReportMessage(message.Chat.ID, message.MessageID)
	}

	if strings.HasPrefix(message.Text, "/status") {
		b.handleStatusCommand(message)
		return
	}

	if strings.HasPrefix(state, "waiting_custom_period_") {
		reportType := strings.TrimPrefix(state, "waiting_custom_period_")

		startDate, endDate, err := b.parseCustomPeriod(message.Text)
		if err != nil {
			msg := tgbotapi.NewMessage(message.Chat.ID, fmt.Sprintf("Ошибка: %s\nПожалуйста, введите период в формате ДД.ММ.ГГГГ-ДД.ММ.ГГГГ", err.Error()))
			sentMsg, _ := b.api.Send(msg)
			b.trackReportMessage(message.Chat.ID, sentMsg.MessageID)
			return
		}

		periodCode := fmt.Sprintf("custom_%s_%s",
			startDate.Format("02.01.2006"),
			endDate.Format("02.01.2006"))

		b.clearUserState(message.Chat.ID)

		confirmMsg := tgbotapi.NewMessage(message.Chat.ID,
			fmt.Sprintf("Выбран период с %s по %s",
				startDate.Format("02.01.2006"),
				endDate.Format("02.01.2006")))
		sentMsg, _ := b.api.Send(confirmMsg)
		b.trackReportMessage(message.Chat.ID, sentMsg.MessageID)

		b.sendFormatSelection(message.Chat.ID, reportType, periodCode)
	} else if strings.HasPrefix(state, "waiting_email_") {
		parts := strings.Split(state, "_")
		if len(parts) >= 3 {
			reportType := parts[2]
			period := parts[3]

			var isValid bool
			if b.emailService != nil {
				isValid = isValidEmail(message.Text)
			} else {
				isValid = isValidEmail(message.Text)
			}

			if !isValid {
				msg := tgbotapi.NewMessage(message.Chat.ID, "Пожалуйста, введите корректный адрес электронной почты.")
				b.api.Send(msg)
				return
			}

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
			if message.Text != "" {
				b.sendReportMenu(message.Chat.ID)
			}
		}
	}
}

func (b *Bot) Initialize() error {
	// Инициализация таблицы для хранения email адресов
	if err := b.initializeEmailStorage(); err != nil {
		return fmt.Errorf("ошибка инициализации хранилища email: %w", err)
	}
	return nil
}

func (b *Bot) initializeEmailStorage() error {

	if b.emailService != nil {
		log.Printf("Используется EmailService для инициализации хранилища email")
		return nil
	}

	log.Printf("Используется прямая инициализация хранилища email")
	_, err := b.db.Exec(`
		CREATE TABLE IF NOT EXISTS user_emails (
			user_id BIGINT PRIMARY KEY,
			email TEXT NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
		)
	`)
	if err != nil {
		return fmt.Errorf("ошибка создания таблицы user_emails: %w", err)
	}

	return nil
}

func (b *Bot) getUserEmail(userID int64) (string, error) {

	if b.emailService != nil {
		return b.emailService.GetUserEmail(userID)
	}

	return b.getUserEmailLegacy(userID)
}
