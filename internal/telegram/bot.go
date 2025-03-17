package telegram

import (
	"context"
	"fmt"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/jmoiron/sqlx"
	"gopkg.in/mail.v2"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
	"wbmonitoring/monitoring/internal/telegram/report"
)

// Bot представляет Telegram-бота
type Bot struct {
	api          *tgbotapi.BotAPI
	chatID       int64
	db           *sqlx.DB
	allowedUsers map[int64]bool
	config       report.ReportConfig
	userStates   map[int64]string

	emailService   *EmailService
	pdfGenerator   *report.PDFGenerator
	excelGenerator *report.ExcelGenerator
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
		api:          api,
		chatID:       chatID,
		db:           db,
		allowedUsers: allowedUsers,
		config:       config,
		userStates:   make(map[int64]string),

		// Новые сервисы инициализируются как nil и могут быть установлены позже
		// через метод UpdateReportServices
		emailService:   nil,
		pdfGenerator:   nil,
		excelGenerator: nil,
	}

	if err := bot.Initialize(); err != nil {
		return nil, err
	}

	return bot, nil
}

// UpdateReportServices обновляет сервисы отчетов для телеграм-бота
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

// handleCustomPeriodSelection запрашивает у пользователя ввод произвольного периода
func (b *Bot) handleCustomPeriodSelection(chatID int64, reportType string) {
	// Устанавливаем состояние пользователя
	b.setUserState(chatID, "waiting_custom_period_"+reportType)

	// Отправляем сообщение с просьбой ввести период
	msg := tgbotapi.NewMessage(chatID,
		"Пожалуйста, введите период в формате ДД.ММ.ГГГГ-ДД.ММ.ГГГГ\n"+
			"Например: 01.01.2023-31.01.2023")
	b.api.Send(msg)
}

// parseCustomPeriod проверяет и парсит произвольный период
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

// Обновленный метод для отправки отчета на email
func (b *Bot) sendReportToEmail(chatID int64, userID int64, reportType, period, email string) {
	// Отправляем сообщение о начале генерации отчета
	msg := tgbotapi.NewMessage(chatID, "Генерирую отчет...")
	sentMsg, _ := b.api.Send(msg)

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

	// Генерируем отчет и получаем путь к файлу
	filePath, reportName, err := b.generateReportFile(reportType, period, "excel")
	if err != nil {
		log.Printf("Ошибка при генерации отчета: %v", err)
		b.api.Send(tgbotapi.NewEditMessageText(
			chatID, sentMsg.MessageID,
			fmt.Sprintf("Ошибка при генерации отчета: %v", err),
		))
		return
	}

	// Отправляем email, используя EmailService, если он доступен
	var sendErr error
	if b.emailService != nil {
		sendErr = b.emailService.SendReportEmail(email, reportType, displayPeriod, filePath, reportName)
	} else {
		// Обратная совместимость - используем старый метод
		sendErr = b.sendEmailLegacy(email, reportType, displayPeriod, filePath, reportName)
	}

	if sendErr != nil {
		log.Printf("Ошибка при отправке email: %v", sendErr)
		b.api.Send(tgbotapi.NewEditMessageText(
			chatID, sentMsg.MessageID,
			fmt.Sprintf("Ошибка при отправке отчета: %v", sendErr),
		))
		return
	}

	// Спрашиваем, хочет ли пользователь сохранить email
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

		b.api.Send(tgbotapi.NewEditMessageText(
			chatID, sentMsg.MessageID,
			fmt.Sprintf("Отчет успешно отправлен на %s. Сохранить этот email для будущих отчетов?", email),
		))

		msgWithKeyboard := tgbotapi.NewMessage(chatID, "Выберите действие:")
		msgWithKeyboard.ReplyMarkup = keyboard
		b.api.Send(msgWithKeyboard)
	} else {
		b.api.Send(tgbotapi.NewEditMessageText(
			chatID, sentMsg.MessageID,
			fmt.Sprintf("Отчет успешно отправлен на %s.", email),
		))
	}

	// Удаляем временный файл
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

// generateReportFile генерирует отчёт за заданный период и сохраняет его в файл,
// возвращая путь к файлу, имя отчёта и ошибку, если она произошла.
func (b *Bot) generateReportFile(reportType, period, format string) (string, string, error) {
	var startDate, endDate time.Time
	now := time.Now()

	// Расчёт начала и конца периода
	if strings.HasPrefix(period, "custom_") {
		parts := strings.Split(period, "_")
		if len(parts) != 3 {
			return "", "", fmt.Errorf("неверный формат кастомного периода: %s", period)
		}

		startDateStr, endDateStr := parts[1], parts[2]

		// Парсим даты из кастомного периода
		var err error
		startDate, err = time.ParseInLocation("02.01.2006", startDateStr, now.Location())
		if err != nil {
			return "", "", fmt.Errorf("ошибка парсинга начальной даты: %v", err)
		}

		endDate, err = time.ParseInLocation("02.01.2006", endDateStr, now.Location())
		if err != nil {
			return "", "", fmt.Errorf("ошибка парсинга конечной даты: %v", err)
		}

		// Устанавливаем конец дня для конечной даты
		endDate = time.Date(endDate.Year(), endDate.Month(), endDate.Day(), 23, 59, 59, 999999999, endDate.Location())
	} else {
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
			return "", "", fmt.Errorf("неизвестный период: %s", period)
		}
	}

	// Генерация отчёта в зависимости от типа и формата
	ctx := context.Background()

	if reportType == "prices" {
		if format == "excel" {
			// Используем новый Excel генератор если он доступен
			if b.excelGenerator != nil {
				return b.excelGenerator.GeneratePriceReportExcel(ctx, startDate, endDate)
			}
			// Иначе используем старый метод
			return b.generatePriceReportExcelToFile(startDate, endDate, b.config)
		} else if format == "pdf" {
			// Используем новый PDF генератор если он доступен
			if b.pdfGenerator != nil {
				return b.pdfGenerator.GeneratePriceReportPDF(ctx, startDate, endDate, b.config.MinPriceChangePercent)
			}
			// Иначе используем старый метод
			return b.generatePriceReportPDFToFile(startDate, endDate, b.config)
		}
	} else if reportType == "stocks" {
		if format == "excel" {
			// Используем новый Excel генератор если он доступен
			if b.excelGenerator != nil {
				return b.excelGenerator.GenerateStockReportExcel(ctx, startDate, endDate)
			}
			// Иначе используем старый метод
			return b.generateStockReportExcelToFile(startDate, endDate, b.config)
		} else if format == "pdf" {
			// Используем новый PDF генератор если он доступен
			if b.pdfGenerator != nil {
				return b.pdfGenerator.GenerateStockReportPDF(ctx, startDate, endDate, b.config.MinStockChangePercent)
			}
			// Иначе используем старый метод
			return b.generateStockReportPDFToFile(startDate, endDate, b.config)
		}
	}

	return "", "", fmt.Errorf("неизвестный тип отчёта или формат")
}

// Обновленный метод handleMessage
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
		periodCode := fmt.Sprintf("custom_%s_%s",
			startDate.Format("02.01.2006"),
			endDate.Format("02.01.2006"))

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

// Initialize initializes the bot and its dependencies
func (b *Bot) Initialize() error {
	// Инициализация таблицы для хранения email адресов
	if err := b.initializeEmailStorage(); err != nil {
		return fmt.Errorf("ошибка инициализации хранилища email: %w", err)
	}
	return nil
}

// initializeEmailStorage creates the email storage table if it doesn't exist
func (b *Bot) initializeEmailStorage() error {
	// If we have an EmailService, delegate email storage initialization to it
	if b.emailService != nil {
		log.Printf("Используется EmailService для инициализации хранилища email")
		return nil // EmailService handles its own initialization in its constructor
	}

	// Legacy initialization if EmailService is not available
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

// getUserEmail is a unified method to get user email that works with both approaches
func (b *Bot) getUserEmail(userID int64) (string, error) {
	// Try using EmailService if available
	if b.emailService != nil {
		return b.emailService.GetUserEmail(userID)
	}

	// Fall back to legacy method
	return b.getUserEmailLegacy(userID)
}
