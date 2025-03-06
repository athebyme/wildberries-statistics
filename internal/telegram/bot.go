package telegram

import (
	"context"
	"fmt"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/jmoiron/sqlx"
	"log"
	"os"
	"strings"
	"time"
)

// Bot представляет Telegram-бота
type Bot struct {
	api          *tgbotapi.BotAPI
	chatID       int64
	db           *sqlx.DB
	allowedUsers map[int64]bool
	config       ReportConfig
	userStates   map[int64]string
}

type ReportConfig struct {
	MinPriceChangePercent float64 `json:"minPriceChangePercent"`
	MinStockChangePercent float64 `json:"minStockChangePercent"`
}

func NewBot(token string, chatID int64, db *sqlx.DB, allowedUserIDs []int64, config ReportConfig) (*Bot, error) {
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
	}
	if err := bot.Initialize(); err != nil {
		return nil, err
	}
	return bot, nil
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

		// Если пришло четыре параметра
		if len(parts) == 4 {
			period := parts[2]
			format := parts[3]

			// Обработка email формата
			if format == "email" {
				b.handleEmailReportOption(query.Message.Chat.ID, query.From.ID, reportType, period)
				return
			}

			// Обработка обычного формата
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

func (b *Bot) sendReportToEmail(chatID int64, userID int64, reportType, period, email string) {
	// Отправляем сообщение о начале генерации отчета
	msg := tgbotapi.NewMessage(chatID, "Генерирую отчет...")
	sentMsg, _ := b.api.Send(msg)

	// Форматируем период для отображения пользователю
	displayPeriod := period
	if strings.HasPrefix(period, "custom_") {
		parts := strings.Split(period, "_")
		if len(parts) == 3 {
			startDate, _ := time.ParseInLocation("20060102", parts[1], time.Local)
			endDate, _ := time.ParseInLocation("20060102", parts[2], time.Local)
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

	// Генерируем отчет (предполагаем Excel формат для email)
	filePath, reportName, err := b.generateReportFile(reportType, period, "excel")
	if err != nil {
		log.Printf("Ошибка при генерации отчета: %v", err)
		b.api.Send(tgbotapi.NewEditMessageText(
			chatID, sentMsg.MessageID,
			fmt.Sprintf("Ошибка при генерации отчета: %v", err),
		))
		return
	}

	// Отправляем email
	err = b.sendEmail(email, reportType, displayPeriod, filePath, reportName)
	if err != nil {
		log.Printf("Ошибка при отправке email: %v", err)
		b.api.Send(tgbotapi.NewEditMessageText(
			chatID, sentMsg.MessageID,
			fmt.Sprintf("Ошибка при отправке отчета: %v", err),
		))
		return
	}

	// Спрашиваем, хочет ли пользователь сохранить email
	savedEmail, _ := b.getUserEmail(userID)
	if savedEmail != email {
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

func (b *Bot) Initialize() error {
	// Инициализация таблицы для хранения email адресов
	if err := b.initializeEmailStorage(); err != nil {
		return fmt.Errorf("ошибка инициализации хранилища email: %w", err)
	}

	return nil
}
