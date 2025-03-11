package telegram

import (
	"fmt"
	"gopkg.in/mail.v2"
	"log"
	"net/smtp"
	"os"
	"regexp"
	"strconv"
	"time"
)

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

// Функция для фактической отправки email
func (b *Bot) sendEmail(to string, reportType string, period string, filePath string, reportName string) error {
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

	// Критически важно: отключаем SSL и StartTLS для MailHog
	d.SSL = false
	d.TLSConfig = nil
	d.StartTLSPolicy = mail.MandatoryStartTLS

	// Добавляем логирование перед отправкой
	log.Printf("Отправка email на %s через SMTP сервер %s:%d", to, smtpHost, smtpPort)

	return d.DialAndSend(m)
}
func (b *Bot) sendEmailSimple(to string, reportType string, period string, filePath string, reportName string) error {
	smtpHost := os.Getenv("SMTP_HOST")
	smtpPortStr := os.Getenv("SMTP_PORT")
	smtpPort, _ := strconv.Atoi(smtpPortStr)
	smtpAddr := fmt.Sprintf("%s:%d", smtpHost, smtpPort)

	// Чтение файла для прикрепления
	_, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("не удалось прочитать файл: %v", err)
	}

	// Формирование простого сообщения
	var subject string
	if reportType == "prices" {
		subject = "Отчет по ценам"
	} else {
		subject = "Отчет по остаткам"
	}

	// Простая отправка через SMTP без шифрования
	message := []byte(fmt.Sprintf("To: %s\r\n"+
		"Subject: %s\r\n"+
		"Content-Type: text/plain; charset=UTF-8\r\n"+
		"\r\n"+
		"Отчет %s за период %s\r\n",
		to, subject, subject, period))

	log.Printf("Отправка email на %s через SMTP сервер %s", to, smtpAddr)

	// Отправка без аутентификации для MailHog
	return smtp.SendMail(smtpAddr, nil, "noreply@example.com", []string{to}, message)
}
