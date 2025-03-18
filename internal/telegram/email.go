package telegram

import (
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"gopkg.in/mail.v2"
)

type SMTPConfig struct {
	Host     string
	Port     int
	Username string
	Password string
	FromAddr string
	UseSSL   bool
}

type UserEmail struct {
	UserID    int64     `db:"user_id"`
	Email     string    `db:"email"`
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
}

type EmailService struct {
	db         *sqlx.DB
	smtpConfig SMTPConfig
}

func NewEmailService(db *sqlx.DB) (*EmailService, error) {

	smtpHost := os.Getenv("SMTP_HOST")
	smtpPortStr := os.Getenv("SMTP_PORT")
	smtpPort, err := strconv.Atoi(smtpPortStr)
	if err != nil || smtpPort <= 0 {
		smtpPort = 587
	}

	smtpUser := os.Getenv("SMTP_USER")
	smtpPass := os.Getenv("SMTP_PASSWORD")
	smtpSSL := strings.ToLower(os.Getenv("SMTP_SSL")) == "true"
	fromAddr := os.Getenv("SMTP_FROM")

	if fromAddr == "" {
		fromAddr = "noreply@wildberries-monitor.com"
	}

	service := &EmailService{
		db: db,
		smtpConfig: SMTPConfig{
			Host:     smtpHost,
			Port:     smtpPort,
			Username: smtpUser,
			Password: smtpPass,
			FromAddr: fromAddr,
			UseSSL:   smtpSSL,
		},
	}

	if err := service.initializeEmailStorage(); err != nil {
		return nil, fmt.Errorf("failed to initialize email storage: %w", err)
	}

	return service, nil
}

func (e *EmailService) initializeEmailStorage() error {
	_, err := e.db.Exec(`
		CREATE TABLE IF NOT EXISTS user_emails (
			user_id BIGINT PRIMARY KEY,
			email TEXT NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
		)
	`)
	return err
}

func (e *EmailService) GetUserEmail(userID int64) (string, error) {
	var email string
	err := e.db.Get(&email, "SELECT email FROM user_emails WHERE user_id = $1", userID)
	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			return "", nil
		}
		return "", fmt.Errorf("database error: %w", err)
	}
	return email, nil
}

func (e *EmailService) SaveUserEmail(userID int64, email string) error {

	if !isValidEmail(email) {
		return fmt.Errorf("invalid email address format")
	}

	tx, err := e.db.Beginx()
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback()

	var exists bool
	err = tx.Get(&exists, "SELECT EXISTS(SELECT 1 FROM user_emails WHERE user_id = $1)", userID)
	if err != nil {
		return fmt.Errorf("failed to check existing email: %w", err)
	}

	if exists {

		_, err = tx.Exec(
			"UPDATE user_emails SET email = $1, updated_at = NOW() WHERE user_id = $2",
			email, userID)
	} else {

		_, err = tx.Exec(
			"INSERT INTO user_emails (user_id, email, created_at, updated_at) VALUES ($1, $2, NOW(), NOW())",
			userID, email)
	}

	if err != nil {
		return fmt.Errorf("failed to save email: %w", err)
	}

	return tx.Commit()
}

func (e *EmailService) SendEmail(to, subject, body, attachmentPath, attachmentName string) error {

	if e.smtpConfig.Host == "" {
		return fmt.Errorf("SMTP host not configured")
	}

	log.Printf("Sending email to %s via SMTP server %s:%d", to, e.smtpConfig.Host, e.smtpConfig.Port)

	isMailHog := strings.Contains(strings.ToLower(e.smtpConfig.Host), "mailhog")

	if isMailHog {
		return e.sendEmailToMailHog(to, subject, body, attachmentPath, attachmentName)
	}

	return e.sendEmailWithAuth(to, subject, body, attachmentPath, attachmentName)
}

func (e *EmailService) sendEmailToMailHog(to, subject, body, attachmentPath, attachmentName string) error {
	m := mail.NewMessage()
	m.SetHeader("From", e.smtpConfig.FromAddr)
	m.SetHeader("To", to)
	m.SetHeader("Subject", subject)
	m.SetBody("text/plain", body)

	if attachmentPath != "" {
		if _, err := os.Stat(attachmentPath); os.IsNotExist(err) {
			return fmt.Errorf("attachment file does not exist: %s", attachmentPath)
		}

		if attachmentName != "" {
			m.Attach(attachmentPath, mail.Rename(attachmentName))
		} else {
			m.Attach(attachmentPath)
		}
	}

	d := mail.NewDialer(e.smtpConfig.Host, e.smtpConfig.Port, "", "")

	d.SSL = false
	d.TLSConfig = nil
	d.StartTLSPolicy = mail.OpportunisticStartTLS

	return d.DialAndSend(m)
}

func (e *EmailService) sendEmailWithAuth(to, subject, body, attachmentPath, attachmentName string) error {
	m := mail.NewMessage()
	m.SetHeader("From", e.smtpConfig.FromAddr)
	m.SetHeader("To", to)
	m.SetHeader("Subject", subject)
	m.SetBody("text/plain", body)

	if attachmentPath != "" {
		if _, err := os.Stat(attachmentPath); os.IsNotExist(err) {
			return fmt.Errorf("attachment file does not exist: %s", attachmentPath)
		}

		if attachmentName != "" {
			m.Attach(attachmentPath, mail.Rename(attachmentName))
		} else {
			m.Attach(attachmentPath)
		}
	}

	d := mail.NewDialer(e.smtpConfig.Host, e.smtpConfig.Port, e.smtpConfig.Username, e.smtpConfig.Password)

	d.SSL = e.smtpConfig.UseSSL

	if !e.smtpConfig.UseSSL {

		d.StartTLSPolicy = mail.MandatoryStartTLS
	}

	return d.DialAndSend(m)
}

func (e *EmailService) SendReportEmail(to, reportType, period, filePath, reportName string) error {

	if !isValidEmail(to) {
		return fmt.Errorf("invalid email address: %s", to)
	}

	subject := fmt.Sprintf("Wildberries %s Report - %s", reportType, period)

	body := fmt.Sprintf(`Hello,

Your requested Wildberries %s report for period %s is attached.

Thank you for using Wildberries Monitoring Service!

This is an automated message, please do not reply.
`, reportType, period)

	maxRetries := 3
	for attempt := 0; attempt < maxRetries; attempt++ {
		err := e.SendEmail(to, subject, body, filePath, reportName)
		if err == nil {
			log.Printf("Successfully sent report email to %s", to)
			return nil
		}

		log.Printf("Attempt %d: Failed to send email: %v", attempt+1, err)

		if attempt < maxRetries-1 {

			retryDelay := time.Duration(attempt+1) * 2 * time.Second
			time.Sleep(retryDelay)
		}
	}

	return fmt.Errorf("failed to send email after %d attempts", maxRetries)
}

func isValidEmail(email string) bool {

	emailRegex := regexp.MustCompile(`^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}`)
	return emailRegex.MatchString(email)
}

type AttachmentInfo struct {
	FilePath  string
	FileName  string
	MimeType  string
	FileSize  int64
	IsVisible bool
}

func NewAttachmentInfo(filePath string, customFileName string) (*AttachmentInfo, error) {

	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return nil, fmt.Errorf("error accessing attachment file: %w", err)
	}

	if fileInfo.IsDir() {
		return nil, fmt.Errorf("attachment path is a directory, not a file")
	}

	fileName := customFileName
	if fileName == "" {
		fileName = fileInfo.Name()
	}

	mimeType := guessMimeType(fileName)

	return &AttachmentInfo{
		FilePath:  filePath,
		FileName:  fileName,
		MimeType:  mimeType,
		FileSize:  fileInfo.Size(),
		IsVisible: true,
	}, nil
}

func guessMimeType(fileName string) string {
	ext := strings.ToLower(fileName[strings.LastIndex(fileName, ".")+1:])

	switch ext {
	case "pdf":
		return "application/pdf"
	case "xlsx":
		return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
	case "xls":
		return "application/vnd.ms-excel"
	case "doc":
		return "application/msword"
	case "docx":
		return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
	case "jpg", "jpeg":
		return "image/jpeg"
	case "png":
		return "image/png"
	case "txt":
		return "text/plain"
	default:
		return "application/octet-stream"
	}
}

func ReadFileContents(filePath string) ([]byte, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	return content, nil
}
