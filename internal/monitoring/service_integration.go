package monitoring

import (
	"fmt"
	"log"
	"time"
	"wbmonitoring/monitoring/internal/config"
	"wbmonitoring/monitoring/internal/telegram"
	"wbmonitoring/monitoring/internal/telegram/report"

	"github.com/jmoiron/sqlx"
)

func InitializeImprovedServices(
	cfg config.Config,
	db *sqlx.DB,
	bot *telegram.Bot,
) (*HourlyDataKeeper, *RecordCleanupService, *telegram.EmailService, *report.PDFGenerator, *report.ExcelGenerator, error) {

	const (
		defaultHourlyWorkers  = 10
		defaultCleanupWorkers = 5
		defaultReportWorkers  = 8
	)

	hourlyKeeper := NewHourlyDataKeeper(
		db,
		time.Hour,
		30*24*time.Hour,
		defaultHourlyWorkers,
	)

	cleanupService := NewRecordCleanupService(
		db,
		24*time.Hour,
		10*24*time.Hour,
		hourlyKeeper,
		defaultCleanupWorkers,
	)

	emailService, err := telegram.NewEmailService(db)
	if err != nil {
		return nil, nil, nil, nil, nil, fmt.Errorf("error initializing email service: %w", err)
	}

	pdfGenerator := report.NewPDFGenerator(db)

	excelGenerator := report.NewExcelGenerator(
		db,
		report.ReportConfig{
			MinPriceChangePercent: cfg.PriceThreshold,
			MinStockChangePercent: cfg.StockThreshold,
		},
		defaultReportWorkers,
	)

	return hourlyKeeper, cleanupService, emailService, pdfGenerator, excelGenerator, nil
}

func UpdateMonitoringServiceWithImprovedComponents(service *Service) error {
	hourlyKeeper, cleanupService, emailService, pdfGenerator, excelGenerator, err :=
		InitializeImprovedServices(service.config, service.db, service.telegramBot)

	if err != nil {
		return fmt.Errorf("failed to initialize improved services: %w", err)
	}

	if err := service.telegramBot.UpdateReportServices(emailService, pdfGenerator, excelGenerator); err != nil {
		return fmt.Errorf("failed to update telegram bot services: %w", err)
	}

	// Сохраняем ссылку на сервис очистки
	service.recordCleanupSvc = cleanupService

	go func() {
		log.Println("Starting improved hourly data keeper")
		if err := hourlyKeeper.RunHourlySnapshots(service.ctx); err != nil {
			log.Printf("Hourly data keeper stopped with error: %v", err)
		}
	}()

	go func() {
		log.Println("Starting improved record cleanup service")
		cleanupService.RunCleanupProcess(service.ctx)
	}()

	// Запускаем немедленную очистку после перезапуска
	go func() {
		log.Println("Запуск немедленной очистки после перезапуска")
		if err := cleanupService.CleanupRecords(service.ctx); err != nil {
			log.Printf("Ошибка при запуске немедленной очистки: %v", err)
		}
	}()

	log.Println("Successfully updated monitoring service with improved components")
	return nil
}

func (m *Service) UpdateWithImprovedComponents() error {
	return UpdateMonitoringServiceWithImprovedComponents(m)
}
