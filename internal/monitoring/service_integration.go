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

// InitializeServices sets up improved services for monitoring
func InitializeImprovedServices(
	cfg config.Config,
	db *sqlx.DB,
	bot *telegram.Bot,
) (*HourlyDataKeeper, *RecordCleanupService, *telegram.EmailService, *report.PDFGenerator, *report.ExcelGenerator, error) {

	// Configure worker pool sizes based on system resources
	const (
		defaultHourlyWorkers  = 10
		defaultCleanupWorkers = 5
		defaultReportWorkers  = 8
	)

	// Initialize HourlyDataKeeper
	hourlyKeeper := NewHourlyDataKeeper(
		db,
		time.Hour,       // Take snapshots every hour
		30*24*time.Hour, // Keep data for 30 days
		defaultHourlyWorkers,
	)

	// Initialize RecordCleanupService
	cleanupService := NewRecordCleanupService(
		db,
		24*time.Hour,    // Run cleanup once a day
		30*24*time.Hour, // Keep detailed records for 30 days
		hourlyKeeper,
		defaultCleanupWorkers,
	)

	// Initialize EmailService
	emailService, err := telegram.NewEmailService(db)
	if err != nil {
		return nil, nil, nil, nil, nil, fmt.Errorf("error initializing email service: %w", err)
	}

	// Initialize PDFGenerator
	pdfGenerator := report.NewPDFGenerator(db)

	// Initialize ExcelGenerator
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

// UpdateMonitoringServiceWithImprovedComponents updates the existing service with improved components
func UpdateMonitoringServiceWithImprovedComponents(service *Service) error {
	// Create improved services
	hourlyKeeper, cleanupService, emailService, pdfGenerator, excelGenerator, err :=
		InitializeImprovedServices(service.config, service.db, service.telegramBot)

	if err != nil {
		return fmt.Errorf("failed to initialize improved services: %w", err)
	}

	// Update the telegram bot with new services
	if err := service.telegramBot.UpdateReportServices(emailService, pdfGenerator, excelGenerator); err != nil {
		return fmt.Errorf("failed to update telegram bot services: %w", err)
	}

	// Start the hourly data keeper in a goroutine
	go func() {
		log.Println("Starting improved hourly data keeper")
		if err := hourlyKeeper.RunHourlySnapshots(service.ctx); err != nil {
			log.Printf("Hourly data keeper stopped with error: %v", err)
		}
	}()

	// Start the record cleanup service in a goroutine
	go func() {
		log.Println("Starting improved record cleanup service")
		cleanupService.RunCleanupProcess(service.ctx)
	}()

	log.Println("Successfully updated monitoring service with improved components")
	return nil
}

// Helper method to add to MonitoringService struct
func (m *Service) UpdateWithImprovedComponents() error {
	return UpdateMonitoringServiceWithImprovedComponents(m)
}
