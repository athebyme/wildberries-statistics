package search

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
	"wbmonitoring/monitoring/internal/app_errors"
	"wbmonitoring/monitoring/internal/models"
	"wbmonitoring/monitoring/internal/workers"

	_ "github.com/lib/pq"
	"golang.org/x/time/rate"
)

// SearchEngineConfig Configuration for SearchEngine
type SearchEngineConfig struct {
	WorkerCount    int
	MaxRetries     int
	RetryInterval  time.Duration
	RequestTimeout time.Duration
	ApiKey         string // Добавляем API ключ в конфиг
}

// SearchEngine struct
type SearchEngine struct {
	db      *sql.DB
	writer  io.Writer
	config  SearchEngineConfig
	limiter *rate.Limiter
}

// NewSearchEngine creates a new SearchEngine instance.
func NewSearchEngine(db *sql.DB, writer io.Writer, config SearchEngineConfig) *SearchEngine {
	return &SearchEngine{
		db:      db,
		writer:  writer,
		config:  config,
		limiter: rate.NewLimiter(rate.Every(time.Minute/70), 10),
	}
}

const postNomenclature = "https://content-api.wildberries.ru/content/v2/get/cards/list"

// GetNomenclatures retrieves nomenclatures from Wildberries API.
func (d *SearchEngine) GetNomenclatures(settings models.Settings, locale string) (*models.NomenclatureResponse, error) {
	url := postNomenclature
	if locale != "" {
		url = fmt.Sprintf("%s?locale=%s", url, locale)
	}

	client := &http.Client{Timeout: d.config.RequestTimeout}

	requestBody, err := settings.CreateRequestBody()
	if err != nil {
		return nil, fmt.Errorf("creating request body: %w", err)
	}

	req, err := http.NewRequest("POST", url, requestBody)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+d.config.ApiKey) // Используем API ключ из конфига
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %s", resp.Status)
	}

	var nomenclatureResponse models.NomenclatureResponse
	if err := json.NewDecoder(resp.Body).Decode(&nomenclatureResponse); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	return &nomenclatureResponse, nil
}

// GetNomenclaturesWithLimitConcurrentlyPutIntoChannel retrieves nomenclatures concurrently and puts them into a channel.
func (d *SearchEngine) GetNomenclaturesWithLimitConcurrentlyPutIntoChannel(
	ctx context.Context,
	settings models.Settings,
	locale string,
	nomenclatureChan chan models.Nomenclature,
) error {
	defer log.Printf("Search engine finished its job.")

	limit := settings.Cursor.Limit
	log.Printf("Getting Wildberries nomenclatures with limit: %d", limit)

	ctx, cancel := context.WithTimeout(ctx, time.Minute*20)
	defer cancel()

	cursorManager := workers.NewSafeCursorManager()
	taskChan := make(chan models.Cursor, d.config.WorkerCount)
	errChan := make(chan error, d.config.WorkerCount)

	var (
		wg             sync.WaitGroup
		totalProcessed atomic.Int32
	)

	for i := 0; i < d.config.WorkerCount; i++ {
		wg.Add(1)
		go d.safeWorker(
			ctx,
			i,
			&wg,
			taskChan,
			nomenclatureChan,
			errChan,
			settings,
			locale,
			limit,
			&totalProcessed,
			cursorManager,
		)
	}

	log.Printf("Sending initial task to taskChan")
	select {
	case taskChan <- models.Cursor{NmID: 0, UpdatedAt: "", Limit: 100}:
		log.Printf("Initial task sent successfully")
	case <-ctx.Done():
		log.Printf("Context cancelled while sending initial task")
		return ctx.Err()
	}

	go func() {
		wg.Wait()
		log.Printf("Starting cleanup goroutine")
		log.Printf("All workers finished, closing channels")
		close(taskChan)
		close(errChan)
		log.Printf("Channels closed")
	}()

	for err := range errChan {
		if err == nil {
			continue
		}
		log.Printf("Received error from worker: %v", err)

		if errors.Is(err, app_errors.ErrNoMoreData) {
			log.Printf("No more data to process")
			continue
		}
		if errors.Is(err, app_errors.ErrTotalLimitReached) {
			log.Printf("Total limit reached")
			continue
		}
		if errors.Is(err, app_errors.ErrContextCanceled) {
			log.Printf("Context was canceled")
			continue
		}

		cancel()
		return fmt.Errorf("worker error: %w", err)
	}

	return nil
}

func (d *SearchEngine) safeWorker(
	ctx context.Context,
	workerID int,
	wg *sync.WaitGroup,
	taskChan chan models.Cursor,
	nomenclatureChan chan<- models.Nomenclature,
	errChan chan error,
	settings models.Settings,
	locale string,
	totalLimit int,
	totalProcessed *atomic.Int32,
	cursorManager *workers.SafeCursorManager,
) {
	d.workerLogic(ctx, workerID, wg, taskChan, nomenclatureChan, errChan, settings, locale, totalLimit, totalProcessed, cursorManager)
}

func (d *SearchEngine) workerLogic(
	ctx context.Context,
	workerID int,
	wg *sync.WaitGroup,
	taskChan chan models.Cursor,
	nomenclatureChan chan<- models.Nomenclature,
	errChan chan error,
	settings models.Settings,
	locale string,
	totalLimit int,
	totalProcessed *atomic.Int32,
	cursorManager *workers.SafeCursorManager,
) {
	defer func() {
		log.Printf("Worker %d: job is done.", workerID)
		wg.Done()
	}()

	for {
		select {
		case <-ctx.Done():
			errChan <- app_errors.ErrContextCanceled
			return
		case cursor, ok := <-taskChan:
			if !ok {
				return
			}

			uniqueCursor, ok := cursorManager.GetUniqueCursor(cursor.NmID, cursor.UpdatedAt)
			if !ok {
				continue
			}

			err := d.processSafeCursorTask(
				ctx,
				workerID,
				uniqueCursor,
				nomenclatureChan,
				settings,
				locale,
				totalLimit,
				totalProcessed,
				taskChan,
			)

			if err != nil && !errors.Is(err, app_errors.ErrNoMoreData) && !errors.Is(err, app_errors.ErrTotalLimitReached) {
				errChan <- err
			}
		}
	}
}

func (d *SearchEngine) processSafeCursorTask(
	ctx context.Context,
	workerID int,
	cursor models.Cursor,
	nomenclatureChan chan<- models.Nomenclature,
	settings models.Settings,
	locale string,
	totalLimit int,
	totalProcessed *atomic.Int32,
	taskChan chan<- models.Cursor,
) error {
	if err := d.limiter.Wait(ctx); err != nil {
		log.Printf("Worker %d: Rate limiter error: %v", workerID, err)
		return fmt.Errorf("%w: %v", app_errors.ErrRateLimiter, err)
	}

	settings.Cursor = cursor
	log.Printf("Worker %d: Fetching nomenclatures with cursor: NmID=%d, UpdatedAt=%v, Limit=%d",
		workerID, cursor.NmID, cursor.UpdatedAt, cursor.Limit)

	nomenclatureResponse, err := d.retryGetNomenclatures(ctx, settings, &cursor, locale)
	if err != nil {
		if errors.Is(err, app_errors.ErrContextCanceled) {
			return err
		}
		return fmt.Errorf("failed to get nomenclatures: %w", err)
	}

	lastNomenclature := nomenclatureResponse.Data[len(nomenclatureResponse.Data)-1]

	if len(nomenclatureResponse.Data) == cursor.Limit {
		select {
		case taskChan <- models.Cursor{
			NmID:      lastNomenclature.NmID,
			UpdatedAt: lastNomenclature.UpdatedAt,
			Limit:     cursor.Limit,
		}:
		case <-ctx.Done():
			return app_errors.ErrContextCanceled
		}
	}

	for _, nomenclature := range nomenclatureResponse.Data {
		totalProcessed.Add(1)
		if totalProcessed.Load() >= int32(totalLimit) {
			return app_errors.ErrTotalLimitReached
		}

		select {
		case <-ctx.Done():
			return app_errors.ErrContextCanceled
		case nomenclatureChan <- nomenclature:
		}
	}

	if len(nomenclatureResponse.Data) < cursor.Limit {
		defer close(nomenclatureChan)
		return app_errors.ErrNoMoreData
	}

	return nil
}

func (d *SearchEngine) retryGetNomenclatures(
	ctx context.Context,
	settings models.Settings,
	cursor *models.Cursor,
	locale string,
) (*models.NomenclatureResponse, error) {
	var lastErr error

	for retry := 0; retry < d.config.MaxRetries; retry++ {
		select {
		case <-ctx.Done():
			return nil, app_errors.ErrContextCanceled
		default:
		}

		settings.Cursor = *cursor
		nomenclatureResponse, err := d.GetNomenclatures(settings, locale)
		if err == nil {
			return nomenclatureResponse, nil
		}

		if errors.Is(err, app_errors.ErrConnectionAborted) {
			log.Printf("Retrying to get nomenclatures due to connection error. Attempt: %d", retry+1)
			lastErr = err
			time.Sleep(d.config.RetryInterval)
			continue
		}

		return nil, fmt.Errorf("failed to get nomenclatures: %w", err)
	}

	return nil, fmt.Errorf("%w: %v", app_errors.ErrFailedAfterRetries, lastErr)
}
