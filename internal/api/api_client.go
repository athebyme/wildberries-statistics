package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"wbmonitoring/monitoring/internal/app_errors"
	"wbmonitoring/monitoring/internal/models"

	"golang.org/x/time/rate"
)

func GetSellerInfo(ctx context.Context, client http.Client, apiKey string, limiter *rate.Limiter) (models.Seller, error) {
	if err := limiter.Wait(ctx); err != nil {
		return models.Seller{}, fmt.Errorf("%w: %v", app_errors.ErrRateLimiter, err)
	}

	req, err := http.NewRequest("GET", "https://common-api.wildberries.ru/api/v1/seller-info", nil)
	if err != nil {
		return models.Seller{}, fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+apiKey)

	resp, err := client.Do(req)
	if err != nil {
		return models.Seller{}, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return models.Seller{}, fmt.Errorf("unexpected status code: %s", resp.Status)
	}

	var seller models.Seller
	if err := json.NewDecoder(resp.Body).Decode(&seller); err != nil {
		return models.Seller{}, fmt.Errorf("decoding response: %w", err)
	}

	return seller, nil
}

// GetWarehouses retrieves warehouses from Wildberries API.
func GetWarehouses(ctx context.Context, client *http.Client, apiKey string, limiter *rate.Limiter) ([]models.Warehouse, error) {
	if err := limiter.Wait(ctx); err != nil {
		return nil, fmt.Errorf("%w: %v", app_errors.ErrRateLimiter, err)
	}

	req, err := http.NewRequest("GET", "https://marketplace-api.wildberries.ru/api/v3/warehouses", nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+apiKey)

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %s", resp.Status)
	}

	var warehouses []models.Warehouse
	if err := json.NewDecoder(resp.Body).Decode(&warehouses); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	return warehouses, nil
}

// GetStocks retrieves stock information from Wildberries API.
func GetStocks(ctx context.Context, client *http.Client, apiKey string, limiter *rate.Limiter, warehouseID int64, skus []string) (*models.StockResponse, error) {
	if err := limiter.Wait(ctx); err != nil {
		return nil, fmt.Errorf("%w: %v", app_errors.ErrRateLimiter, err)
	}

	requestBody := models.StockRequest{
		Skus: skus,
	}

	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("marshaling request body: %w", err)
	}

	apiURL := fmt.Sprintf("https://marketplace-api.wildberries.ru/api/v3/stocks/%d", warehouseID)
	req, err := http.NewRequest("POST", apiURL, io.NopCloser(bytes.NewBuffer(jsonBody))) // Use bytes.Buffer and NopCloser
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %s", resp.Status)
	}

	var stockResponse models.StockResponse
	if err := json.NewDecoder(resp.Body).Decode(&stockResponse); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	return &stockResponse, nil
}

// GetPriceHistory retrieves price history from Wildberries API.
func GetPriceHistory(ctx context.Context, client *http.Client, apiKey string, limiter *rate.Limiter, uploadID int, limit, offset int) (*models.PriceHistoryResponse, error) {
	if err := limiter.Wait(ctx); err != nil {
		return nil, fmt.Errorf("%w: %v", app_errors.ErrRateLimiter, err)
	}

	apiURL := fmt.Sprintf("https://discounts-prices-api.wildberries.ru/api/v2/history/goods/task?uploadID=%d&limit=%d&offset=%d",
		uploadID, limit, offset)

	req, err := http.NewRequest("GET", apiURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+apiKey)

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %s", resp.Status)
	}

	var priceHistoryResponse models.PriceHistoryResponse
	if err := json.NewDecoder(resp.Body).Decode(&priceHistoryResponse); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	return &priceHistoryResponse, nil
}

// GetGoodsPrices retrieves goods prices from Wildberries API.
func GetGoodsPrices(ctx context.Context, client *http.Client, apiKey string, limiter *rate.Limiter, limit int, offset int, filterNmID int) (*models.GoodsPricesResponse, error) {
	if err := limiter.Wait(ctx); err != nil {
		return nil, fmt.Errorf("%w: %v", app_errors.ErrRateLimiter, err)
	}

	// Базовый URL
	baseURL := "https://discounts-prices-api.wildberries.ru/api/v2/list/goods/filter"

	// Формируем URL с параметрами
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("invalid base URL: %w", err)
	}

	q := u.Query()
	q.Set("limit", strconv.Itoa(limit))
	q.Set("offset", strconv.Itoa(offset))

	// Добавляем фильтр по nmID, если указан
	if filterNmID > 0 {
		q.Set("filterNmID", strconv.Itoa(filterNmID))
	}

	u.RawQuery = q.Encode()

	// Создаем запрос
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	// Добавляем заголовок с API-ключом
	req.Header.Set("Authorization", "Bearer"+apiKey)

	// Выполняем запрос
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	// Проверяем статус ответа
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("non-OK status: %d, body: %s", resp.StatusCode, string(body))
	}

	// Декодируем ответ
	var result models.GoodsPricesResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	return &result, nil
}
