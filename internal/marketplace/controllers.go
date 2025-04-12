package marketplace

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
)

// MarketplaceController обрабатывает запросы API для управления товарами на маркетплейсе
type MarketplaceController struct {
	service *MarketplaceService
}

// NewMarketplaceController создает новый контроллер для управления товарами маркетплейса
func NewMarketplaceController(service *MarketplaceService) *MarketplaceController {
	return &MarketplaceController{
		service: service,
	}
}

// RegisterRoutes регистрирует все маршруты API для управления товарами
func (c *MarketplaceController) RegisterRoutes(router *mux.Router) {
	// Префикс для API
	api := router.PathPrefix("/api/marketplace").Subrouter()

	// CORS и JWT middleware
	api.Use(CORSMiddleware)
	api.Use(JWTAuthMiddleware)

	// Маршруты для работы с товарами
	api.HandleFunc("/products", c.GetProducts).Methods("GET", "OPTIONS")
	api.HandleFunc("/products/sync", c.SyncProducts).Methods("POST", "OPTIONS")
	api.HandleFunc("/products/{id}", c.GetProduct).Methods("GET", "OPTIONS")
	api.HandleFunc("/products", c.CreateProduct).Methods("POST", "OPTIONS")
	api.HandleFunc("/products/{id}", c.UpdateProduct).Methods("PUT", "OPTIONS")
	api.HandleFunc("/products/{id}", c.DeleteProduct).Methods("DELETE", "OPTIONS")

	// Маршруты для работы с категориями
	api.HandleFunc("/categories", c.GetCategories).Methods("GET", "OPTIONS")

	// Маршруты для работы с характеристиками
	api.HandleFunc("/characteristics/{subjectId}", c.GetCharacteristics).Methods("GET", "OPTIONS")

	// Маршруты для загрузки изображений
	api.HandleFunc("/upload/image/{id}", c.UploadImage).Methods("POST", "OPTIONS")

	log.Println("Зарегистрированы маршруты API для управления товарами на маркетплейсе")
}

// Структуры для фильтрации и пагинации
type ProductsFilter struct {
	Search     string   `json:"search"`
	Categories []int    `json:"categories"`
	Brands     []string `json:"brands"`
	InMarket   *bool    `json:"inMarket"`
	Page       int      `json:"page"`
	PerPage    int      `json:"perPage"`
}

// Обработчики API

// GetProducts возвращает список товаров с фильтрацией и пагинацией
func (c *MarketplaceController) GetProducts(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	// Извлекаем параметры фильтрации и пагинации
	filter := ProductsFilter{
		Page:    1,
		PerPage: 20,
	}

	// Получаем параметры из query string
	if search := r.URL.Query().Get("search"); search != "" {
		filter.Search = search
	}

	if page := r.URL.Query().Get("page"); page != "" {
		if pageNum, err := strconv.Atoi(page); err == nil && pageNum > 0 {
			filter.Page = pageNum
		}
	}

	if perPage := r.URL.Query().Get("perPage"); perPage != "" {
		if perPageNum, err := strconv.Atoi(perPage); err == nil && perPageNum > 0 && perPageNum <= 100 {
			filter.PerPage = perPageNum
		}
	}

	if categories := r.URL.Query()["category"]; len(categories) > 0 {
		for _, cat := range categories {
			if catID, err := strconv.Atoi(cat); err == nil {
				filter.Categories = append(filter.Categories, catID)
			}
		}
	}

	if brands := r.URL.Query()["brand"]; len(brands) > 0 {
		filter.Brands = brands
	}

	if inMarket := r.URL.Query().Get("inMarket"); inMarket != "" {
		inMarketBool := inMarket == "true"
		filter.InMarket = &inMarketBool
	}

	// Получаем продукты из сервиса
	products, total, err := c.service.GetProducts(ctx, filter)
	if err != nil {
		log.Printf("Ошибка при получении списка товаров: %v", err)
		http.Error(w, fmt.Sprintf("Ошибка при получении списка товаров: %v", err), http.StatusInternalServerError)
		return
	}

	// Формируем ответ
	response := map[string]interface{}{
		"items":      products,
		"total":      total,
		"page":       filter.Page,
		"perPage":    filter.PerPage,
		"totalPages": (total + filter.PerPage - 1) / filter.PerPage,
	}

	// Добавляем заголовки для пагинации в стиле HATEOAS
	addPaginationHeaders(w, filter.Page, filter.PerPage, total, r.URL.Path)

	// Отправляем ответ
	sendJSONResponse(w, response)
}

// GetProduct возвращает информацию о конкретном товаре
func (c *MarketplaceController) GetProduct(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// Получаем ID товара из URL
	vars := mux.Vars(r)
	productID, err := strconv.Atoi(vars["id"])
	if err != nil {
		http.Error(w, "Некорректный ID товара", http.StatusBadRequest)
		return
	}

	// Получаем товар из сервиса
	product, err := c.service.GetProduct(ctx, productID)
	if err != nil {
		if err == ErrProductNotFound {
			http.Error(w, "Товар не найден", http.StatusNotFound)
		} else {
			log.Printf("Ошибка при получении товара: %v", err)
			http.Error(w, fmt.Sprintf("Ошибка при получении товара: %v", err), http.StatusInternalServerError)
		}
		return
	}

	// Отправляем ответ
	sendJSONResponse(w, product)
}

// CreateProduct создает новый товар
func (c *MarketplaceController) CreateProduct(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()

	// Проверяем права доступа - только админы могут создавать товары
	claims := getUserClaims(r)
	if claims == nil || claims.Role != "admin" {
		http.Error(w, "Недостаточно прав для выполнения операции", http.StatusForbidden)
		return
	}

	// Читаем и декодируем тело запроса
	var product Product
	err := json.NewDecoder(r.Body).Decode(&product)
	if err != nil {
		http.Error(w, "Ошибка в формате данных", http.StatusBadRequest)
		return
	}

	// Валидируем данные
	if err := validateProduct(&product); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Создаем товар в локальной БД
	createdProduct, err := c.service.CreateProduct(ctx, &product)
	if err != nil {
		log.Printf("Ошибка при создании товара: %v", err)
		http.Error(w, fmt.Sprintf("Ошибка при создании товара: %v", err), http.StatusInternalServerError)
		return
	}

	// Отправляем ответ с кодом 201 Created
	w.Header().Set("Location", fmt.Sprintf("/api/marketplace/products/%d", createdProduct.ID))
	w.WriteHeader(http.StatusCreated)
	sendJSONResponse(w, createdProduct)
}

// UpdateProduct обновляет существующий товар
func (c *MarketplaceController) UpdateProduct(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()

	// Проверяем права доступа
	claims := getUserClaims(r)
	if claims == nil || claims.Role != "admin" {
		http.Error(w, "Недостаточно прав для выполнения операции", http.StatusForbidden)
		return
	}

	// Получаем ID товара из URL
	vars := mux.Vars(r)
	productID, err := strconv.Atoi(vars["id"])
	if err != nil {
		http.Error(w, "Некорректный ID товара", http.StatusBadRequest)
		return
	}

	// Читаем и декодируем тело запроса
	var product Product
	err = json.NewDecoder(r.Body).Decode(&product)
	if err != nil {
		http.Error(w, "Ошибка в формате данных", http.StatusBadRequest)
		return
	}

	// Устанавливаем ID из URL
	product.ID = productID

	// Валидируем данные
	if err := validateProduct(&product); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Обновляем товар
	updatedProduct, err := c.service.UpdateProduct(ctx, &product)
	if err != nil {
		if err == ErrProductNotFound {
			http.Error(w, "Товар не найден", http.StatusNotFound)
		} else {
			log.Printf("Ошибка при обновлении товара: %v", err)
			http.Error(w, fmt.Sprintf("Ошибка при обновлении товара: %v", err), http.StatusInternalServerError)
		}
		return
	}

	// Отправляем обновленный товар в ответе
	sendJSONResponse(w, updatedProduct)
}

// DeleteProduct удаляет товар
func (c *MarketplaceController) DeleteProduct(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	// Проверяем права доступа
	claims := getUserClaims(r)
	if claims == nil || claims.Role != "admin" {
		http.Error(w, "Недостаточно прав для выполнения операции", http.StatusForbidden)
		return
	}

	// Получаем ID товара из URL
	vars := mux.Vars(r)
	productID, err := strconv.Atoi(vars["id"])
	if err != nil {
		http.Error(w, "Некорректный ID товара", http.StatusBadRequest)
		return
	}

	// Удаляем товар
	err = c.service.DeleteProduct(ctx, productID)
	if err != nil {
		if err == ErrProductNotFound {
			http.Error(w, "Товар не найден", http.StatusNotFound)
		} else {
			log.Printf("Ошибка при удалении товара: %v", err)
			http.Error(w, fmt.Sprintf("Ошибка при удалении товара: %v", err), http.StatusInternalServerError)
		}
		return
	}

	// Отправляем успешный ответ без тела
	w.WriteHeader(http.StatusNoContent)
}

// SyncProducts синхронизирует товары с маркетплейсом
func (c *MarketplaceController) SyncProducts(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	// Проверяем права доступа
	claims := getUserClaims(r)
	if claims == nil || claims.Role != "admin" {
		http.Error(w, "Недостаточно прав для выполнения операции", http.StatusForbidden)
		return
	}

	// Получаем IDs товаров для синхронизации
	var request struct {
		ProductIDs []int  `json:"productIds"`
		Action     string `json:"action"` // "upload", "delete", "update"
	}

	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		http.Error(w, "Ошибка в формате данных", http.StatusBadRequest)
		return
	}

	// Проверяем, что указано действие
	if request.Action == "" {
		http.Error(w, "Необходимо указать действие (action)", http.StatusBadRequest)
		return
	}

	// Валидируем действие
	if request.Action != "upload" && request.Action != "delete" && request.Action != "update" {
		http.Error(w, "Неверное действие. Допустимые значения: upload, delete, update", http.StatusBadRequest)
		return
	}

	// Выполняем синхронизацию в зависимости от действия
	var result interface{}
	var syncErr error

	switch request.Action {
	case "upload":
		result, syncErr = c.service.UploadProductsToMarketplace(ctx, request.ProductIDs)
	case "delete":
		result, syncErr = c.service.RemoveProductsFromMarketplace(ctx, request.ProductIDs)
	case "update":
		result, syncErr = c.service.UpdateProductsOnMarketplace(ctx, request.ProductIDs)
	}

	if syncErr != nil {
		log.Printf("Ошибка при синхронизации товаров: %v", syncErr)
		http.Error(w, fmt.Sprintf("Ошибка при синхронизации товаров: %v", syncErr), http.StatusInternalServerError)
		return
	}

	// Отправляем результат
	sendJSONResponse(w, map[string]interface{}{
		"success": true,
		"action":  request.Action,
		"result":  result,
	})
}

// GetCategories возвращает список категорий маркетплейса
func (c *MarketplaceController) GetCategories(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	// Получаем категории
	categories, err := c.service.GetCategories(ctx)
	if err != nil {
		log.Printf("Ошибка при получении категорий: %v", err)
		http.Error(w, fmt.Sprintf("Ошибка при получении категорий: %v", err), http.StatusInternalServerError)
		return
	}

	// Отправляем ответ
	sendJSONResponse(w, categories)
}

// GetCharacteristics возвращает список характеристик для категории
func (c *MarketplaceController) GetCharacteristics(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	// Получаем ID категории из URL
	vars := mux.Vars(r)
	subjectID, err := strconv.Atoi(vars["subjectId"])
	if err != nil {
		http.Error(w, "Некорректный ID категории", http.StatusBadRequest)
		return
	}

	// Получаем характеристики
	characteristics, err := c.service.GetCharacteristics(ctx, subjectID)
	if err != nil {
		log.Printf("Ошибка при получении характеристик: %v", err)
		http.Error(w, fmt.Sprintf("Ошибка при получении характеристик: %v", err), http.StatusInternalServerError)
		return
	}

	// Отправляем ответ
	sendJSONResponse(w, characteristics)
}

// UploadImage загружает изображение для товара
func (c *MarketplaceController) UploadImage(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Проверяем права доступа
	claims := getUserClaims(r)
	if claims == nil || claims.Role != "admin" {
		http.Error(w, "Недостаточно прав для выполнения операции", http.StatusForbidden)
		return
	}

	// Получаем ID товара из URL
	vars := mux.Vars(r)
	productID, err := strconv.Atoi(vars["id"])
	if err != nil {
		http.Error(w, "Некорректный ID товара", http.StatusBadRequest)
		return
	}

	// Проверяем размер файла (ограничение в 5 МБ)
	err = r.ParseMultipartForm(5 << 20) // 5 MB
	if err != nil {
		http.Error(w, "Ошибка при обработке файла: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Получаем файл из формы
	file, handler, err := r.FormFile("image")
	if err != nil {
		http.Error(w, "Ошибка при получении файла: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()

	// Проверяем тип файла
	contentType := handler.Header.Get("Content-Type")
	if contentType != "image/jpeg" && contentType != "image/png" && contentType != "image/webp" {
		http.Error(w, "Неподдерживаемый тип файла. Разрешены только JPEG, PNG и WebP", http.StatusBadRequest)
		return
	}

	// Читаем содержимое файла
	var buf bytes.Buffer
	io.Copy(&buf, file)

	// Загружаем изображение
	imageURL, err := c.service.UploadProductImage(ctx, productID, buf.Bytes(), contentType)
	if err != nil {
		log.Printf("Ошибка при загрузке изображения: %v", err)
		http.Error(w, fmt.Sprintf("Ошибка при загрузке изображения: %v", err), http.StatusInternalServerError)
		return
	}

	// Отправляем URL загруженного изображения
	sendJSONResponse(w, map[string]string{
		"url": imageURL,
	})
}

// Вспомогательные функции

// validateProduct проверяет корректность данных товара
func validateProduct(product *Product) error {
	if product.NmID <= 0 {
		return fmt.Errorf("артикул WB должен быть положительным числом")
	}

	if product.VendorCode == "" {
		return fmt.Errorf("артикул продавца не может быть пустым")
	}

	if product.Title == "" {
		return fmt.Errorf("название товара не может быть пустым")
	}

	if product.SubjectID <= 0 {
		return fmt.Errorf("ID категории должен быть положительным числом")
	}

	return nil
}

// getUserClaims извлекает данные пользователя из контекста запроса
func getUserClaims(r *http.Request) *JWTClaims {
	claims, ok := r.Context().Value("claims").(*JWTClaims)
	if !ok {
		return nil
	}
	return claims
}

// addPaginationHeaders добавляет заголовки для пагинации в ответ
func addPaginationHeaders(w http.ResponseWriter, page, perPage, total int, basePath string) {
	totalPages := (total + perPage - 1) / perPage

	links := []string{}

	// Первая страница
	links = append(links, fmt.Sprintf("<%s?page=1&perPage=%d>; rel=\"first\"", basePath, perPage))

	// Предыдущая страница
	if page > 1 {
		prevPage := page - 1
		links = append(links, fmt.Sprintf("<%s?page=%d&perPage=%d>; rel=\"prev\"", basePath, prevPage, perPage))
	}

	// Следующая страница
	if page < totalPages {
		nextPage := page + 1
		links = append(links, fmt.Sprintf("<%s?page=%d&perPage=%d>; rel=\"next\"", basePath, nextPage, perPage))
	}

	// Последняя страница
	links = append(links, fmt.Sprintf("<%s?page=%d&perPage=%d>; rel=\"last\"", basePath, totalPages, perPage))

	// Устанавливаем заголовок Link
	w.Header().Set("Link", strings.Join(links, ", "))

	// Устанавливаем X-Total-Count
	w.Header().Set("X-Total-Count", strconv.Itoa(total))
}

// sendJSONResponse отправляет JSON-ответ
func sendJSONResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")

	// Если статус еще не установлен, устанавливаем 200 OK
	if w.Header().Get("Status") == "" {
		w.WriteHeader(http.StatusOK)
	}

	json.NewEncoder(w).Encode(data)
}
