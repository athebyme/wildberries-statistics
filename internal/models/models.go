package models

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"
)

// Структуры для запросов и ответов API

// Склады WB
type Warehouse struct {
	Name         string `json:"name"`
	OfficeID     int64  `json:"officeId"`
	ID           int64  `json:"id"`
	CargoType    int    `json:"cargoType"`
	DeliveryType int    `json:"deliveryType"`
}

// Запрос остатков
type StockRequest struct {
	Skus []string `json:"skus"`
}

// Ответ по остаткам
type StockResponse struct {
	Stocks []Stock `json:"stocks"`
}

type Stock struct {
	Sku    string `json:"sku"`
	Amount int    `json:"amount"`
}

// Информация о ценах и скидках
type PriceHistoryResponse struct {
	Data struct {
		UploadID     int           `json:"uploadID"`
		HistoryGoods []GoodHistory `json:"historyGoods"`
	} `json:"data"`
}

type GoodHistory struct {
	NmID            int    `json:"nmID"`
	VendorCode      string `json:"vendorCode"`
	SizeID          int    `json:"sizeID"`
	TechSizeName    string `json:"techSizeName"`
	Price           int    `json:"price"`
	CurrencyIsoCode string `json:"currencyIsoCode4217"`
	Discount        int    `json:"discount"`
	ClubDiscount    int    `json:"clubDiscount"`
	Status          int    `json:"status"`
	ErrorText       string `json:"errorText,omitempty"`
}

// Структура для записи о цене
type PriceRecord struct {
	ID                int       `db:"id"`
	ProductID         int       `db:"product_id"`
	SizeID            int       `db:"size_id"`
	Price             int       `db:"price"`
	Discount          int       `db:"discount"`
	ClubDiscount      int       `db:"club_discount"`
	FinalPrice        int       `db:"final_price"`
	ClubFinalPrice    int       `db:"club_final_price"`
	CurrencyIsoCode   string    `db:"currency_iso_code"`
	TechSizeName      string    `db:"tech_size_name"`
	EditableSizePrice bool      `db:"editable_size_price"`
	RecordedAt        time.Time `db:"recorded_at"`
}

// Структура для записи о складских остатках
type StockRecord struct {
	ID          int       `db:"id"` // Add this line
	ProductID   int       `db:"product_id"`
	WarehouseID int64     `db:"warehouse_id"`
	Amount      int       `db:"amount"`
	RecordedAt  time.Time `db:"recorded_at"`
}

// ProductRecord представляет информацию о продукте
type ProductRecord struct {
	ID         int       `db:"id"`
	NmID       int       `db:"nm_id"`
	VendorCode string    `db:"vendor_code"`
	Barcode    string    `db:"barcode"`
	Name       string    `db:"name"`
	CreatedAt  time.Time `db:"created_at"`
}

// Структура для ответа API цен и скидок
type GoodsPricesResponse struct {
	Data struct {
		ListGoods []struct {
			NmID         int        `json:"nmID"`
			VendorCode   string     `json:"vendorCode"`
			Sizes        []GoodSize `json:"sizes"`
			Discount     int        `json:"discount"`
			ClubDiscount int        `json:"clubDiscount"`
		} `json:"listGoods"`
	} `json:"data"`
}

// Структура для размера товара с ценами и скидками
type GoodSize struct {
	SizeID              int     `json:"sizeID"`
	Price               int     `json:"price"`
	DiscountedPrice     float64 `json:"discountedPrice"`
	ClubDiscountedPrice float64 `json:"clubDiscountedPrice"`
	TechSizeName        string  `json:"techSizeName"`
	CurrencyIsoCode4217 string  `json:"currencyIsoCode4217"`
	Discount            int     `json:"discount"`
	ClubDiscount        int     `json:"clubDiscount"`
	EditableSizePrice   bool    `json:"editableSizePrice"`
}

// --- SearchEngine related structures ---

type SettingsRequestWrapper struct {
	Settings Settings `json:"settings"`
}

type Settings struct {
	Sort   Sort   `json:"sort"`
	Filter Filter `json:"filter"`
	Cursor Cursor `json:"cursor"`
}

// CreateRequestBody creates request body for SearchEngine API calls.
func (s *Settings) CreateRequestBody() (*bytes.Buffer, error) {
	wrapper := SettingsRequestWrapper{Settings: *s}

	jsonData, err := json.Marshal(wrapper)
	if err != nil {
		return nil, fmt.Errorf("marshalling settings: %w", err)
	}
	return bytes.NewBuffer(jsonData), nil
}

type Sort struct {
	Ascending bool `json:"ascending"` // Сортировать по полю updatedAt (false - по убыванию, true - по возрастанию)
}

type Filter struct {
	/*
		Фильтр по фото:
		0 — только карточки без фото
		1 — только карточки с фото
		-1 — все карточки товара
	*/
	WithPhoto int `json:"withPhoto"`

	/*
		Поиск по артикулу продавца, артикулу WB, баркоду
	*/
	TextSearch string `json:"textSearch"`

	/*
		Поиск по ID тегов
	*/
	TagIDs []int `json:"tagIDs"`

	/*
		Фильтр по категории. true - только разрешённые, false - все. Не используется в песочнице.
	*/
	AllowedCategoriesOnly bool `json:"allowedCategoriesOnly"`

	/*
		Поиск по id предметов
	*/
	ObjectIDs []int `json:"objectIDs"`

	/*
		Поиск по брендам
	*/
	Brands []string `json:"brands"`

	/*
		Поиск по ID карточки товара
	*/
	ImtID int `json:"imtID"`
}

type Cursor struct {
	Limit     int    `json:"limit"` // Сколько карточек товара выдать в ответе.
	UpdatedAt string `json:"updatedAt,omitempty"`
	NmID      int    `json:"nmId,omitempty"`
}

type NomenclatureResponse struct {
	Data   []Nomenclature `json:"cards"`
	Cursor Cursor         `json:"cursor"`
}

type Nomenclature struct {
	NmID            int        `json:"nmID"`
	ImtID           int        `json:"imtID"`
	NmUUID          string     `json:"nmUUID"`
	SubjectID       int        `json:"subjectID"`
	VendorCode      string     `json:"vendorCode"`
	SubjectName     string     `json:"subjectName"`
	Brand           string     `json:"brand"`
	Title           string     `json:"title"`
	Photos          []Photo    `json:"photos"`
	Video           string     `json:"video"`
	Dimensions      Dimensions `json:"dimensions"`
	Characteristics []Charc    `json:"characteristics"`
	Sizes           []Size     `json:"sizes"`
	Tags            []Tag      `json:"tags"`
	CreatedAt       string     `json:"createdAt"`
	UpdatedAt       string     `json:"updatedAt"`
}

type Photo struct {
	Big    string `json:"big"`
	Tiny   string `json:"tm"`
	Small  string `json:"c246x328"`
	Square string `json:"square"`
	Medium string `json:"c516x688"`
}

type Dimensions struct {
	Length  int  `json:"length"`
	Width   int  `json:"width"`
	Height  int  `json:"height"`
	IsValid bool `json:"isValid"`
}

type Charc struct {
	Id    int    `json:"id"`    // ID характеристики
	Name  string `json:"name"`  // Название характеристики
	Value any    `json:"value"` // Значение характеристики. Тип значения зависит от типа характеристики
}
type Size struct {
	ChrtID   int      `json:"chrtID"`   //Числовой ID размера для данного артикула WB
	TechSize string   `json:"techSize"` // Размер товара (А, XXL, 57 и др.)
	WbSize   string   `json:"wbSize"`   // Российский размер товара
	Skus     []string `json:"skus"`     // Баркод товара
}

type Tag struct {
	Id    int    `json:"id"`
	Name  string `json:"name"`
	Color string `json:"color"`
}
