package report

type ReportConfig struct {
	MinPriceChangePercent float64 `json:"minPriceChangePercent"`
	MinStockChangePercent float64 `json:"minStockChangePercent"`
}
