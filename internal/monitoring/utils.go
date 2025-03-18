package monitoring

import "wbmonitoring/monitoring/internal/models"

// getKeysFromPriceMap извлекает ключи из карты цен
func getKeysFromPriceMap(m map[int]map[int]models.PriceRecord) []int {
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// getKeysFromStockMap извлекает ключи из карты остатков
func getKeysFromStockMap(m map[int]map[int64]models.StockRecord) []int {
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
