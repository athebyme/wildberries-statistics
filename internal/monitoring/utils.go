package monitoring

import "wbmonitoring/monitoring/internal/models"

func getKeysFromPriceMap(m map[int]map[int]models.PriceRecord) []int {
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// getKeysFromPriceSnapMap извлекает ключи из карты снапшотов цен
func getKeysFromPriceSnapMap(m map[int]map[int]models.PriceSnapshot) []int {
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

// getKeysFromStockSnapMap извлекает ключи из карты снапшотов остатков
func getKeysFromStockSnapMap(m map[int]map[int64]models.StockSnapshot) []int {
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
