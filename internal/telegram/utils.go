package telegram

// Вспомогательная функция для вычисления модуля числа
func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

// Вспомогательная функция для определения максимального значения
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Вспомогательная функция для определения минимального значения
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
