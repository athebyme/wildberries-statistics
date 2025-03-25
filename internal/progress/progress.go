package progress

import (
	"fmt"
	"sync"
	"time"
)

// Tracker предоставляет функциональность отслеживания прогресса операций
type Tracker struct {
	mu           sync.RWMutex
	operations   map[string]*OperationProgress
	messageLimit int // Максимальное количество сообщений для хранения
}

// OperationProgress содержит информацию о прогрессе операции
type OperationProgress struct {
	ID               string
	Name             string
	StartTime        time.Time
	LastUpdateTime   time.Time
	TotalItems       int
	ProcessedItems   int
	CompletedItems   int
	FailedItems      int
	EstimatedEndTime time.Time
	Messages         []ProgressMessage
	IsComplete       bool
	Error            string
}

// ProgressMessage содержит информационное сообщение о выполнении операции
type ProgressMessage struct {
	Time    time.Time
	Level   string // "info", "warning", "error"
	Message string
}

// NewProgressTracker создает новый трекер прогресса
func NewProgressTracker(messageLimit int) *Tracker {
	if messageLimit <= 0 {
		messageLimit = 100
	}

	return &Tracker{
		operations:   make(map[string]*OperationProgress),
		messageLimit: messageLimit,
	}
}

// StartOperation начинает отслеживание новой операции
func (pt *Tracker) StartOperation(id, name string, totalItems int) *OperationProgress {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	now := time.Now()

	op := &OperationProgress{
		ID:             id,
		Name:           name,
		StartTime:      now,
		LastUpdateTime: now,
		TotalItems:     totalItems,
		Messages:       make([]ProgressMessage, 0, pt.messageLimit),
	}

	// Добавляем начальное сообщение
	op.Messages = append(op.Messages, ProgressMessage{
		Time:    now,
		Level:   "info",
		Message: fmt.Sprintf("Операция '%s' начата. Всего элементов: %d", name, totalItems),
	})

	pt.operations[id] = op
	return op
}

// UpdateProgress обновляет прогресс выполнения операции
func (pt *Tracker) UpdateProgress(id string, processed, completed, failed int, message string) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	op, exists := pt.operations[id]
	if !exists {
		return
	}

	now := time.Now()
	op.ProcessedItems = processed
	op.CompletedItems = completed
	op.FailedItems = failed
	op.LastUpdateTime = now

	// Оценка времени завершения на основе текущей скорости
	if processed > 0 && op.TotalItems > 0 {
		elapsedTime := now.Sub(op.StartTime)
		itemsLeft := op.TotalItems - processed

		// Скорость обработки (элементов в секунду)
		speed := float64(processed) / elapsedTime.Seconds()

		// Оценка оставшегося времени
		if speed > 0 {
			remainingSeconds := float64(itemsLeft) / speed
			op.EstimatedEndTime = now.Add(time.Duration(remainingSeconds) * time.Second)
		}
	}

	// Добавляем сообщение о прогрессе, если оно предоставлено
	if message != "" {
		pt.addMessage(op, "info", message)
	}
}

// AddWarning добавляет предупреждение к операции
func (pt *Tracker) AddWarning(id string, message string) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	op, exists := pt.operations[id]
	if !exists {
		return
	}

	pt.addMessage(op, "warning", message)
}

// AddError добавляет сообщение об ошибке к операции
func (pt *Tracker) AddError(id string, message string) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	op, exists := pt.operations[id]
	if !exists {
		return
	}

	pt.addMessage(op, "error", message)
}

// CompleteOperation помечает операцию как завершенную
func (pt *Tracker) CompleteOperation(id string, withError string) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	op, exists := pt.operations[id]
	if !exists {
		return
	}

	now := time.Now()
	op.IsComplete = true
	op.LastUpdateTime = now

	if withError != "" {
		op.Error = withError
		pt.addMessage(op, "error", fmt.Sprintf("Операция завершена с ошибкой: %s", withError))
	} else {
		duration := now.Sub(op.StartTime).Round(time.Second)
		message := fmt.Sprintf("Операция успешно завершена за %s. Обработано: %d, Успешно: %d, С ошибками: %d",
			duration, op.ProcessedItems, op.CompletedItems, op.FailedItems)
		pt.addMessage(op, "info", message)
	}
}

// GetOperation возвращает информацию о прогрессе операции
func (pt *Tracker) GetOperation(id string) *OperationProgress {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	op, exists := pt.operations[id]
	if !exists {
		return nil
	}

	// Создаем копию, чтобы избежать проблем с конкурентным доступом
	opCopy := *op
	opCopy.Messages = make([]ProgressMessage, len(op.Messages))
	copy(opCopy.Messages, op.Messages)

	return &opCopy
}

// GetAllOperations возвращает список всех отслеживаемых операций
func (pt *Tracker) GetAllOperations() []*OperationProgress {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	ops := make([]*OperationProgress, 0, len(pt.operations))

	for _, op := range pt.operations {
		// Создаем копию для безопасности
		opCopy := *op
		opCopy.Messages = make([]ProgressMessage, len(op.Messages))
		copy(opCopy.Messages, op.Messages)

		ops = append(ops, &opCopy)
	}

	return ops
}

// CleanupCompletedOperations удаляет завершенные операции старше указанного времени
func (pt *Tracker) CleanupCompletedOperations(olderThan time.Duration) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	cutoffTime := time.Now().Add(-olderThan)

	for id, op := range pt.operations {
		if op.IsComplete && op.LastUpdateTime.Before(cutoffTime) {
			delete(pt.operations, id)
		}
	}
}

// addMessage добавляет сообщение к операции с ограничением на количество сообщений
func (pt *Tracker) addMessage(op *OperationProgress, level, message string) {
	// Если достигнут лимит сообщений, удаляем самое старое
	if len(op.Messages) >= pt.messageLimit {
		op.Messages = op.Messages[1:]
	}

	op.Messages = append(op.Messages, ProgressMessage{
		Time:    time.Now(),
		Level:   level,
		Message: message,
	})
}
