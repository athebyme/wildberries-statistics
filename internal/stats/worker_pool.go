package stats

import (
	"context"
	"sync"
)

// Task представляет задачу для выполнения
type Task func() interface{}

// Result представляет результат выполнения задачи
type Result struct {
	Value interface{}
	Err   error
}

// WorkerPool представляет пул горутин для параллельного выполнения задач
type WorkerPool struct {
	taskChan chan Task
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
}

// NewWorkerPool создает новый пул воркеров указанного размера
func NewWorkerPool(size int) *WorkerPool {
	if size <= 0 {
		size = 1
	}

	ctx, cancel := context.WithCancel(context.Background())

	wp := &WorkerPool{
		taskChan: make(chan Task),
		ctx:      ctx,
		cancel:   cancel,
	}

	// Запускаем воркеров
	for i := 0; i < size; i++ {
		wp.wg.Add(1)
		go wp.worker()
	}

	return wp
}

// worker представляет горутину, обрабатывающую задачи
func (wp *WorkerPool) worker() {
	defer wp.wg.Done()

	for {
		select {
		case task, ok := <-wp.taskChan:
			if !ok {
				return
			}
			// Выполняем задачу и игнорируем результат, так как он будет получен через канал
			_ = task()
		case <-wp.ctx.Done():
			return
		}
	}
}

// Submit отправляет задачу на выполнение и возвращает канал для получения результата
func (wp *WorkerPool) Submit(task func() (interface{}, error)) <-chan Result {
	resultChan := make(chan Result, 1)

	// Оборачиваем задачу для передачи результата через канал
	wrappedTask := func() interface{} {
		value, err := task()
		resultChan <- Result{Value: value, Err: err}
		close(resultChan)
		return nil
	}

	select {
	case wp.taskChan <- wrappedTask:
		// Задача принята к выполнению
	case <-wp.ctx.Done():
		// Пул воркеров завершает работу
		close(resultChan)
	}

	return resultChan
}

// ExecuteBatch выполняет набор задач параллельно и возвращает результаты
func (wp *WorkerPool) ExecuteBatch(tasks []func() (interface{}, error)) []Result {
	resultChans := make([]<-chan Result, len(tasks))

	// Отправляем все задачи на выполнение
	for i, task := range tasks {
		resultChans[i] = wp.Submit(task)
	}

	// Собираем результаты
	results := make([]Result, len(tasks))
	for i, resultChan := range resultChans {
		results[i] = <-resultChan
	}

	return results
}

// Shutdown корректно завершает работу пула воркеров
func (wp *WorkerPool) Shutdown() {
	wp.cancel()
	close(wp.taskChan)
	wp.wg.Wait()
}
