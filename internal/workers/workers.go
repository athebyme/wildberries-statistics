package workers

import (
	"fmt"
	"sync"
	"wbmonitoring/monitoring/internal/models"
)

type SafeCursorManager struct {
	mu          sync.Mutex
	usedCursors map[string]bool
	lastCursor  models.Cursor
}

func NewSafeCursorManager() *SafeCursorManager {
	return &SafeCursorManager{
		usedCursors: make(map[string]bool),
	}
}

func (scm *SafeCursorManager) GetUniqueCursor(nmID int, updatedAt string) (models.Cursor, bool) {
	scm.mu.Lock()
	defer scm.mu.Unlock()

	cursorKey := fmt.Sprintf("%d_%s", nmID, updatedAt)

	if scm.usedCursors[cursorKey] {
		return models.Cursor{}, false
	}

	scm.usedCursors[cursorKey] = true
	cursor := models.Cursor{
		NmID:      nmID,
		UpdatedAt: updatedAt,
		Limit:     100,
	}
	scm.lastCursor = cursor

	return cursor, true
}
