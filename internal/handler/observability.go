package handler

import (
	"net/http"

	"bilibili-up-admin/internal/polling"

	"github.com/gin-gonic/gin"
)

// ObservabilityHandler 可观测性接口
type ObservabilityHandler struct {
	snapshot func() polling.Snapshot
}

func NewObservabilityHandler(snapshot func() polling.Snapshot) *ObservabilityHandler {
	return &ObservabilityHandler{snapshot: snapshot}
}

func (h *ObservabilityHandler) PollingStats(c *gin.Context) {
	if h.snapshot == nil {
		c.JSON(http.StatusOK, gin.H{
			"started":      false,
			"task_count":   0,
			"generated_at": nil,
			"tasks":        []any{},
		})
		return
	}

	snapshot := h.snapshot()
	if snapshot.GeneratedAt.IsZero() && len(snapshot.Tasks) == 0 && !snapshot.Started {
		c.JSON(http.StatusOK, gin.H{
			"started":      false,
			"task_count":   0,
			"generated_at": nil,
			"tasks":        []any{},
		})
		return
	}

	c.JSON(http.StatusOK, snapshot)
}
