package handler

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/kadriyebarlak/car-command-dispatcher/internal/domain"
	"github.com/kadriyebarlak/car-command-dispatcher/internal/service"
)

type CommandHandler struct {
	service *service.CommandService
	logger  *slog.Logger
}

func NewCommandHandler(service *service.CommandService, logger *slog.Logger) *CommandHandler {
	return &CommandHandler{
		service: service,
		logger:  logger,
	}
}

type createCommandRequest struct {
	CarID   string `json:"car_id"`
	Type    string `json:"type"`
	Payload string `json:"payload"`
}

func (req createCommandRequest) validate() []string {
	var errs []string

	if req.CarID == "" {
		errs = append(errs, "car_id is required")
	}

	if req.Type == "" {
		errs = append(errs, "type is required")
	}

	return errs
}

type createCommandResponse struct {
	ID     string `json:"id"`
	CarID  string `json:"car_id"`
	Type   string `json:"type"`
	Status string `json:"status"`
}

func (h *CommandHandler) CreateCommand(w http.ResponseWriter, r *http.Request) {
	var req createCommandRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.logger.Warn("invalid request body", "error", err)
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if errs := req.validate(); len(errs) > 0 {
		h.logger.Warn("command validation failed", "validation_errors", errs)
		writeJSON(w, http.StatusBadRequest, map[string][]string{
			"errors": errs,
		})
		return
	}

	command, err := h.service.Submit(
		r.Context(),
		req.CarID,
		domain.CommandType(req.Type),
		req.Payload,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to submit command")
		return
	}

	writeJSON(w, http.StatusAccepted, createCommandResponse{
		ID:     command.ID,
		CarID:  command.CarID,
		Type:   string(command.Type),
		Status: string(command.Status),
	})
}
