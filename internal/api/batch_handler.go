package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/wyw14/cry-149/internal/cip"
	"github.com/wyw14/cry-149/internal/feed"
	"github.com/wyw14/cry-149/internal/model"
	"github.com/wyw14/cry-149/internal/probe"
)

type inoculateRequest struct {
	VesselID string `json:"vessel_id"`
	RecipeID string `json:"recipe_id"`
}

func (s *Server) inoculate(writer http.ResponseWriter, request *http.Request) {
	var input inoculateRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", err)
		return
	}
	operation, created, plan, err := s.runtime.Inoculate(input.VesselID, input.RecipeID)
	if err != nil {
		writeError(writer, http.StatusConflict, "inoculation_failed", err)
		return
	}
	writeJSON(writer, http.StatusCreated, map[string]any{
		"operation": operation, "batch": created, "plan": plan,
	})
}

type phaseRequest struct {
	Phase model.Phase `json:"phase"`
}

func (s *Server) advance(writer http.ResponseWriter, request *http.Request) {
	var input phaseRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", err)
		return
	}
	operation, updated, err := s.runtime.Advance(chi.URLParam(request, "batchID"), input.Phase)
	if err != nil {
		writeError(writer, http.StatusConflict, "phase_failed", err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"operation": operation, "batch": updated})
}

type probeRequest struct {
	ProbeID  string  `json:"probe_id"`
	BatchID  string  `json:"batch_id"`
	VesselID string  `json:"vessel_id"`
	Kind     string  `json:"kind"`
	Value    float64 `json:"value"`
}

func (s *Server) probeReading(writer http.ResponseWriter, request *http.Request) {
	var input probeRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", err)
		return
	}
	operation, output, err := s.runtime.ReceiveProbe(probe.Reading{
		ProbeID: input.ProbeID, BatchID: input.BatchID, VesselID: input.VesselID,
		Kind: input.Kind, Value: input.Value, At: time.Now(),
	})
	if err != nil {
		writeError(writer, http.StatusConflict, "probe_failed", err)
		return
	}
	writeJSON(writer, http.StatusAccepted, map[string]any{"operation": operation, "output": output})
}

type calibrationRequest struct {
	ProbeID string  `json:"probe_id"`
	Offset  float64 `json:"offset"`
	Slope   float64 `json:"slope"`
}

func (s *Server) probeCalibration(writer http.ResponseWriter, request *http.Request) {
	var input calibrationRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", err)
		return
	}
	delivered := s.runtime.CalibrateProbe(probe.Calibration{
		ProbeID: input.ProbeID, Offset: input.Offset, Slope: input.Slope, At: time.Now(),
	})
	writeJSON(writer, http.StatusAccepted, map[string]any{"delivered": delivered})
}

type feedRequest struct {
	BatchID  string  `json:"batch_id"`
	VesselID string  `json:"vessel_id"`
	AmountML float64 `json:"amount_ml"`
	ValidMS  int64   `json:"valid_ms"`
}

func (s *Server) feedPulse(writer http.ResponseWriter, request *http.Request) {
	var input feedRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", err)
		return
	}
	operation, err := s.runtime.ScheduleFeed(input.BatchID, input.VesselID, input.AmountML, time.Duration(input.ValidMS)*time.Millisecond)
	if err != nil {
		code := "feed_failed"
		if errors.Is(err, feed.ErrBackpressure) {
			code = "feed_backpressure"
		}
		writeError(writer, http.StatusTooManyRequests, code, err)
		return
	}
	writeJSON(writer, http.StatusAccepted, operation)
}

type cipRequest struct {
	PlanID   string `json:"plan_id"`
	VesselID string `json:"vessel_id"`
	FailStep string `json:"fail_step"`
}

var ErrRinseValveTimeout = errors.New("rinse valve timeout")

func (s *Server) simulateCIP(writer http.ResponseWriter, request *http.Request) {
	var input cipRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", err)
		return
	}
	names := []string{"open-return", "start-pump", "heat-caustic", "rinse", "stop-pump"}
	steps := make([]cip.Step, 0, len(names))
	for _, name := range names {
		stepName := name
		steps = append(steps, cip.Step{
			Name: stepName,
			Apply: func(context.Context) error {
				if input.FailStep == stepName {
					return fmt.Errorf("%s: %w", stepName, ErrRinseValveTimeout)
				}
				return nil
			},
			Compensate: func(context.Context) error { return nil },
		})
	}
	operation, err := s.runtime.RunCIP(request.Context(), cip.Plan{ID: input.PlanID, VesselID: input.VesselID, Steps: steps})
	if err != nil {
		writeError(writer, statusFor(err), "cleaning_failed", errors.New("cleaning failed"))
		return
	}
	writeJSON(writer, http.StatusAccepted, operation)
}
