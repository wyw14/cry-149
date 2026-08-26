package api

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/wyw14/cry-149/internal/model"
	"github.com/wyw14/cry-149/internal/offgas"
	"github.com/wyw14/cry-149/internal/oxygen"
	"github.com/wyw14/cry-149/internal/vessel"
)

func (s *Server) feedStatus(writer http.ResponseWriter, request *http.Request) {
	writeJSON(writer, http.StatusOK, s.runtime.FeedStatus(chi.URLParam(request, "vesselID")))
}

type offgasRequest struct {
	Records []offgas.Record `json:"records"`
}

func (s *Server) offgasImport(writer http.ResponseWriter, request *http.Request) {
	var input offgasRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", err)
		return
	}
	for index := range input.Records {
		if input.Records[index].At.IsZero() {
			input.Records[index].At = time.Now()
		}
	}
	operation, count, err := s.runtime.ImportOffgasRecords(input.Records)
	if err != nil {
		writeError(writer, http.StatusConflict, "offgas_import_failed", err)
		return
	}
	writeJSON(writer, http.StatusAccepted, map[string]any{"operation": operation, "records": count})
}

type oxygenModeRequest struct {
	BatchID   string      `json:"batch_id"`
	Mode      oxygen.Mode `json:"mode"`
	FeedRate  float64     `json:"feed_rate"`
	Agitation float64     `json:"agitation"`
}

func (s *Server) oxygenMode(writer http.ResponseWriter, request *http.Request) {
	var input oxygenModeRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", err)
		return
	}
	output, err := s.runtime.SetOxygenMode(input.BatchID, input.Mode, oxygen.Output{FeedRate: input.FeedRate, Agitation: input.Agitation})
	if err != nil {
		writeError(writer, http.StatusConflict, "oxygen_mode_failed", err)
		return
	}
	writeJSON(writer, http.StatusOK, output)
}

func (s *Server) controlStatus(writer http.ResponseWriter, request *http.Request) {
	writeJSON(writer, http.StatusOK, s.runtime.ControlStatus(chi.URLParam(request, "batchID"), request.URL.Query().Get("probe_id")))
}

type segmentRequest struct {
	ID   string `json:"id"`
	From string `json:"from"`
	To   string `json:"to"`
}

type harvestRequest struct {
	RouteID  string           `json:"route_id"`
	BatchID  string           `json:"batch_id"`
	VesselID string           `json:"vessel_id"`
	Segments []segmentRequest `json:"segments"`
	Blocked  map[string]bool  `json:"blocked"`
}

func routeFromRequest(input harvestRequest) (vessel.Route, error) {
	segments := make([]vessel.Segment, 0, len(input.Segments))
	for _, segment := range input.Segments {
		segments = append(segments, vessel.Segment{ID: segment.ID, From: segment.From, To: segment.To, Open: true})
	}
	return vessel.NewRoute(input.RouteID, input.BatchID, segments)
}

func (s *Server) harvestPreview(writer http.ResponseWriter, request *http.Request) {
	var input harvestRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", err)
		return
	}
	route, err := routeFromRequest(input)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_route", err)
		return
	}
	preview, available := s.runtime.PreviewHarvestRoute(route, input.Blocked)
	writeJSON(writer, http.StatusOK, map[string]any{"route": preview, "available": available})
}

func (s *Server) harvestTransfer(writer http.ResponseWriter, request *http.Request) {
	var input harvestRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", err)
		return
	}
	route, err := routeFromRequest(input)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_route", err)
		return
	}
	transfer, err := s.runtime.StartHarvest(request.Context(), input.BatchID, input.VesselID, route)
	if err != nil {
		writeError(writer, http.StatusConflict, "harvest_failed", err)
		return
	}
	writeJSON(writer, http.StatusAccepted, transfer)
}

func (s *Server) harvestComplete(writer http.ResponseWriter, request *http.Request) {
	if !s.runtime.CompleteHarvest(chi.URLParam(request, "transferID")) {
		writeError(writer, http.StatusNotFound, "transfer_not_found", http.ErrMissingFile)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]bool{"complete": true})
}

func (s *Server) harvestStatus(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, s.runtime.HarvestStatus())
}

func (s *Server) cipStatus(writer http.ResponseWriter, request *http.Request) {
	value, ok := s.runtime.CIPStatus(chi.URLParam(request, "vesselID"))
	if !ok {
		writeError(writer, http.StatusNotFound, "cip_not_found", http.ErrMissingFile)
		return
	}
	writeJSON(writer, http.StatusOK, value)
}

func (s *Server) utilityStatus(writer http.ResponseWriter, request *http.Request) {
	writeJSON(writer, http.StatusOK, s.runtime.UtilityStatus(chi.URLParam(request, "resource")))
}

type interlockRequest struct {
	Resource string `json:"resource"`
	Owner    string `json:"owner"`
	Reason   string `json:"reason"`
	Active   bool   `json:"active"`
}

func (s *Server) setInterlock(writer http.ResponseWriter, request *http.Request) {
	var input interlockRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", err)
		return
	}
	writeJSON(writer, http.StatusOK, s.runtime.SetInterlock(input.Resource, input.Owner, input.Reason, input.Active))
}

func (s *Server) checkpoint(writer http.ResponseWriter, _ *http.Request) {
	if err := s.runtime.Checkpoint(); err != nil {
		writeError(writer, http.StatusConflict, "checkpoint_failed", err)
		return
	}
	writeJSON(writer, http.StatusOK, model.NewOperation("runtime.checkpoint", "", time.Now()).WithState("complete", "saved", time.Now()))
}
