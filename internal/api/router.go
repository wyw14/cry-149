package api

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/wyw14/cry-149/internal/service"
)

type Server struct {
	runtime *service.Runtime
	router  chi.Router
}

func New(runtime *service.Runtime) *Server {
	server := &Server{runtime: runtime}
	router := chi.NewRouter()
	router.Use(middleware.RequestID)
	router.Use(middleware.RealIP)
	router.Use(middleware.Recoverer)
	router.Use(middleware.Timeout(15 * time.Second))
	router.Get("/healthz", server.health)
	router.Route("/api", func(api chi.Router) {
		api.Get("/operations", server.operations)
		api.Get("/events", server.events)
		api.Get("/equipment", server.equipment)
		api.Get("/interlocks", server.interlocks)
		api.Post("/interlocks", server.setInterlock)
		api.Get("/incidents", server.incidents)
		api.Get("/batches", server.batches)
		api.Post("/batches", server.inoculate)
		api.Post("/batches/{batchID}/phase", server.advance)
		api.Post("/probes/readings", server.probeReading)
		api.Post("/probes/calibrations", server.probeCalibration)
		api.Post("/feed/pulses", server.feedPulse)
		api.Get("/feed/{vesselID}", server.feedStatus)
		api.Post("/offgas/import", server.offgasImport)
		api.Post("/oxygen/mode", server.oxygenMode)
		api.Get("/control/{batchID}", server.controlStatus)
		api.Post("/harvest/preview", server.harvestPreview)
		api.Post("/harvest/transfers", server.harvestTransfer)
		api.Post("/harvest/transfers/{transferID}/complete", server.harvestComplete)
		api.Get("/harvest/status", server.harvestStatus)
		api.Post("/cip/simulate", server.simulateCIP)
		api.Get("/cip/{vesselID}", server.cipStatus)
		api.Get("/utilities/{resource}", server.utilityStatus)
		api.Post("/checkpoint", server.checkpoint)
	})
	router.MethodNotAllowed(methodNotAllowed)
	server.router = router
	return server
}

func (s *Server) Handler() http.Handler {
	return s.router
}

func (s *Server) health(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, s.runtime.Health())
}
