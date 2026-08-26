package api

import (
	"context"
	"errors"
	"net/http"
)

func (s *Server) operations(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, s.runtime.Operations())
}

func (s *Server) events(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, s.runtime.Events())
}

func (s *Server) equipment(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, s.runtime.Equipment())
}

func (s *Server) interlocks(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, s.runtime.Interlocks())
}

func (s *Server) incidents(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, s.runtime.Incidents())
}

func (s *Server) batches(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, s.runtime.Batches())
}

func statusFor(err error) int {
	if err == nil {
		return http.StatusOK
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return http.StatusRequestTimeout
	}
	return http.StatusConflict
}
