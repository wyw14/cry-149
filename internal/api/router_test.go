package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/wyw14/cry-149/internal/service"
)

func TestOperationalRoutesRespond(t *testing.T) {
	runtime, err := service.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	handler := New(runtime).Handler()
	paths := []string{"/healthz", "/api/operations", "/api/equipment", "/api/interlocks", "/api/incidents"}
	for _, path := range paths {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("%s returned %d: %s", path, response.Code, response.Body.String())
		}
		if response.Header().Get("Content-Type") != "application/json" {
			t.Fatalf("%s did not return JSON", path)
		}
	}
}
