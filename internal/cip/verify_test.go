package cip_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/wyw14/cry-149/internal/api"
	"github.com/wyw14/cry-149/internal/service"
)

func TestCIPFailureRetainsStepIdentityThroughAPI(t *testing.T) {
	runtime, err := service.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	handler := api.New(runtime).Handler()
	body := bytes.NewBufferString(`{"plan_id":"CIP-7","vessel_id":"FV-101","fail_step":"rinse"}`)
	request := httptest.NewRequest(http.MethodPost, "/api/cip/simulate", body)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusConflict {
		t.Fatalf("status %d: %s", response.Code, response.Body.String())
	}
	message := response.Body.String()
	if !strings.Contains(message, "rinse") || !strings.Contains(message, "rinse valve timeout") {
		t.Fatalf("step identity was lost from API response: %s", message)
	}
}
