package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type mockChaos struct {
	killCalled    bool
	floodCalled   bool
	latencyCalled bool
}

func (m *mockChaos) HandleKillWorker(w http.ResponseWriter, r *http.Request) {
	m.killCalled = true
	w.WriteHeader(http.StatusOK)
}

func (m *mockChaos) HandleFlood(w http.ResponseWriter, r *http.Request) {
	m.floodCalled = true
	w.WriteHeader(http.StatusOK)
}

func (m *mockChaos) HandleLatency(w http.ResponseWriter, r *http.Request) {
	m.latencyCalled = true
	w.WriteHeader(http.StatusOK)
}

func TestSetupRouter_ChaosGating(t *testing.T) {
	handler := &Handler{}
	events := NewEventBus()

	t.Run("chaos disabled returns 404", func(t *testing.T) {
		chaos := &mockChaos{}
		router := SetupRouter(handler, events, chaos, nil, false)

		endpoints := []string{
			"/api/v1/chaos/kill-worker",
			"/api/v1/chaos/flood",
			"/api/v1/chaos/latency",
		}

		for _, ep := range endpoints {
			req := httptest.NewRequest(http.MethodPost, ep, strings.NewReader("{}"))
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			if w.Code != http.StatusNotFound {
				t.Errorf("expected 404 for %s when chaos disabled, got %d", ep, w.Code)
			}
			if !strings.Contains(w.Body.String(), "chaos endpoints are disabled") {
				t.Errorf("expected error message in body for %s, got: %s", ep, w.Body.String())
			}
		}

		if chaos.killCalled || chaos.floodCalled || chaos.latencyCalled {
			t.Error("expected no chaos handlers to be invoked when disabled")
		}
	})

	t.Run("chaos enabled routes to handlers", func(t *testing.T) {
		chaos := &mockChaos{}
		router := SetupRouter(handler, events, chaos, nil, true)

		// Test kill-worker
		req := httptest.NewRequest(http.MethodPost, "/api/v1/chaos/kill-worker", strings.NewReader("{}"))
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != http.StatusOK || !chaos.killCalled {
			t.Errorf("expected 200 and killCalled=true, got %d and %v", w.Code, chaos.killCalled)
		}

		// Test flood
		req = httptest.NewRequest(http.MethodPost, "/api/v1/chaos/flood", strings.NewReader("{}"))
		w = httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != http.StatusOK || !chaos.floodCalled {
			t.Errorf("expected 200 and floodCalled=true, got %d and %v", w.Code, chaos.floodCalled)
		}

		// Test latency
		req = httptest.NewRequest(http.MethodPost, "/api/v1/chaos/latency", strings.NewReader("{}"))
		w = httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != http.StatusOK || !chaos.latencyCalled {
			t.Errorf("expected 200 and latencyCalled=true, got %d and %v", w.Code, chaos.latencyCalled)
		}
	})
}
