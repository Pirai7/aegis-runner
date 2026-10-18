package api

import (
	"io/fs"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
)

// ChaosInterface represents chaos handlers.
type ChaosInterface interface {
	HandleKillWorker(w http.ResponseWriter, r *http.Request)
	HandleFlood(w http.ResponseWriter, r *http.Request)
	HandleLatency(w http.ResponseWriter, r *http.Request)
}

// SetupRouter creates the Chi HTTP router.
func SetupRouter(handler *Handler, events *EventBus, chaos ChaosInterface, staticFS fs.FS) *chi.Mux {
	r := chi.NewRouter()

	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)

	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-Tenant-Id", "X-Idempotency-Key"},
		ExposedHeaders:   []string{"Link", "X-Total-Count"},
		AllowCredentials: false,
		MaxAge:           300,
	}))

	// Health probes
	r.Get("/healthz", handler.Healthz)
	r.Get("/readyz", handler.Readyz)

	// API v1
	r.Route("/api/v1", func(r chi.Router) {
		// Jobs
		r.Post("/jobs", handler.CreateJob)
		r.Post("/jobs/batch", handler.CreateBatchJobs)
		r.Get("/jobs", handler.ListJobs)
		r.Get("/jobs/{id}", handler.GetJob)

		// Tenants & Stats
		r.Get("/tenants", handler.ListTenants)
		r.Get("/stats", handler.GetStats)

		// Real-time Event Streaming
		r.Get("/stream/events", events.ServeSSE)

		// Chaos Controls
		r.Post("/chaos/kill-worker", chaos.HandleKillWorker)
		r.Post("/chaos/flood", chaos.HandleFlood)
		r.Post("/chaos/latency", chaos.HandleLatency)
	})

	// Static Frontend Serving
	if staticFS != nil {
		fileServer := http.FileServer(http.FS(staticFS))
		r.Handle("/*", fileServer)
	}

	return r
}
