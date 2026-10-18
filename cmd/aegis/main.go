package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"aegis/internal/api"
	"aegis/internal/chaos"
	"aegis/internal/db"
	"aegis/internal/reaper"
	"aegis/internal/scheduler"
	"aegis/internal/worker"
	"aegis/web"
)

func main() {
	log.Println("[AEGIS] Starting AEGIS Runner Engine...")

	// 1. Read configuration from environment
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5432/aegis?sslmode=disable"
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	workerConcurrency := 5
	if cStr := os.Getenv("WORKER_CONCURRENCY"); cStr != "" {
		if c, err := strconv.Atoi(cStr); err == nil && c > 0 {
			workerConcurrency = c
		}
	}

	reaperInterval := 5 * time.Second
	if rStr := os.Getenv("REAPER_INTERVAL_SEC"); rStr != "" {
		if s, err := strconv.Atoi(rStr); err == nil && s > 0 {
			reaperInterval = time.Duration(s) * time.Second
		}
	}

	reaperTTL := 15 * time.Second
	if tStr := os.Getenv("REAPER_TTL_SEC"); tStr != "" {
		if s, err := strconv.Atoi(tStr); err == nil && s > 0 {
			reaperTTL = time.Duration(s) * time.Second
		}
	}

	rootCtx, rootCancel := context.WithCancel(context.Background())
	defer rootCancel()

	// Start listening before database setup so liveness checks work while
	// PostgreSQL connects and schema migrations run.
	type handlerHolder struct{ handler http.Handler }
	var activeHandler atomic.Value
	activeHandler.Store(handlerHolder{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/healthz" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"starting","service":"aegis-runner"}`))
			return
		}
		if r.URL.Path == "/readyz" {
			http.Error(w, `{"status":"starting"}`, http.StatusServiceUnavailable)
			return
		}
		http.Error(w, `{"error":"service is starting"}`, http.StatusServiceUnavailable)
	})})
	server := &http.Server{
		Addr: ":" + port,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			activeHandler.Load().(handlerHolder).handler.ServeHTTP(w, r)
		}),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}
	go func() {
		log.Printf("[AEGIS] HTTP server listening on :%s", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[AEGIS] Server error: %v", err)
		}
	}()

	// Enable application routes only after database initialization and migrations.
	database, err := db.New(rootCtx, dbURL)
	if err != nil {
		log.Fatalf("[AEGIS] Fatal error connecting to database: %v", err)
	}
	defer database.Close()
	log.Println("[AEGIS] Connected to PostgreSQL; schema migrations applied.")

	// 3. Core Engine Components
	eventBus := api.NewEventBus()
	taskQueue := make(chan scheduler.JobTask, 100)

	drrScheduler := scheduler.New(database, eventBus, taskQueue)
	workerPool := worker.New(database, eventBus, taskQueue, workerConcurrency)
	heartbeatReaper := reaper.New(database, eventBus, reaperInterval, reaperTTL)
	chaosEngine := chaos.New(database, eventBus, workerPool)

	apiHandler := api.NewHandler(database, eventBus, workerPool, drrScheduler)

	// 4. Start background loops
	log.Printf("[AEGIS] Starting DRR scheduler, worker pool (%d routines), and heartbeat reaper (%ds TTL)...", workerConcurrency, int(reaperTTL.Seconds()))
	drrScheduler.Start(rootCtx)
	workerPool.Start(rootCtx)
	heartbeatReaper.Start(rootCtx)

	// 5. Setup Router with embedded web UI
	router := api.SetupRouter(apiHandler, eventBus, chaosEngine, web.DistFS)

	activeHandler.Store(handlerHolder{router})
	log.Println("[AEGIS] API is ready to accept traffic.")

	// 6. Graceful shutdown listener
	stopSignals := make(chan os.Signal, 1)
	signal.Notify(stopSignals, os.Interrupt, syscall.SIGTERM)

	sig := <-stopSignals
	log.Printf("[AEGIS] Received signal %v; initiating graceful shutdown...", sig)

	// Context for graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("[AEGIS] HTTP server shutdown error: %v", err)
	}

	drrScheduler.Stop()
	workerPool.Stop()
	heartbeatReaper.Stop()

	log.Println("[AEGIS] AEGIS Engine shutdown cleanly.")
}
