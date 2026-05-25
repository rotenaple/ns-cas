// CAS — Coordinated Allocation Server for NationStates API quota management.
// See cas_implementation_plan.md for full design documentation.
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/rotenaple/ns-cas/internal/dashboard"
	"github.com/rotenaple/ns-cas/internal/db"
	"github.com/rotenaple/ns-cas/internal/handlers"
	"github.com/rotenaple/ns-cas/internal/queue"
	"github.com/rotenaple/ns-cas/internal/state"
)

func main() {
	// ---- Configuration from environment ----
	port := envStr("CAS_PORT", "8080")
	dbPath := envStr("CAS_DB_PATH", "/data/cas.db")
	agingW := envFloat("CAS_AGING_WEIGHT", 1.0)
	maxQueue := envInt("CAS_MAX_QUEUE", 100)
	certFile := os.Getenv("CAS_CERT_FILE")
	keyFile := os.Getenv("CAS_KEY_FILE")

	log.Printf("[cas] starting — port=%s db=%s aging_weight=%.2f max_queue=%d",
		port, dbPath, agingW, maxQueue)

	// ---- Database ----
	database, err := db.Open(dbPath)
	if err != nil {
		log.Fatalf("[cas] database init failed: %v", err)
	}
	defer database.Close()

	// ---- State + Queue ----
	st := state.New()
	q := queue.New(agingW)

	// ---- Engine (launches background goroutines) ----
	eng := handlers.NewEngine(st, q, database, maxQueue)

	// ---- HTTP router ----
	mux := http.NewServeMux()
	handlers.RegisterRoutes(mux, eng)
	dashboard.RegisterRoutes(mux, database, eng, st)

	// Redirect / → /dashboard
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			http.Redirect(w, r, "/dashboard", http.StatusFound)
			return
		}
		http.NotFound(w, r)
	})

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%s", port),
		Handler:      corsMiddleware(loggingMiddleware(mux)),
		ReadTimeout:  40 * time.Second, // > 30s long-poll + overhead
		WriteTimeout: 40 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// ---- Graceful shutdown ----
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

	if certFile != "" && keyFile != "" {
		log.Printf("[cas] listening on :%s (TLS, cert=%s)", port, certFile)
		go func() {
			if err := srv.ListenAndServeTLS(certFile, keyFile); err != nil && err != http.ErrServerClosed {
				log.Fatalf("[cas] tls server error: %v", err)
			}
		}()
	} else {
		log.Printf("[cas] listening on :%s (plain HTTP)", port)
		go func() {
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Fatalf("[cas] server error: %v", err)
			}
		}()
	}

	<-stop
	log.Println("[cas] shutdown signal received")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("[cas] shutdown error: %v", err)
	}

	// Drain waiting long-polls.
	st.Lock()
	q.DrainAll()
	st.Unlock()

	log.Println("[cas] shutdown complete")
}

// ---- Helpers ----

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
		log.Printf("[cas] invalid %s=%q, using default %d", key, v, def)
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
		log.Printf("[cas] invalid %s=%q, using default %.2f", key, v, def)
	}
	return def
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, User-Agent")
		w.Header().Set("Access-Control-Max-Age", "86400")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		// Skip logging for dashboard auto-refresh noise
		if r.URL.Path != "/dashboard" && r.URL.Path != "/dashboard/" && r.URL.Path != "/healthz" {
			defer func() {
				log.Printf("[http] %s %s %s", r.Method, r.URL.Path, time.Since(start))
			}()
		}
		next.ServeHTTP(w, r)
	})
}
