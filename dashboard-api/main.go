package main

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "dashboard-api/satelliterms"
)

//go:embed static
var staticFiles embed.FS

var jwtSecret []byte

// ─────────────────────────────────────────────
// Prometheus metrics
// ─────────────────────────────────────────────

var (
	httpRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "rms_http_requests_total",
		Help: "Total HTTP requests by method, path, and status code.",
	}, []string{"method", "path", "status"})

	httpDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "rms_http_request_duration_seconds",
		Help:    "HTTP request latency by method and path.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "path"})

	grpcCalls = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "rms_grpc_client_calls_total",
		Help: "Total outbound gRPC calls from dashboard-api to CSS.",
	}, []string{"method", "result"})
)

// responseWriter wraps http.ResponseWriter to capture the status code written by handlers.
type responseWriter struct {
	http.ResponseWriter
	status int
}

func newResponseWriter(w http.ResponseWriter) *responseWriter {
	return &responseWriter{ResponseWriter: w, status: http.StatusOK}
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}

// metricsMiddleware records request count and latency for every route.
// It wraps the full middleware chain so the captured status code is accurate
// regardless of which layer (cors, jwt, handler) sets it.
func metricsMiddleware(path string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rw := newResponseWriter(w)                                                      // capture status code
		timer := prometheus.NewTimer(httpDuration.WithLabelValues(r.Method, path))      // start latency timer
		next(rw, r)                                                                     // run the real handler
		timer.ObserveDuration()                                                         // record latency
		httpRequests.WithLabelValues(r.Method, path, strconv.Itoa(rw.status)).Inc()     // ← increment counter HERE
	}
}

// ─────────────────────────────────────────────
// Claims & CORS
// ─────────────────────────────────────────────

type Claims struct {
	OperatorID  string `json:"operator_id"`
	GatewayZone string `json:"gateway_zone"`
	jwt.RegisteredClaims
}

type claimsKey struct{}

func corsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next(w, r)
	}
}

// ─────────────────────────────────────────────
// Per-operator rate limiter
// ─────────────────────────────────────────────

type rateLimiter struct {
	mu      sync.Mutex
	counts  map[string]int
	windows map[string]time.Time
	limit   int
}

func newRateLimiter(limit int) *rateLimiter {
	return &rateLimiter{
		counts:  make(map[string]int),
		windows: make(map[string]time.Time),
		limit:   limit,
	}
}

func (rl *rateLimiter) allow(operatorID string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	now := time.Now()
	if win, ok := rl.windows[operatorID]; !ok || now.After(win.Add(time.Minute)) {
		rl.windows[operatorID] = now
		rl.counts[operatorID] = 1
		return true
	}
	rl.counts[operatorID]++
	return rl.counts[operatorID] <= rl.limit
}

// ─────────────────────────────────────────────
// Server
// ─────────────────────────────────────────────

type server struct {
	css pb.ConstellationStateServiceClient
	rl  *rateLimiter
}

func (s *server) jwtMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if !strings.HasPrefix(authHeader, "Bearer ") {
			writeJSON(w, http.StatusUnauthorized,
				map[string]string{"error": "missing Authorization: Bearer <token>"})
			return
		}
		tokenStr := strings.TrimPrefix(authHeader, "Bearer ")
		claims := &Claims{}
		token, err := jwt.ParseWithClaims(tokenStr, claims, func(t *jwt.Token) (interface{}, error) {
			if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
			}
			return jwtSecret, nil
		})
		if err != nil || !token.Valid {
			writeJSON(w, http.StatusUnauthorized,
				map[string]string{"error": "invalid or expired token"})
			return
		}
		ctx := context.WithValue(r.Context(), claimsKey{}, claims)
		next(w, r.WithContext(ctx))
	}
}

func claimsFrom(r *http.Request) *Claims {
	c, _ := r.Context().Value(claimsKey{}).(*Claims)
	return c
}

func inZone(angle float64, zone string) bool {
	switch zone {
	case "GW-NORTH":
		return angle >= 0 && angle < 120
	case "GW-CENTRAL":
		return angle >= 120 && angle < 240
	case "GW-SOUTH":
		return angle >= 240 && angle < 360
	}
	return false
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// ─────────────────────────────────────────────
// Route handlers — gRPC calls tracked via grpcCalls counter
// ─────────────────────────────────────────────

func (s *server) handlePing(w http.ResponseWriter, r *http.Request) {
	c := claimsFrom(r)
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	resp, err := s.css.GetPing(ctx, &pb.PingRequest{})
	if err != nil {
		grpcCalls.WithLabelValues("GetPing", "error").Inc()
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "CSS unavailable"})
		return
	}
	grpcCalls.WithLabelValues("GetPing", "ok").Inc()
	fmt.Printf("[Dashboard] %s | GET /api/ping → %s\n", c.OperatorID, resp.Status)
	writeJSON(w, http.StatusOK, map[string]string{
		"css_status": resp.Status,
		"operator":   c.OperatorID,
		"zone":       c.GatewayZone,
	})
}

func (s *server) handleGetSatellites(w http.ResponseWriter, r *http.Request) {
	c := claimsFrom(r)
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	resp, err := s.css.GetAllEphemeris(ctx, &pb.EphemerisRequest{})
	if err != nil {
		grpcCalls.WithLabelValues("GetAllEphemeris", "error").Inc()
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "CSS error"})
		return
	}
	grpcCalls.WithLabelValues("GetAllEphemeris", "ok").Inc()
	var filtered []*pb.Satellite
	for _, sat := range resp.Satellites {
		if inZone(sat.OrbitalAngle, c.GatewayZone) {
			filtered = append(filtered, sat)
		}
	}
	fmt.Printf("[Dashboard] %s | GET /api/satellites → %d/%d sats (zone=%s)\n",
		c.OperatorID, len(filtered), len(resp.Satellites), c.GatewayZone)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"operator":    c.OperatorID,
		"zone":        c.GatewayZone,
		"version":     resp.Version,
		"timestamp":   resp.Timestamp,
		"total_count": len(filtered),
		"satellites":  filtered,
	})
}

func (s *server) handleGetSatellite(w http.ResponseWriter, r *http.Request) {
	c := claimsFrom(r)
	satID := strings.TrimPrefix(r.URL.Path, "/api/satellites/")
	if satID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "satellite_id required"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	resp, err := s.css.GetSatellite(ctx, &pb.SatelliteRequest{SatelliteId: satID})
	if err != nil {
		grpcCalls.WithLabelValues("GetSatellite", "error").Inc()
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "satellite not found"})
		return
	}
	grpcCalls.WithLabelValues("GetSatellite", "ok").Inc()
	if !inZone(resp.Satellite.OrbitalAngle, c.GatewayZone) {
		writeJSON(w, http.StatusForbidden, map[string]string{
			"error": fmt.Sprintf("%s is not in your zone (%s)", satID, c.GatewayZone),
		})
		return
	}
	fmt.Printf("[Dashboard] %s | GET /api/satellites/%s\n", c.OperatorID, satID)
	writeJSON(w, http.StatusOK, resp)
}

func (s *server) handleGetHistory(w http.ResponseWriter, r *http.Request) {
	c := claimsFrom(r)
	withoutBase := strings.TrimPrefix(r.URL.Path, "/api/satellites/")
	satID := strings.TrimSuffix(withoutBase, "/history")
	if satID == "" || satID == withoutBase {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid path"})
		return
	}
	ctx1, cancel1 := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel1()
	satResp, err := s.css.GetSatellite(ctx1, &pb.SatelliteRequest{SatelliteId: satID})
	if err != nil || !inZone(satResp.Satellite.OrbitalAngle, c.GatewayZone) {
		grpcCalls.WithLabelValues("GetSatellite", "error").Inc()
		writeJSON(w, http.StatusForbidden, map[string]string{
			"error": fmt.Sprintf("%s is not in your zone (%s)", satID, c.GatewayZone),
		})
		return
	}
	grpcCalls.WithLabelValues("GetSatellite", "ok").Inc()
	ctx2, cancel2 := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel2()
	resp, err := s.css.GetSatelliteHistory(ctx2, &pb.HistoryRequest{SatelliteId: satID})
	if err != nil {
		grpcCalls.WithLabelValues("GetSatelliteHistory", "error").Inc()
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "history not found"})
		return
	}
	grpcCalls.WithLabelValues("GetSatelliteHistory", "ok").Inc()
	fmt.Printf("[Dashboard] %s | GET /api/satellites/%s/history → %d records\n",
		c.OperatorID, satID, resp.RecordCount)
	writeJSON(w, http.StatusOK, resp)
}

func (s *server) handleGetSchedules(w http.ResponseWriter, r *http.Request) {
	c := claimsFrom(r)
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	resp, err := s.css.GetSchedules(ctx, &pb.ScheduleListRequest{})
	if err != nil {
		grpcCalls.WithLabelValues("GetSchedules", "error").Inc()
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "CSS error"})
		return
	}
	grpcCalls.WithLabelValues("GetSchedules", "ok").Inc()
	type zoneSchedule struct {
		Run             int32            `json:"run"`
		Timestamp       string           `json:"timestamp"`
		ZoneAssignments []*pb.Assignment `json:"zone_assignments"`
	}
	out := make([]zoneSchedule, 0, len(resp.Schedules))
	for _, sched := range resp.Schedules {
		var za []*pb.Assignment
		for _, a := range sched.Assignments {
			if a.Gateway == c.GatewayZone {
				za = append(za, a)
			}
		}
		out = append(out, zoneSchedule{Run: sched.Run, Timestamp: sched.Timestamp, ZoneAssignments: za})
	}
	fmt.Printf("[Dashboard] %s | GET /api/schedules → %d runs (zone=%s)\n",
		c.OperatorID, len(out), c.GatewayZone)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"operator":   c.OperatorID,
		"zone":       c.GatewayZone,
		"total_runs": len(out),
		"schedules":  out,
	})
}

func (s *server) handleSubmitSchedule(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "use POST"})
		return
	}
	c := claimsFrom(r)
	if !s.rl.allow(c.OperatorID) {
		fmt.Printf("[Dashboard] RATE LIMITED: %s exceeded 10 submissions/min\n", c.OperatorID)
		w.Header().Set("Retry-After", "60")
		writeJSON(w, http.StatusTooManyRequests, map[string]string{
			"error": "rate limit exceeded — max 10 schedule submissions per operator per minute",
		})
		return
	}
	var payload pb.SchedulePayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	ack, err := s.css.ReceiveSchedule(ctx, &payload)
	if err != nil {
		grpcCalls.WithLabelValues("ReceiveSchedule", "error").Inc()
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "CSS error"})
		return
	}
	grpcCalls.WithLabelValues("ReceiveSchedule", "ok").Inc()
	fmt.Printf("[Dashboard] %s | POST /api/schedules/submit → run #%d ack: %s\n",
		c.OperatorID, ack.Run, ack.Message)
	writeJSON(w, http.StatusOK, ack)
}

func (s *server) handleSatellitesRouter(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	if path == "/api/satellites" || path == "/api/satellites/" {
		s.handleGetSatellites(w, r)
		return
	}
	if strings.HasSuffix(path, "/history") {
		s.handleGetHistory(w, r)
		return
	}
	s.handleGetSatellite(w, r)
}

func main() {
	secret := os.Getenv("JWT_SECRET")
	if secret == "" {
		secret = "satellite-rms-secret-2024"
	}
	jwtSecret = []byte(secret)

	cssAddr := os.Getenv("CSS_ADDR")
	if cssAddr == "" {
		cssAddr = "css-service:50051"
	}

	fmt.Println("============================================================")
	fmt.Println("[Dashboard API] HTTP→gRPC bridge | JWT auth | Zone filtering")
	fmt.Printf("[Dashboard API] CSS backend : %s\n", cssAddr)
	fmt.Println("[Dashboard API] Rate limit  : 10 schedule submissions/operator/min")
	fmt.Println("[Dashboard API] Static UI   : embedded, served at /")
	fmt.Println("[Dashboard API] Prometheus  : metrics at /metrics")
	fmt.Println("[Dashboard API] Listening on :8080")
	fmt.Println("============================================================")

	conn, err := grpc.NewClient(cssAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "gRPC dial failed: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	srv := &server{
		css: pb.NewConstellationStateServiceClient(conn),
		rl:  newRateLimiter(10),
	}

	mux := http.NewServeMux()

	// API routes — metrics → cors → jwt → handler (outermost captures final status code)
	mux.HandleFunc("/api/ping",
		metricsMiddleware("/api/ping", corsMiddleware(srv.jwtMiddleware(srv.handlePing))))
	mux.HandleFunc("/api/satellites",
		metricsMiddleware("/api/satellites", corsMiddleware(srv.jwtMiddleware(srv.handleSatellitesRouter))))
	mux.HandleFunc("/api/satellites/",
		metricsMiddleware("/api/satellites", corsMiddleware(srv.jwtMiddleware(srv.handleSatellitesRouter))))
	mux.HandleFunc("/api/schedules/submit",
		metricsMiddleware("/api/schedules/submit", corsMiddleware(srv.jwtMiddleware(srv.handleSubmitSchedule))))
	mux.HandleFunc("/api/schedules",
		metricsMiddleware("/api/schedules", corsMiddleware(srv.jwtMiddleware(srv.handleGetSchedules))))

	// Health endpoint — no auth, not tracked in business metrics
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"ok"}`))
	})

	// Prometheus metrics endpoint
	mux.Handle("/metrics", promhttp.Handler())

	// Static browser UI — catch-all; API routes take priority via longest-prefix matching
	sub, err := fs.Sub(staticFiles, "static")
	if err != nil {
		log.Fatal(err)
	}
	mux.Handle("/", http.FileServer(http.FS(sub)))

	log.Fatal(http.ListenAndServe(":8080", mux))
}
