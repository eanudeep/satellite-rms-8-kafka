package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var jwtSecret []byte

// ─────────────────────────────────────────────
// Prometheus metrics
// ─────────────────────────────────────────────

var (
	loginAttempts = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "rms_auth_login_attempts_total",
		Help: "Total login attempts split by result (success / bad_credentials / bad_request).",
	}, []string{"result"})
)

// ─────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────

type operatorRecord struct {
	password    string
	gatewayZone string
}

var operators = map[string]operatorRecord{
	"alice": {password: "alice123", gatewayZone: "GW-NORTH"},
	"bob":   {password: "bob123", gatewayZone: "GW-SOUTH"},
}

type Claims struct {
	OperatorID  string `json:"operator_id"`
	GatewayZone string `json:"gateway_zone"`
	jwt.RegisteredClaims
}

type loginReq struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type loginResp struct {
	Token       string `json:"token"`
	OperatorID  string `json:"operator_id"`
	GatewayZone string `json:"gateway_zone"`
}

// ─────────────────────────────────────────────
// Middleware & handlers
// ─────────────────────────────────────────────

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

// POST /auth/login — validates credentials, issues JWT, records outcome in loginAttempts counter
func handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	var req loginReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		loginAttempts.WithLabelValues("bad_request").Inc()
		http.Error(w, `{"error":"bad request body"}`, http.StatusBadRequest)
		return
	}
	op, ok := operators[req.Username]
	if !ok || op.password != req.Password {
		loginAttempts.WithLabelValues("bad_credentials").Inc()
		http.Error(w, `{"error":"invalid credentials"}`, http.StatusUnauthorized)
		return
	}
	claims := Claims{
		OperatorID:  req.Username,
		GatewayZone: op.gatewayZone,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			Issuer:    "satellite-rms",
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString(jwtSecret)
	if err != nil {
		loginAttempts.WithLabelValues("signing_error").Inc()
		http.Error(w, `{"error":"token signing failed"}`, http.StatusInternalServerError)
		return
	}
	loginAttempts.WithLabelValues("success").Inc()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(loginResp{
		Token:       signed,
		OperatorID:  req.Username,
		GatewayZone: op.gatewayZone,
	})
	fmt.Printf("[Auth] Login OK: operator=%s zone=%s\n", req.Username, op.gatewayZone)
}

func main() {
	secret := os.Getenv("JWT_SECRET")
	if secret == "" {
		secret = "satellite-rms-secret-2024"
	}
	jwtSecret = []byte(secret)

	http.HandleFunc("/auth/login", corsMiddleware(handleLogin))
	http.HandleFunc("/auth/health", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"ok"}`))
	}))
	http.Handle("/metrics", promhttp.Handler())

	fmt.Println("[Auth Service] Operators: alice→GW-NORTH  bob→GW-SOUTH")
	fmt.Println("[Auth Service] Prometheus : metrics at /metrics")
	fmt.Println("[Auth Service] Listening on :8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}
