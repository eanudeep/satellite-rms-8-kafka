package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "telemetry/satelliterms"
)

// ─────────────────────────────────────────────
// Prometheus metrics
// ─────────────────────────────────────────────

var (
	telemetryTicksTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "rms_telemetry_ticks_total",
		Help: "Total telemetry polling ticks completed.",
	})

	nominalSatellites = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "rms_telemetry_nominal_satellites",
		Help: "Number of NOMINAL satellites observed in the latest tick.",
	})

	degradedSatellites = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "rms_telemetry_degraded_satellites",
		Help: "Number of DEGRADED satellites observed in the latest tick.",
	})

	eclipseSatellites = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "rms_telemetry_eclipse_satellites",
		Help: "Number of satellites in eclipse in the latest tick.",
	})

	grpcCalls = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "rms_telemetry_grpc_calls_total",
		Help: "Total outbound gRPC calls made by the telemetry service.",
	}, []string{"method", "result"})
)

// startMetricsServer serves /metrics on port 9091. Telemetry is a pure gRPC client
// with no existing HTTP server, so this is the only HTTP listener it runs.
func startMetricsServer() {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	fmt.Println("[Telemetry] Prometheus metrics at :9091/metrics")
	if err := http.ListenAndServe(":9091", mux); err != nil {
		fmt.Fprintf(os.Stderr, "[Telemetry] metrics server error: %v\n", err)
	}
}

func main() {
	cssAddr := os.Getenv("CSS_ADDR")
	if cssAddr == "" {
		cssAddr = "css-service:50051"
	}

	fmt.Println("============================================================")
	fmt.Println("[Telemetry Go] Second consumer — passive observer")
	fmt.Printf("[Telemetry Go] Connecting to CSS at %s\n", cssAddr)
	fmt.Println("[Telemetry Go] Prometheus metrics at :9091/metrics")
	fmt.Println("============================================================")

	conn, err := grpc.NewClient(cssAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial failed: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	client := pb.NewConstellationStateServiceClient(conn)

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		resp, err := client.GetPing(ctx, &pb.PingRequest{})
		cancel()
		if err == nil && resp.Status == "ok" {
			grpcCalls.WithLabelValues("GetPing", "ok").Inc()
			fmt.Println("[Telemetry Go] CSS ready. Starting telemetry loop.")
			break
		}
		grpcCalls.WithLabelValues("GetPing", "error").Inc()
		fmt.Println("[Telemetry Go] Waiting for CSS...")
		time.Sleep(2 * time.Second)
	}

	go startMetricsServer()

	for tick := 1; ; tick++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := client.GetAllEphemeris(ctx, &pb.EphemerisRequest{})
		cancel()
		if err != nil {
			grpcCalls.WithLabelValues("GetAllEphemeris", "error").Inc()
			fmt.Printf("[Telemetry] tick=%d ERROR: %v\n", tick, err)
			time.Sleep(time.Second)
			continue
		}
		grpcCalls.WithLabelValues("GetAllEphemeris", "ok").Inc()

		nominal, degraded, eclipse := 0, 0, 0
		for _, sat := range resp.Satellites {
			if sat.Health == "DEGRADED" {
				degraded++
			} else {
				nominal++
			}
			if sat.Eclipse {
				eclipse++
			}
		}

		// Update Prometheus gauges with latest constellation state
		telemetryTicksTotal.Inc()
		nominalSatellites.Set(float64(nominal))
		degradedSatellites.Set(float64(degraded))
		eclipseSatellites.Set(float64(eclipse))

		fmt.Printf("[Telemetry] tick=%-4d v=%-4d total=%-3d nominal=%-3d degraded=%-3d eclipse=%-3d\n",
			tick, resp.Version, len(resp.Satellites), nominal, degraded, eclipse)
		time.Sleep(time.Second)
	}
}
