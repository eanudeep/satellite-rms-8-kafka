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

	pb "scheduler/satelliterms"
)

const interval = time.Second

var gatewayCells = map[string][]string{
	"GW-NORTH":   {"CELL-A", "CELL-B"},
	"GW-CENTRAL": {"CELL-C", "CELL-D"},
	"GW-SOUTH":   {"CELL-E", "CELL-F"},
}

// ─────────────────────────────────────────────
// Prometheus metrics
// ─────────────────────────────────────────────

var (
	scheduleRunsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "rms_scheduler_runs_total",
		Help: "Total scheduling runs completed.",
	})

	satellitesScheduled = promauto.NewCounter(prometheus.CounterOpts{
		Name: "rms_scheduler_satellites_scheduled_total",
		Help: "Total satellites successfully scheduled across all runs.",
	})

	satellitesSkipped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "rms_scheduler_satellites_skipped_total",
		Help: "Total satellites skipped (DEGRADED health) across all runs.",
	})

	grpcCalls = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "rms_scheduler_grpc_calls_total",
		Help: "Total outbound gRPC calls made by the scheduler.",
	}, []string{"method", "result"})
)

// startMetricsServer serves /metrics on port 9091. Scheduler is a pure gRPC client
// with no existing HTTP server, so this is the only HTTP listener it runs.
func startMetricsServer() {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	fmt.Println("[Scheduler] Prometheus metrics at :9091/metrics")
	if err := http.ListenAndServe(":9091", mux); err != nil {
		fmt.Fprintf(os.Stderr, "[Scheduler] metrics server error: %v\n", err)
	}
}

// ─────────────────────────────────────────────
// Scheduler logic (unchanged from rms-6)
// ─────────────────────────────────────────────

func assignGateway(angle float64) string {
	switch {
	case angle < 120:
		return "GW-NORTH"
	case angle < 240:
		return "GW-CENTRAL"
	default:
		return "GW-SOUTH"
	}
}

func allocateTimeslots(cells []string, powerW int32) map[string]*pb.Timeslot {
	perCell := powerW / int32(len(cells))
	out := make(map[string]*pb.Timeslot, len(cells))
	for i, cell := range cells {
		out[cell] = &pb.Timeslot{Timeslot: fmt.Sprintf("TS-%d", i+1), PowerW: perCell}
	}
	return out
}

func waitForCSS(stub pb.ConstellationStateServiceClient) {
	fmt.Println("[Scheduler Go] Waiting for CSS...")
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		resp, err := stub.GetPing(ctx, &pb.PingRequest{})
		cancel()
		if err == nil && resp.Status == "ok" {
			fmt.Println("[Scheduler Go] CSS is ready.")
			return
		}
		time.Sleep(2 * time.Second)
	}
}

func runSchedule(stub pb.ConstellationStateServiceClient, run int32) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := stub.GetAllEphemeris(ctx, &pb.EphemerisRequest{})
	if err != nil {
		grpcCalls.WithLabelValues("GetAllEphemeris", "error").Inc()
		fmt.Printf("[Scheduler] gRPC error run #%d: %v\n", run, err)
		return
	}
	grpcCalls.WithLabelValues("GetAllEphemeris", "ok").Inc()

	var scheduled, skipped int32
	assignments := make([]*pb.Assignment, 0, len(resp.Satellites))

	for _, sat := range resp.Satellites {
		if sat.Health == "DEGRADED" {
			skipped++
			assignments = append(assignments, &pb.Assignment{
				SatelliteId:  sat.Id,
				Status:       "SKIPPED",
				Reason:       "DEGRADED",
				PowerBudgetW: sat.PowerBudgetW,
			})
			continue
		}
		gateway := assignGateway(sat.OrbitalAngle)
		timeslots := allocateTimeslots(gatewayCells[gateway], sat.PowerBudgetW)
		scheduled++
		assignments = append(assignments, &pb.Assignment{
			SatelliteId: sat.Id,
			Status:      "SCHEDULED",
			Gateway:     gateway,
			Eclipse:     sat.Eclipse,
			Cells:       timeslots,
		})
	}

	ts := time.Now().Format("15:04:05.000")
	fmt.Printf("[Scheduler] Run #%d [%s] v%d total=%d sched=%d skip=%d\n",
		run, ts, resp.Version, len(resp.Satellites), scheduled, skipped)

	ackCtx, ackCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer ackCancel()
	ack, err := stub.ReceiveSchedule(ackCtx, &pb.SchedulePayload{
		Run:              run,
		EphemerisVersion: resp.Version,
		Timestamp:        time.Now().Format(time.RFC3339),
		ScheduledCount:   scheduled,
		SkippedCount:     skipped,
		Assignments:      assignments,
	})
	if err != nil {
		grpcCalls.WithLabelValues("ReceiveSchedule", "error").Inc()
		fmt.Printf("  [ReceiveSchedule] error: %v\n", err)
		return
	}
	grpcCalls.WithLabelValues("ReceiveSchedule", "ok").Inc()
	fmt.Printf("  → CSS ack: %s (run #%d)\n", ack.Message, ack.Run)

	// Update Prometheus counters after successful run
	scheduleRunsTotal.Inc()
	satellitesScheduled.Add(float64(scheduled))
	satellitesSkipped.Add(float64(skipped))
}

func main() {
	cssAddr := os.Getenv("CSS_ADDR")
	if cssAddr == "" {
		cssAddr = "css-service:50051"
	}

	fmt.Println("============================================================")
	fmt.Println("[Scheduler Go] Batched Non-RT Tier")
	fmt.Printf("[Scheduler Go] Poll interval: %v  (50 satellites)\n", interval)
	fmt.Printf("[Scheduler Go] Connecting to CSS at %s\n", cssAddr)
	fmt.Println("[Scheduler Go] Prometheus metrics at :9091/metrics")
	fmt.Println("============================================================")

	conn, err := grpc.NewClient(cssAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial failed: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	stub := pb.NewConstellationStateServiceClient(conn)
	waitForCSS(stub)

	go startMetricsServer()

	for run := int32(1); ; run++ {
		runSchedule(stub, run)
		time.Sleep(interval)
	}
}
