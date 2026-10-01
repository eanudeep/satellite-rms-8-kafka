package main

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	pb "css/satelliterms"
)

const (
	satCount     = 50
	historyLimit = 20
	angleStep    = 1.0
	orbitTick    = 500 * time.Millisecond
)

// ─────────────────────────────────────────────
// Prometheus metrics
// ─────────────────────────────────────────────

var (
	grpcCallsReceived = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "rms_css_grpc_calls_total",
		Help: "Total inbound gRPC calls received by CSS, labeled by method.",
	}, []string{"method"})

	orbitTicksTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "rms_css_orbit_ticks_total",
		Help: "Total orbit simulation ticks executed by this CSS replica.",
	})

	degradedSatellites = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "rms_css_degraded_satellites",
		Help: "Number of DEGRADED satellites in the current orbit tick.",
	})

	eclipseSatellites = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "rms_css_eclipse_satellites",
		Help: "Number of satellites currently in eclipse.",
	})
)

// startMetricsServer serves /metrics on port 9091 so Prometheus can scrape CSS
// independently of the gRPC port (50051). Runs in a background goroutine.
func startMetricsServer() {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	fmt.Println("[CSS] Prometheus metrics at :9091/metrics")
	if err := http.ListenAndServe(":9091", mux); err != nil {
		fmt.Fprintf(os.Stderr, "[CSS] metrics server error: %v\n", err)
	}
}

// ─────────────────────────────────────────────
// CSS server
// ─────────────────────────────────────────────

type cssServer struct {
	pb.UnimplementedConstellationStateServiceServer
	rdb *redis.Client
}

func newCSSServer(rdb *redis.Client) *cssServer {
	return &cssServer{rdb: rdb}
}

func (s *cssServer) initConstellationIfNeeded(ctx context.Context) error {
	won, err := s.rdb.SetNX(ctx, "constellation_initialized", "1", 0).Result()
	if err != nil {
		return err
	}
	if !won {
		fmt.Println("[CSS] Redis already seeded by another replica, skipping.")
		return nil
	}
	fmt.Printf("[CSS] Seeding %d satellites into Redis...\n", satCount)
	pipe := s.rdb.Pipeline()
	for i := 1; i <= satCount; i++ {
		id := fmt.Sprintf("SAT-%03d", i)
		angle := math.Round(float64(i-1)*(360.0/satCount)*10) / 10
		pipe.RPush(ctx, "sat_ids", id)
		pipe.HSet(ctx, "sat:"+id,
			"orbital_angle", strconv.FormatFloat(angle, 'f', 6, 64),
			"altitude_km", "550.000000",
			"health", "NOMINAL",
			"power_budget_w", "800",
			"eclipse", "false",
		)
	}
	pipe.Set(ctx, "version", "0", 0)
	_, err = pipe.Exec(ctx)
	return err
}

func (s *cssServer) runOrbitTick(ctx context.Context, now string) (int64, int, int, error) {
	ver, err := s.rdb.Incr(ctx, "version").Result()
	if err != nil {
		return 0, 0, 0, err
	}
	satIDs, err := s.rdb.LRange(ctx, "sat_ids", 0, -1).Result()
	if err != nil {
		return 0, 0, 0, err
	}
	readPipe := s.rdb.Pipeline()
	hgetCmds := make([]*redis.MapStringStringCmd, len(satIDs))
	for i, id := range satIDs {
		hgetCmds[i] = readPipe.HGetAll(ctx, "sat:"+id)
	}
	if _, err := readPipe.Exec(ctx); err != nil {
		return 0, 0, 0, err
	}

	degraded, eclipse := 0, 0
	writePipe := s.rdb.Pipeline()

	for i, id := range satIDs {
		vals, _ := hgetCmds[i].Result()
		angle, _ := strconv.ParseFloat(vals["orbital_angle"], 64)
		altKm, _ := strconv.ParseFloat(vals["altitude_km"], 64)

		angle = math.Mod(angle+angleStep, 360)
		power := int32(rand.Intn(191) + 610)
		inEclipse := angle > 180
		health := "NOMINAL"
		if power < 660 {
			health = "DEGRADED"
			degraded++
		}
		if inEclipse {
			eclipse++
		}

		writePipe.HSet(ctx, "sat:"+id,
			"orbital_angle", strconv.FormatFloat(angle, 'f', 6, 64),
			"altitude_km", strconv.FormatFloat(altKm, 'f', 6, 64),
			"health", health,
			"power_budget_w", strconv.Itoa(int(power)),
			"eclipse", strconv.FormatBool(inEclipse),
		)
		snapData, err := protojson.Marshal(&pb.OrbitSnapshot{
			Timestamp:    now,
			OrbitalAngle: angle,
			AltitudeKm:   altKm,
			Health:       health,
			PowerBudgetW: power,
			Eclipse:      inEclipse,
		})
		if err != nil {
			continue
		}
		writePipe.LPush(ctx, "history:"+id, string(snapData))
		writePipe.LTrim(ctx, "history:"+id, 0, historyLimit-1)
	}

	if _, err := writePipe.Exec(ctx); err != nil {
		return 0, 0, 0, err
	}
	return ver, degraded, eclipse, nil
}

func (s *cssServer) orbitLoop() {
	for {
		time.Sleep(orbitTick)
		ctx := context.Background()

		acquired, err := s.rdb.SetNX(ctx, "orbit_lock", "1", orbitTick*2).Result()
		if err != nil || !acquired {
			continue
		}

		now := time.Now().Format(time.RFC3339Nano)
		ver, degraded, eclipse, err := s.runOrbitTick(ctx, now)
		s.rdb.Del(ctx, "orbit_lock")

		if err != nil {
			fmt.Fprintf(os.Stderr, "[CSS] orbit tick error: %v\n", err)
			continue
		}

		// Update Prometheus gauges each tick
		orbitTicksTotal.Inc()
		degradedSatellites.Set(float64(degraded))
		eclipseSatellites.Set(float64(eclipse))

		ts := time.Now().Format("15:04:05.000")
		fmt.Printf("[CSS] v%d [%s] total=%d degraded=%d eclipse=%d\n",
			ver, ts, satCount, degraded, eclipse)
	}
}

func hsetToSatellite(id string, vals map[string]string) *pb.Satellite {
	angle, _ := strconv.ParseFloat(vals["orbital_angle"], 64)
	altKm, _ := strconv.ParseFloat(vals["altitude_km"], 64)
	power, _ := strconv.ParseInt(vals["power_budget_w"], 10, 32)
	return &pb.Satellite{
		Id:           id,
		OrbitalAngle: angle,
		AltitudeKm:   altKm,
		Health:       vals["health"],
		PowerBudgetW: int32(power),
		Eclipse:      vals["eclipse"] == "true",
	}
}

func (s *cssServer) GetPing(_ context.Context, _ *pb.PingRequest) (*pb.PingResponse, error) {
	grpcCallsReceived.WithLabelValues("GetPing").Inc()
	return &pb.PingResponse{Status: "ok"}, nil
}

func (s *cssServer) GetAllEphemeris(ctx context.Context, _ *pb.EphemerisRequest) (*pb.EphemerisResponse, error) {
	grpcCallsReceived.WithLabelValues("GetAllEphemeris").Inc()
	satIDs, err := s.rdb.LRange(ctx, "sat_ids", 0, -1).Result()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "redis LRange: %v", err)
	}
	verStr, _ := s.rdb.Get(ctx, "version").Result()
	ver, _ := strconv.ParseInt(verStr, 10, 32)

	pipe := s.rdb.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(satIDs))
	for i, id := range satIDs {
		cmds[i] = pipe.HGetAll(ctx, "sat:"+id)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, status.Errorf(codes.Internal, "redis pipeline: %v", err)
	}
	sats := make([]*pb.Satellite, len(satIDs))
	for i, id := range satIDs {
		vals, _ := cmds[i].Result()
		sats[i] = hsetToSatellite(id, vals)
	}
	return &pb.EphemerisResponse{
		Version:    int32(ver),
		Timestamp:  time.Now().Format(time.RFC3339Nano),
		Satellites: sats,
	}, nil
}

func (s *cssServer) GetSatellite(ctx context.Context, req *pb.SatelliteRequest) (*pb.SatelliteResponse, error) {
	grpcCallsReceived.WithLabelValues("GetSatellite").Inc()
	vals, err := s.rdb.HGetAll(ctx, "sat:"+req.SatelliteId).Result()
	if err != nil || len(vals) == 0 {
		return nil, status.Errorf(codes.NotFound, "satellite '%s' not found", req.SatelliteId)
	}
	verStr, _ := s.rdb.Get(ctx, "version").Result()
	ver, _ := strconv.ParseInt(verStr, 10, 32)
	return &pb.SatelliteResponse{
		Version:   int32(ver),
		Timestamp: time.Now().Format(time.RFC3339Nano),
		Satellite: hsetToSatellite(req.SatelliteId, vals),
	}, nil
}

func (s *cssServer) GetSatelliteHistory(ctx context.Context, req *pb.HistoryRequest) (*pb.HistoryResponse, error) {
	grpcCallsReceived.WithLabelValues("GetSatelliteHistory").Inc()
	exists, err := s.rdb.Exists(ctx, "sat:"+req.SatelliteId).Result()
	if err != nil || exists == 0 {
		return nil, status.Errorf(codes.NotFound, "satellite '%s' not found", req.SatelliteId)
	}
	raws, err := s.rdb.LRange(ctx, "history:"+req.SatelliteId, 0, -1).Result()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "redis LRange: %v", err)
	}
	snaps := make([]*pb.OrbitSnapshot, 0, len(raws))
	for _, raw := range raws {
		var snap pb.OrbitSnapshot
		if err := protojson.Unmarshal([]byte(raw), &snap); err == nil {
			snaps = append(snaps, &snap)
		}
	}
	return &pb.HistoryResponse{
		SatelliteId: req.SatelliteId,
		RecordCount: int32(len(snaps)),
		Limit:       historyLimit,
		History:     snaps,
	}, nil
}

func (s *cssServer) ReceiveSchedule(ctx context.Context, req *pb.SchedulePayload) (*pb.ScheduleAck, error) {
	grpcCallsReceived.WithLabelValues("ReceiveSchedule").Inc()
	data, err := protojson.Marshal(req)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal: %v", err)
	}
	if err := s.rdb.RPush(ctx, "schedules", string(data)).Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "redis RPush: %v", err)
	}
	fmt.Printf("[CSS] Schedule run #%d — scheduled=%d skipped=%d\n",
		req.Run, req.ScheduledCount, req.SkippedCount)
	return &pb.ScheduleAck{Message: "schedule stored", Run: req.Run}, nil
}

func (s *cssServer) GetSchedules(ctx context.Context, _ *pb.ScheduleListRequest) (*pb.ScheduleListResponse, error) {
	grpcCallsReceived.WithLabelValues("GetSchedules").Inc()
	raws, err := s.rdb.LRange(ctx, "schedules", 0, -1).Result()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "redis LRange: %v", err)
	}
	schedules := make([]*pb.SchedulePayload, 0, len(raws))
	for _, raw := range raws {
		var sp pb.SchedulePayload
		if err := protojson.Unmarshal([]byte(raw), &sp); err == nil {
			schedules = append(schedules, &sp)
		}
	}
	return &pb.ScheduleListResponse{
		TotalRuns: int32(len(schedules)),
		Schedules: schedules,
	}, nil
}

func main() {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "redis-service:6379"
	}

	fmt.Println("============================================================")
	fmt.Printf("[CSS Go Stateless] Tracking %d satellites, orbit tick %v\n", satCount, orbitTick)
	fmt.Println("[CSS Go Stateless] gRPC port 50051 | metrics port 9091")
	fmt.Printf("[CSS Go Stateless] Redis backend: %s\n", redisAddr)
	fmt.Println("============================================================")

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	ctx := context.Background()

	for {
		if err := rdb.Ping(ctx).Err(); err == nil {
			break
		}
		fmt.Println("[CSS] Waiting for Redis...")
		time.Sleep(2 * time.Second)
	}
	fmt.Println("[CSS] Redis connected.")

	srv := newCSSServer(rdb)
	if err := srv.initConstellationIfNeeded(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "[CSS] init failed: %v\n", err)
		os.Exit(1)
	}

	go srv.orbitLoop()
	go startMetricsServer()

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		fmt.Fprintf(os.Stderr, "listen failed: %v\n", err)
		os.Exit(1)
	}
	grpcSrv := grpc.NewServer()
	pb.RegisterConstellationStateServiceServer(grpcSrv, srv)
	if err := grpcSrv.Serve(lis); err != nil {
		fmt.Fprintf(os.Stderr, "serve failed: %v\n", err)
		os.Exit(1)
	}
}
