package fraud

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	fraudv1 "github.com/wreckx-in-scene/GoWallet/gen/fraud/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

// Server wraps the gRPC server and everything it depends on.
type Server struct {
	cfg  Config
	log  *slog.Logger
	db   *pgxpool.Pool
	srv  *grpc.Server
	hsrv *health.Server
}

// NewServer builds the gRPC server and registers its services.
func NewServer(cfg Config, log *slog.Logger, db *pgxpool.Pool) *Server {
	s := &Server{
		cfg:  cfg,
		log:  log,
		db:   db,
		srv:  grpc.NewServer(),
		hsrv: health.NewServer(),
	}

	// Not ready until the first successful database ping (see watchDB).
	s.hsrv.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	healthpb.RegisterHealthServer(s.srv, s.hsrv)

	store := NewStore(db, Rules{MaxAmountPaise: cfg.MaxAmountPaise, MaxPerMinute: cfg.MaxPerMinute})
	fraudv1.RegisterFraudServiceServer(s.srv, &handler{store: store, log: log})

	if cfg.Env == "dev" {
		reflection.Register(s.srv)
	}
	return s
}

// Run starts listening and blocks until ctx is cancelled, then shuts down gracefully.
func (s *Server) Run(ctx context.Context) error {
	addr := fmt.Sprintf(":%d", s.cfg.GRPCPort)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}

	go s.watchDB(ctx)

	serveErr := make(chan error, 1)
	go func() {
		s.log.Info("grpc server listening", "addr", addr)
		serveErr <- s.srv.Serve(lis)
	}()

	select {
	case err := <-serveErr:
		return fmt.Errorf("grpc serve: %w", err)
	case <-ctx.Done():
		s.log.Info("shutdown signal received")
	}

	s.hsrv.Shutdown()

	stopped := make(chan struct{})
	go func() {
		s.srv.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
		s.log.Info("grpc server stopped gracefully")
	case <-time.After(10 * time.Second):
		s.log.Warn("graceful stop timed out, forcing stop")
		s.srv.Stop()
	}
	return nil
}

// watchDB pings the database periodically and reports the result through
// the gRPC health service. It stops when ctx is cancelled.
func (s *Server) watchDB(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := s.db.Ping(pingCtx)
		cancel()

		if ctx.Err() != nil {
			return
		}

		status := healthpb.HealthCheckResponse_SERVING
		if err != nil {
			status = healthpb.HealthCheckResponse_NOT_SERVING
			s.log.Warn("database ping failed", "error", err)
		}
		s.hsrv.SetServingStatus("", status)

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
