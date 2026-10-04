package wallet

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	walletv1 "github.com/wreckx-in-scene/GoWallet/gen/wallet/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

// server will wrap the grpc and everything it depends on
type Server struct {
	cfg  Config
	log  *slog.Logger
	srv  *grpc.Server
	hsrv *health.Server
	db   *pgxpool.Pool
}

// new server build the grpc server and registers its services
func NewServer(cfg Config, log *slog.Logger, db *pgxpool.Pool) *Server {
	s := &Server{
		cfg:  cfg,
		log:  log,
		srv:  grpc.NewServer(),
		hsrv: health.NewServer(),
		db:   db,
	}

	s.hsrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	grpc_health_v1.RegisterHealthServer(s.srv, s.hsrv)

	walletv1.RegisterWalletServiceServer(s.srv, &handler{store: NewStore(db), log: log})

	if cfg.Env == "dev" {
		reflection.Register(s.srv)
	}

	return s
}

// run starts listening and blocks until ctx is cancelled
// then follows a graceful shutdown

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

	//telling heathcheckers we are going away , then drain
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

// watchDB pings the database periodically and reports the result
// through the gRPC health service. It stops when ctx is cancelled.
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

		status := grpc_health_v1.HealthCheckResponse_SERVING
		if err != nil {
			status = grpc_health_v1.HealthCheckResponse_NOT_SERVING
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
