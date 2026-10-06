package grpcserver

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

type Options struct {
	Name       string
	Port       int
	Log        *slog.Logger
	DB         *pgxpool.Pool // optional: when set, health follows database reachability
	Reflection bool
	Register   func(*grpc.Server)
}

type Server struct {
	opts Options
	log  *slog.Logger
	srv  *grpc.Server
	hsrv *health.Server
}

func New(o Options) *Server {
	s := &Server{
		opts: o,
		log:  o.Log.With("service", o.Name),
		srv:  grpc.NewServer(grpc.ChainUnaryInterceptor(metrics.UnaryServerInterceptor())),
		hsrv: health.NewServer(),
	}
	s.hsrv.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	healthpb.RegisterHealthServer(s.srv, s.hsrv)
	o.Register(s.srv)
	if o.Reflection {
		reflection.Register(s.srv)
	}
	return s
}

func (s *Server) Run(ctx context.Context) error {
	addr := fmt.Sprintf(":%d", s.opts.Port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}

	go s.watch(ctx)

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

func (s *Server) watch(ctx context.Context) {
	if s.opts.DB == nil {
		s.hsrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
		return
	}
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := s.opts.DB.Ping(pingCtx)
		cancel()
		if ctx.Err() != nil {
			return
		}
		st := healthpb.HealthCheckResponse_SERVING
		if err != nil {
			st = healthpb.HealthCheckResponse_NOT_SERVING
			s.log.Warn("database ping failed", "error", err)
		}
		s.hsrv.SetServingStatus("", st)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
