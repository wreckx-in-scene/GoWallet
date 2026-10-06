package gateway

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	authv1 "github.com/wreckx-in-scene/GoWallet/gen/auth/v1"
	paymentv1 "github.com/wreckx-in-scene/GoWallet/gen/payment/v1"
	userv1 "github.com/wreckx-in-scene/GoWallet/gen/user/v1"
	walletv1 "github.com/wreckx-in-scene/GoWallet/gen/wallet/v1"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/authn"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/ratelimit"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	defaultTimeout = 5 * time.Second
	paymentBudget  = 5 * time.Second
)

var (
	httpRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total", Help: "HTTP requests by route and status.",
	}, []string{"route", "status"})
	httpDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "http_request_duration_seconds", Help: "HTTP latency by route.", Buckets: prometheus.DefBuckets,
	}, []string{"route"})
)

type Server struct {
	cfg       Config
	log       *slog.Logger
	auth      authv1.AuthServiceClient
	user      userv1.UserServiceClient
	wallet    walletv1.WalletServiceClient
	payment   paymentv1.PaymentServiceClient
	verifier  *authn.Verifier
	ipLimit   *ratelimit.Limiter
	userLimit *ratelimit.Limiter
}

func (s *Server) instrument(pattern string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sw := &statusWriter{ResponseWriter: w, code: http.StatusOK}
		start := time.Now()
		next(sw, r)
		httpRequests.WithLabelValues(pattern, strconv.Itoa(sw.code)).Inc()
		httpDuration.WithLabelValues(pattern).Observe(time.Since(start).Seconds())
	}
}

func NewServer(cfg Config, log *slog.Logger,
	auth authv1.AuthServiceClient, user userv1.UserServiceClient,
	wallet walletv1.WalletServiceClient, payment paymentv1.PaymentServiceClient) *Server {
	return &Server{
		cfg: cfg, log: log,
		auth: auth, user: user, wallet: wallet, payment: payment,
		verifier:  authn.NewVerifier(),
		ipLimit:   ratelimit.New(cfg.AuthPerMinute),
		userLimit: ratelimit.New(cfg.UserPerMinute),
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	handle := func(pattern string, h http.HandlerFunc) {
		mux.HandleFunc(pattern, s.instrument(pattern, h))
	}

	handle("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	handle("POST /auth/register", s.byIP(s.register))
	handle("POST /auth/login", s.byIP(s.login))
	handle("POST /auth/refresh", s.byIP(s.refresh))
	handle("POST /auth/logout", s.byIP(s.logout))

	handle("GET /me", s.authed(s.getMe))
	handle("PATCH /me", s.authed(s.updateMe))
	handle("GET /wallet", s.authed(s.getWallet))
	handle("POST /payments", s.authed(s.createPayment))
	handle("GET /payments/{id}", s.authed(s.getPayment))

	return s.observe(mux)
}

// Run serves HTTP until ctx is cancelled, then shuts down gracefully.
func (s *Server) Run(ctx context.Context) error {
	go s.userLimit.Cleanup(ctx)
	go s.ipLimit.Cleanup(ctx)
	go s.refreshKeys(ctx)

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", s.cfg.HTTPPort),
		Handler:           s.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		s.log.Info("http server listening", "addr", srv.Addr)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		return fmt.Errorf("http serve: %w", err)
	case <-ctx.Done():
		s.log.Info("shutdown signal received")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	return nil
}

// refreshKeys keeps the token verification key up to date.
func (s *Server) refreshKeys(ctx context.Context) {
	for {
		wait := 5 * time.Minute
		if err := s.fetchKey(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			s.log.Warn("could not fetch signing key from auth, will retry", "error", err)
			wait = 10 * time.Second
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

func (s *Server) fetchKey(ctx context.Context) error {
	cctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	resp, err := s.auth.GetPublicKey(cctx, &authv1.GetPublicKeyRequest{})
	if err != nil {
		return err
	}
	if len(resp.GetPublicKey()) != ed25519.PublicKeySize {
		return fmt.Errorf("unexpected public key size %d", len(resp.GetPublicKey()))
	}
	s.verifier.SetKey(resp.GetKeyId(), ed25519.PublicKey(resp.GetPublicKey()))
	s.log.Debug("signing key loaded", "key_id", resp.GetKeyId())
	return nil
}

// ---- middleware ----

func (s *Server) byIP(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			host = r.RemoteAddr
		}
		if !s.ipLimit.Allow(host) {
			w.Header().Set("Retry-After", "5")
			writeError(w, http.StatusTooManyRequests, "too many requests")
			return
		}
		next(w, r)
	}
}

func (s *Server) authed(next func(http.ResponseWriter, *http.Request, string)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || token == "" {
			writeError(w, http.StatusUnauthorized, "missing bearer token")
			return
		}
		userID, err := s.verifier.Verify(token)
		if err != nil {
			writeError(w, http.StatusUnauthorized, "invalid or expired token")
			return
		}
		if !s.userLimit.Allow(userID) {
			w.Header().Set("Retry-After", "1")
			writeError(w, http.StatusTooManyRequests, "too many requests")
			return
		}
		next(w, r, userID)
	}
}

// ---- auth routes ----

type credentials struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type refreshBody struct {
	RefreshToken string `json:"refresh_token"`
}

func writeTokens(w http.ResponseWriter, t *authv1.TokenPair) {
	writeJSON(w, http.StatusOK, map[string]any{
		"access_token":       t.GetAccessToken(),
		"refresh_token":      t.GetRefreshToken(),
		"expires_in_seconds": t.GetExpiresInSeconds(),
	})
}

func (s *Server) register(w http.ResponseWriter, r *http.Request) {
	var body credentials
	if !readJSON(w, r, &body) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), defaultTimeout)
	defer cancel()
	res, err := s.auth.Register(ctx, &authv1.RegisterRequest{Email: body.Email, Password: body.Password})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"user_id": res.GetUserId()})
}

func (s *Server) login(w http.ResponseWriter, r *http.Request) {
	var body credentials
	if !readJSON(w, r, &body) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), defaultTimeout)
	defer cancel()
	res, err := s.auth.Login(ctx, &authv1.LoginRequest{Email: body.Email, Password: body.Password})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	writeTokens(w, res)
}

func (s *Server) refresh(w http.ResponseWriter, r *http.Request) {
	var body refreshBody
	if !readJSON(w, r, &body) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), defaultTimeout)
	defer cancel()
	res, err := s.auth.Refresh(ctx, &authv1.RefreshRequest{RefreshToken: body.RefreshToken})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	writeTokens(w, res)
}

func (s *Server) logout(w http.ResponseWriter, r *http.Request) {
	var body refreshBody
	if !readJSON(w, r, &body) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), defaultTimeout)
	defer cancel()
	if _, err := s.auth.Logout(ctx, &authv1.LogoutRequest{RefreshToken: body.RefreshToken}); err != nil {
		writeGRPCError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ---- profile and wallet ----

type profileJSON struct {
	UserID   string `json:"user_id"`
	Email    string `json:"email"`
	FullName string `json:"full_name"`
}

func toProfileJSON(p *userv1.Profile) profileJSON {
	return profileJSON{UserID: p.GetUserId(), Email: p.GetEmail(), FullName: p.GetFullName()}
}

func (s *Server) getMe(w http.ResponseWriter, r *http.Request, userID string) {
	ctx, cancel := context.WithTimeout(r.Context(), defaultTimeout)
	defer cancel()
	res, err := s.user.GetProfile(ctx, &userv1.GetProfileRequest{UserId: userID})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toProfileJSON(res.GetProfile()))
}

func (s *Server) updateMe(w http.ResponseWriter, r *http.Request, userID string) {
	var body struct {
		FullName string `json:"full_name"`
	}
	if !readJSON(w, r, &body) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), defaultTimeout)
	defer cancel()
	res, err := s.user.UpdateProfile(ctx, &userv1.UpdateProfileRequest{UserId: userID, FullName: body.FullName})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toProfileJSON(res.GetProfile()))
}

func (s *Server) getWallet(w http.ResponseWriter, r *http.Request, userID string) {
	ctx, cancel := context.WithTimeout(r.Context(), defaultTimeout)
	defer cancel()
	wl, err := s.wallet.CreateWallet(ctx, &walletv1.CreateWalletRequest{UserId: userID})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	bal, err := s.wallet.GetBalance(ctx, &walletv1.GetBalanceRequest{WalletId: wl.GetWalletId()})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"wallet_id":     wl.GetWalletId(),
		"balance_paise": bal.GetBalancePaise(),
	})
}

// ---- payments ----

type paymentJSON struct {
	PaymentID     string `json:"payment_id"`
	Status        string `json:"status"`
	FailureReason string `json:"failure_reason,omitempty"`
	FromWalletID  string `json:"from_wallet_id"`
	ToWalletID    string `json:"to_wallet_id"`
	AmountPaise   int64  `json:"amount_paise"`
}

func toPaymentJSON(p *paymentv1.Payment) paymentJSON {
	return paymentJSON{
		PaymentID: p.GetPaymentId(), Status: p.GetStatus(), FailureReason: p.GetFailureReason(),
		FromWalletID: p.GetFromWalletId(), ToWalletID: p.GetToWalletId(), AmountPaise: p.GetAmountPaise(),
	}
}

func (s *Server) createPayment(w http.ResponseWriter, r *http.Request, userID string) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		writeError(w, http.StatusBadRequest, "Idempotency-Key header is required")
		return
	}
	var body struct {
		ToWalletID  string `json:"to_wallet_id"`
		AmountPaise int64  `json:"amount_paise"`
	}
	if !readJSON(w, r, &body) {
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), paymentBudget)
	defer cancel()
	res, err := s.payment.CreatePayment(ctx, &paymentv1.CreatePaymentRequest{
		UserId: userID, IdempotencyKey: key, ToWalletId: body.ToWalletID, AmountPaise: body.AmountPaise,
	})
	if err != nil {
		if status.Code(err) == codes.DeadlineExceeded {
			// The payment keeps running server-side; the same request returns its result.
			writeJSON(w, http.StatusAccepted, map[string]string{
				"status": "PROCESSING",
				"detail": "still in progress: repeat this request with the same Idempotency-Key to get the result",
			})
			return
		}
		writeGRPCError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toPaymentJSON(res.GetPayment()))
}

func (s *Server) getPayment(w http.ResponseWriter, r *http.Request, userID string) {
	ctx, cancel := context.WithTimeout(r.Context(), defaultTimeout)
	defer cancel()
	res, err := s.payment.GetPayment(ctx, &paymentv1.GetPaymentRequest{UserId: userID, PaymentId: r.PathValue("id")})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toPaymentJSON(res.GetPayment()))
}
