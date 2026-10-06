package gateway

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

// readJSON decodes a small JSON body and rejects unknown fields.
func readJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return false
	}
	return true
}

// writeGRPCError maps a gRPC status to an HTTP response without leaking internals.
func writeGRPCError(w http.ResponseWriter, err error) {
	st, _ := status.FromError(err)
	switch st.Code() {
	case codes.InvalidArgument:
		writeError(w, http.StatusBadRequest, st.Message())
	case codes.Unauthenticated:
		writeError(w, http.StatusUnauthorized, st.Message())
	case codes.PermissionDenied:
		writeError(w, http.StatusForbidden, st.Message())
	case codes.NotFound:
		writeError(w, http.StatusNotFound, st.Message())
	case codes.AlreadyExists:
		writeError(w, http.StatusConflict, st.Message())
	case codes.FailedPrecondition:
		writeError(w, http.StatusUnprocessableEntity, st.Message())
	case codes.Unavailable:
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, "service temporarily unavailable, please retry")
	case codes.DeadlineExceeded:
		writeError(w, http.StatusGatewayTimeout, "upstream timeout")
	default:
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (s *statusWriter) WriteHeader(code int) {
	s.code = code
	s.ResponseWriter.WriteHeader(code)
}

// observe adds a request id, recovers from panics and writes an access log line.
func (s *Server) observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-Id")
		if id == "" || len(id) > 64 {
			id = uuid.NewString()
		}
		w.Header().Set("X-Request-Id", id)

		sw := &statusWriter{ResponseWriter: w, code: http.StatusOK}
		start := time.Now()
		defer func() {
			if rec := recover(); rec != nil {
				s.log.Error("panic in handler", "request_id", id, "panic", rec)
				writeError(sw, http.StatusInternalServerError, "internal error")
			}
			s.log.Info("request",
				"request_id", id, "method", r.Method, "path", r.URL.Path,
				"status", sw.code, "duration_ms", time.Since(start).Milliseconds())
		}()
		next.ServeHTTP(sw, r)
	})
}
