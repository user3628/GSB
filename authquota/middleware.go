package authquota

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
)

type principalContextKey struct{}

// PrincipalFromContext returns the authenticated principal installed by
// Engine.Middleware.
func PrincipalFromContext(ctx context.Context) (Principal, bool) {
	principal, ok := ctx.Value(principalContextKey{}).(Principal)
	return principal, ok
}

// Middleware is a minimal Bearer-token adapter for host services. OAuth error
// bodies use the same stable business codes as the domain API.
func (e *Engine) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := r.Header.Get("Authorization")
		parts := strings.Fields(header)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
			writeMiddlewareError(w, bizError(CodeInvalidGrant, "Authorization: Bearer token is required", nil))
			return
		}
		principal, err := e.Authenticate(r.Context(), parts[1])
		if err != nil {
			writeMiddlewareError(w, err)
			return
		}
		ctx := context.WithValue(r.Context(), principalContextKey{}, principal)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func writeMiddlewareError(w http.ResponseWriter, err error) {
	code := ErrorCode(err)
	status := http.StatusUnauthorized
	message := err.Error()
	if target, ok := err.(*BusinessError); ok {
		status = target.HTTPStatus
		message = target.Message
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(BusinessError{Code: code, Message: message, HTTPStatus: status})
}
