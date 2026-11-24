package authlab

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
)

// TenantHeader is the host endpoint's tenant context. The middleware verifies
// that the signed token tenant matches this value; session lookup itself always
// uses the signed tenant.
const TenantHeader = "X-Tenant-ID"

type claimsContextKey struct{}

func WithClaims(ctx context.Context, claims Claims) context.Context {
	return context.WithValue(ctx, claimsContextKey{}, claims)
}

func ClaimsFromContext(ctx context.Context) (Claims, bool) {
	claims, ok := ctx.Value(claimsContextKey{}).(Claims)
	return claims, ok
}

// AuthenticationMiddleware validates a bearer access token for the tenant in
// X-Tenant-ID and stores verified claims in the request context.
func (e *Engine) AuthenticationMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, ok := BearerToken(r.Header.Get("Authorization"))
		if !ok {
			WriteAuthError(w, newError(CodeTokenInvalid, "middleware", "bearer token required"))
			return
		}
		claims, err := e.ValidateAccessForTenant(r.Context(), r.Header.Get(TenantHeader), token)
		if err != nil {
			WriteAuthError(w, err)
			return
		}
		next.ServeHTTP(w, r.WithContext(WithClaims(r.Context(), claims)))
	})
}

// RequirePermission is intended to run after AuthenticationMiddleware.
func RequirePermission(permission string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims, ok := ClaimsFromContext(r.Context())
		if !ok {
			WriteAuthError(w, newError(CodeTokenInvalid, "permission", "claims unavailable"))
			return
		}
		if !hasString(claims.Permissions, permission) {
			WriteAuthError(w, newError(CodePrivilegeEscalation, "permission", "permission not granted"))
			return
		}
		next.ServeHTTP(w, r)
	})
}

func BearerToken(header string) (string, bool) {
	parts := strings.Fields(header)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || parts[1] == "" {
		return "", false
	}
	return parts[1], true
}

func WriteAuthError(w http.ResponseWriter, err error) {
	code := CodeOf(err)
	status := http.StatusInternalServerError
	switch code {
	case CodeTokenInvalid, CodeTokenReuse, CodeAuthInvalid:
		status = http.StatusUnauthorized
	case CodeCrossTenant, CodePrivilegeEscalation:
		status = http.StatusForbidden
	case CodeSessionFixation, CodeSnapshotVersion:
		status = http.StatusConflict
	case CodeAuthContention:
		status = http.StatusTooManyRequests
	case CodeNotFound:
		status = http.StatusNotFound
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]string{"code": string(code)},
	})
}
