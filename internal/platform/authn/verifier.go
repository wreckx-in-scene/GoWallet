package authn

import (
	"crypto/ed25519"
	"errors"
	"fmt"
	"sync"

	"github.com/golang-jwt/jwt/v5"
)

const issuer = "gowallet-auth"

// Verifier checks access tokens using public keys fetched from the auth service.
type Verifier struct {
	mu   sync.RWMutex
	keys map[string]ed25519.PublicKey
}

func NewVerifier() *Verifier {
	return &Verifier{keys: make(map[string]ed25519.PublicKey)}
}

func (v *Verifier) SetKey(kid string, pub ed25519.PublicKey) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.keys[kid] = pub
}

// Verify returns the user id (the "sub" claim) of a valid token.
func (v *Verifier) Verify(token string) (string, error) {
	claims := &jwt.RegisteredClaims{}
	_, err := jwt.ParseWithClaims(token, claims, v.keyFunc,
		jwt.WithValidMethods([]string{"EdDSA"}),
		jwt.WithIssuer(issuer),
		jwt.WithExpirationRequired(),
	)
	if err != nil {
		return "", fmt.Errorf("verify token: %w", err)
	}
	if claims.Subject == "" {
		return "", errors.New("token has no subject")
	}
	return claims.Subject, nil
}

func (v *Verifier) keyFunc(t *jwt.Token) (any, error) {
	kid, _ := t.Header["kid"].(string)
	v.mu.RLock()
	defer v.mu.RUnlock()
	key, ok := v.keys[kid]
	if !ok {
		return nil, fmt.Errorf("unknown key id %q", kid)
	}
	return key, nil
}
