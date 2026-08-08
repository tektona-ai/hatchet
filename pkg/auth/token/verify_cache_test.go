package token

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hatchet-dev/hatchet/pkg/encryption"
	v1 "github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

// fakeTokenRepo stands in for the API token repository so these tests can run
// without a database. It counts reads so a test can tell whether the revocation
// check actually happened, which is the property the signature cache must not
// break.
type fakeTokenRepo struct {
	v1.APITokenRepository

	reads   atomic.Int64
	revoked atomic.Bool

	id        uuid.UUID
	expiresAt time.Time
}

func (f *fakeTokenRepo) GetAPITokenById(ctx context.Context, id uuid.UUID) (*sqlcv1.APIToken, error) {
	f.reads.Add(1)

	return &sqlcv1.APIToken{
		ID:        f.id,
		Revoked:   f.revoked.Load(),
		ExpiresAt: pgtype.Timestamp{Valid: true, Time: f.expiresAt},
	}, nil
}

func newTestJWTManager(t *testing.T) (*jwtManagerImpl, *fakeTokenRepo) {
	t.Helper()

	masterKey, privateJWT, publicJWT, _, err := encryption.GenerateLocalKeys()
	require.NoError(t, err)

	svc, err := encryption.NewLocalEncryption(masterKey, privateJWT, publicJWT)
	require.NoError(t, err)

	repo := &fakeTokenRepo{expiresAt: time.Now().UTC().Add(time.Hour)}

	mgr, err := NewJWTManager(svc, repo, &TokenOpts{Issuer: "hatchet", Audience: "hatchet"})
	require.NoError(t, err)

	impl, ok := mgr.(*jwtManagerImpl)
	require.True(t, ok)

	return impl, repo
}

// mintToken signs a token without going through GenerateTenantToken, which
// would try to write it to a database.
func mintToken(t *testing.T, j *jwtManagerImpl, tenantId uuid.UUID) (string, uuid.UUID) {
	t.Helper()

	tok, err := j.createToken(context.Background(), tenantId, "test token", nil, nil)
	require.NoError(t, err)

	return tok.Token, tok.TokenId
}

// Revoking a token must take effect immediately even though its signature is
// cached. This is the invariant the cache split exists to preserve: caching the
// signature must never cache the token's liveness.
func TestRevocationIsNotCachedWithSignature(t *testing.T) {
	j, repo := newTestJWTManager(t)

	tenantId := uuid.New()
	tokenStr, tokenId := mintToken(t, j, tenantId)
	repo.id = tokenId

	gotTenant, _, err := j.ValidateTenantToken(context.Background(), tokenStr)
	require.NoError(t, err)
	assert.Equal(t, tenantId, gotTenant)

	// Warm the signature cache.
	_, _, err = j.ValidateTenantToken(context.Background(), tokenStr)
	require.NoError(t, err)

	repo.revoked.Store(true)

	_, _, err = j.ValidateTenantToken(context.Background(), tokenStr)
	require.Error(t, err, "a revoked token must be rejected even with its signature cached")
	assert.Contains(t, err.Error(), "revoked")
}

// An expired token must likewise be rejected on the cached path, since expiry
// is checked against the stored token rather than the memoized claims.
func TestExpiryIsNotCachedWithSignature(t *testing.T) {
	j, repo := newTestJWTManager(t)

	tenantId := uuid.New()
	tokenStr, tokenId := mintToken(t, j, tenantId)
	repo.id = tokenId

	_, _, err := j.ValidateTenantToken(context.Background(), tokenStr)
	require.NoError(t, err)

	repo.expiresAt = time.Now().UTC().Add(-time.Minute)

	_, _, err = j.ValidateTenantToken(context.Background(), tokenStr)
	require.Error(t, err, "an expired token must be rejected even with its signature cached")
	assert.Contains(t, err.Error(), "expired")
}

// The token repository must be consulted on every validation, not once per
// cache window — that read is what makes revocation timely.
func TestTokenRepoIsReadOnEveryValidation(t *testing.T) {
	j, repo := newTestJWTManager(t)

	tenantId := uuid.New()
	tokenStr, tokenId := mintToken(t, j, tenantId)
	repo.id = tokenId

	const validations = 5

	for range validations {
		_, _, err := j.ValidateTenantToken(context.Background(), tokenStr)
		require.NoError(t, err)
	}

	assert.Equal(t, int64(validations), repo.reads.Load(),
		"every validation must re-check the token against the repository")
}

// The signature cache must actually cache: a repeated validation of the same
// token should not re-run verification. Asserted through the cache itself,
// since the tink verifier gives no call counter.
func TestSignatureVerificationIsCached(t *testing.T) {
	j, repo := newTestJWTManager(t)

	tenantId := uuid.New()
	tokenStr, tokenId := mintToken(t, j, tenantId)
	repo.id = tokenId

	first, err := j.verifySignature(tokenStr)
	require.NoError(t, err)

	second, err := j.verifySignature(tokenStr)
	require.NoError(t, err)

	assert.Same(t, first, second, "a repeated verification should return the memoized result")
}

// A token that fails verification must not be remembered, so that a bad token
// cannot occupy the cache or be served from it later.
func TestFailedVerificationIsNotCached(t *testing.T) {
	j, _ := newTestJWTManager(t)

	_, err := j.verifySignature("not-a-jwt")
	require.Error(t, err)

	_, err = j.verifySignature("not-a-jwt")
	require.Error(t, err, "an invalid token must keep failing rather than being served from cache")
}

// Two different tokens must not collide in the cache.
func TestDistinctTokensCacheSeparately(t *testing.T) {
	j, _ := newTestJWTManager(t)

	tenantA := uuid.New()
	tenantB := uuid.New()

	tokenA, _ := mintToken(t, j, tenantA)
	tokenB, _ := mintToken(t, j, tenantB)

	gotA, err := j.verifySignature(tokenA)
	require.NoError(t, err)

	gotB, err := j.verifySignature(tokenB)
	require.NoError(t, err)

	assert.Equal(t, tenantA, gotA.tenantId)
	assert.Equal(t, tenantB, gotB.tenantId)
	assert.NotEqual(t, gotA.tokenId, gotB.tokenId)
}
