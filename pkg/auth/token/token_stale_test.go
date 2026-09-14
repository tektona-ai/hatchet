//go:build !e2e && !load && !rampup && !integration

package token_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hatchet-dev/hatchet/pkg/auth/token"
	"github.com/hatchet-dev/hatchet/pkg/encryption"
)

// TestValidateTenantTokenSeparatesAStaleTokenFromAnUnreachableDatabase covers the case that
// strands a Kubernetes install: the "hatchet-client-config" secret outlives the database that
// the token in it was minted against. The signature still verifies, but the "APIToken" row the
// token_id claim names is gone, so the engine rejects every call.
//
// hatchet-admin k8s create-worker-token mints a replacement for exactly one of these errors, so
// a database that is only unreachable must not read the same way as one that lost the row.
func TestValidateTenantTokenSeparatesAStaleTokenFromAnUnreachableDatabase(t *testing.T) {
	tenantID := uuid.New()

	newManager := func(t *testing.T, repo *stubAPITokenRepository) token.JWTManager {
		t.Helper()

		master, priv, pub, _, err := encryption.GenerateLocalKeys()
		require.NoError(t, err)

		enc, err := encryption.NewLocalEncryption(master, priv, pub)
		require.NoError(t, err)

		mgr, err := token.NewJWTManager(enc, repo, &token.TokenOpts{
			Issuer:    "hatchet",
			Audience:  "hatchet",
			ServerURL: "http://localhost:8080",
		})
		require.NoError(t, err)

		return mgr
	}

	// The token has to be minted by a manager whose repository accepts writes, then validated
	// against the state under test. Minting and validating share one manager, so the keyset is
	// the same on both sides and only the repository answer differs.
	mint := func(t *testing.T, repo *stubAPITokenRepository) (token.JWTManager, string) {
		t.Helper()

		mgr := newManager(t, repo)

		tok, err := mgr.GenerateTenantToken(context.Background(), tenantID, "worker", false, nil)
		require.NoError(t, err)

		return mgr, tok.Token
	}

	t.Run("the row is present, so the token resolves", func(t *testing.T) {
		repo := &stubAPITokenRepository{}
		mgr, raw := mint(t, repo)

		gotTenant, _, err := mgr.ValidateTenantToken(context.Background(), raw)

		require.NoError(t, err)
		assert.Equal(t, tenantID, gotTenant)
	})

	t.Run("the row is gone, so the token is stale", func(t *testing.T) {
		repo := &stubAPITokenRepository{}
		mgr, raw := mint(t, repo)

		// Stand in for a re-created database: the same keyset, no row for the token_id.
		repo.getErr = pgx.ErrNoRows

		_, _, err := mgr.ValidateTenantToken(context.Background(), raw)

		assert.ErrorIs(t, err, token.ErrTokenNotInDatabase)
	})

	t.Run("a wrapped no-rows error is still stale", func(t *testing.T) {
		repo := &stubAPITokenRepository{}
		mgr, raw := mint(t, repo)

		repo.getErr = fmt.Errorf("querying the token: %w", pgx.ErrNoRows)

		_, _, err := mgr.ValidateTenantToken(context.Background(), raw)

		assert.ErrorIs(t, err, token.ErrTokenNotInDatabase)
	})

	t.Run("an operator revoked the row, which is not the same as stale", func(t *testing.T) {
		repo := &stubAPITokenRepository{}
		mgr, raw := mint(t, repo)

		repo.revoked = true

		_, _, err := mgr.ValidateTenantToken(context.Background(), raw)

		assert.ErrorIs(t, err, token.ErrTokenRevoked)
		assert.NotErrorIs(t, err, token.ErrTokenNotInDatabase)
	})

	t.Run("the database is unreachable, which is not the same as stale", func(t *testing.T) {
		repo := &stubAPITokenRepository{}
		mgr, raw := mint(t, repo)

		repo.getErr = errors.New("failed to connect to `host=hatchet-pg-rw`: dial error")

		_, _, err := mgr.ValidateTenantToken(context.Background(), raw)

		require.Error(t, err)
		assert.NotErrorIs(t, err, token.ErrTokenNotInDatabase)
		assert.NotErrorIs(t, err, token.ErrTokenRevoked)
	})

	t.Run("another keyset signed the token, which is not the same as stale", func(t *testing.T) {
		repo := &stubAPITokenRepository{}
		_, raw := mint(t, repo)

		other := newManager(t, &stubAPITokenRepository{})

		_, _, err := other.ValidateTenantToken(context.Background(), raw)

		require.Error(t, err)
		assert.NotErrorIs(t, err, token.ErrTokenNotInDatabase)
	})
}
