//go:build !e2e && !load && !rampup && !integration

package cli

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hatchet-dev/hatchet/pkg/auth/token"
)

// TestReplaceExistingWorkerToken pins the one branch that mints a second long-lived credential.
// Too wide and every helm upgrade issues a new worker token; too narrow and a cluster whose
// Hatchet database was re-created never recovers, which is the bug this branch exists for.
func TestReplaceExistingWorkerToken(t *testing.T) {
	for _, tc := range []struct {
		name        string
		validateErr error
		replace     bool
		fails       bool
	}{
		{
			name:        "the database accepts the token, so it stays",
			validateErr: nil,
		},
		{
			name:        "the row is gone, so a replacement is minted",
			validateErr: token.ErrTokenNotInDatabase,
			replace:     true,
		},
		{
			name:        "the reason reaches us wrapped and still mints",
			validateErr: fmt.Errorf("validating the worker token: %w", token.ErrTokenNotInDatabase),
			replace:     true,
		},
		{
			name:        "an operator revoked the token, so the command fails",
			validateErr: token.ErrTokenRevoked,
			fails:       true,
		},
		{
			name:        "the database is unreachable, so the command fails",
			validateErr: errors.New("failed to read token from database: dial error"),
			fails:       true,
		},
		{
			name:        "the signature does not verify, so the command fails",
			validateErr: errors.New("failed to verify and decode JWT: invalid JWT"),
			fails:       true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			replace, err := replaceExistingWorkerToken(tc.validateErr)

			if tc.fails {
				require.Error(t, err)
				assert.False(t, replace, "a command that fails must never report that it minted")

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.replace, replace)
		})
	}
}
