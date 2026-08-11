//go:build !e2e && !load && !rampup && !integration

package repository

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// The controller operation pools poll a tenant at their start interval for as long as this
// gauge reports activity. The gauge used to look back over the task retention period, so a
// tenant that ran one task kept reporting activity for the next 30 days and the pools never
// backed off.
func TestDefaultTaskActivityGaugeOnlyCountsRecentQueues(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	repo := createTaskRepository(pool)

	// Far longer than the activity window, so a gauge that keys off retention reports the
	// stale queue below as activity.
	repo.taskRetentionPeriod = 24 * time.Hour

	ctx := context.Background()
	tenantID := uuid.New()

	_, err := pool.Exec(ctx, `
		INSERT INTO v1_queue (tenant_id, name, last_active)
		VALUES ($1, 'stale', NOW() - INTERVAL '1 hour')
	`, tenantID)
	require.NoError(t, err)

	count, err := repo.DefaultTaskActivityGauge(ctx, tenantID.String())
	require.NoError(t, err)
	require.Equal(t, 0, count, "a queue that last ran an hour ago is not activity")

	_, err = pool.Exec(ctx, `
		INSERT INTO v1_queue (tenant_id, name, last_active)
		VALUES ($1, 'live', NOW() - INTERVAL '1 minute')
	`, tenantID)
	require.NoError(t, err)

	count, err = repo.DefaultTaskActivityGauge(ctx, tenantID.String())
	require.NoError(t, err)
	require.Equal(t, 1, count, "a queue that ran a minute ago is activity")
}

// A queue whose last_active is stale only because the task insert path skipped the upsert
// still has to read as activity, or a tenant that works without pause backs off anyway.
func TestQueueActivityWindowCoversTheQueueCacheTTL(t *testing.T) {
	require.Greater(t, queueActivityWindow, queueCacheTTL)
}
