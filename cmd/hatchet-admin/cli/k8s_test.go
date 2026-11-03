//go:build !e2e && !load && !rampup && !integration

package cli

import (
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	"github.com/hatchet-dev/hatchet/pkg/validator"
)

const (
	keyCookies = "SERVER_AUTH_COOKIE_SECRETS"
	keyMaster  = "SERVER_ENCRYPTION_MASTER_KEYSET"
	keyPrivate = "SERVER_ENCRYPTION_JWT_PRIVATE_KEYSET"
	keyPublic  = "SERVER_ENCRYPTION_JWT_PUBLIC_KEYSET"
	keyEmail   = "ADMIN_EMAIL"
	keyPass    = "ADMIN_PASSWORD"
)

func completeConfig() map[string]string {
	return map[string]string{
		keyCookies: "existing-cookies",
		keyMaster:  "existing-master",
		keyPrivate: "existing-private",
		keyPublic:  "existing-public",
		keyEmail:   "owner@example.test",
		keyPass:    "Existing-Password-1",
	}
}

func completeConfigWithout(keys ...string) map[string]string {
	config := completeConfig()
	for _, key := range keys {
		delete(config, key)
	}

	return config
}

func getter(config map[string]string) func(string) string {
	return func(key string) string { return config[key] }
}

func updatedKeys(updates map[string]string) []string {
	keys := make([]string, 0, len(updates))
	for key := range updates {
		keys = append(keys, key)
	}

	return keys
}

func TestQuickstartUpdates(t *testing.T) {
	t.Run("absent config generates every value", func(t *testing.T) {
		updates, err := quickstartUpdates(getter(nil), false)
		require.NoError(t, err)

		assert.ElementsMatch(t, []string{keyCookies, keyMaster, keyPrivate, keyPublic, keyEmail, keyPass}, updatedKeys(updates))
		assert.Equal(t, "admin@example.com", updates[keyEmail])
		assert.Len(t, strings.Fields(updates[keyCookies]), 2)
		assert.NotEmpty(t, updates[keyMaster])
		assert.NotEmpty(t, updates[keyPrivate])
		assert.NotEmpty(t, updates[keyPublic])

		// The same check that the seed step applies to ADMIN_PASSWORD.
		seedInput := struct {
			Password string `validate:"password"`
		}{updates[keyPass]}
		assert.NoError(t, validator.NewDefaultValidator().Validate(seedInput))
	})

	t.Run("missing admin value is the only value generated", func(t *testing.T) {
		updates, err := quickstartUpdates(getter(completeConfigWithout(keyPass)), false)
		require.NoError(t, err)
		assert.Equal(t, []string{keyPass}, updatedKeys(updates))

		updates, err = quickstartUpdates(getter(completeConfigWithout(keyEmail)), false)
		require.NoError(t, err)
		assert.Equal(t, map[string]string{keyEmail: "admin@example.com"}, updates)
	})

	t.Run("missing cookie secrets are the only value generated", func(t *testing.T) {
		updates, err := quickstartUpdates(getter(completeConfigWithout(keyCookies)), false)
		require.NoError(t, err)
		assert.Equal(t, []string{keyCookies}, updatedKeys(updates))
	})

	t.Run("config without keysets gets all three", func(t *testing.T) {
		updates, err := quickstartUpdates(getter(completeConfigWithout(keyMaster, keyPrivate, keyPublic)), false)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{keyMaster, keyPrivate, keyPublic}, updatedKeys(updates))
	})

	t.Run("partial keysets are an error that names the missing keys", func(t *testing.T) {
		for _, missing := range [][]string{
			{keyMaster},
			{keyPrivate},
			{keyPublic},
			{keyMaster, keyPublic},
			{keyPrivate, keyPublic},
		} {
			updates, err := quickstartUpdates(getter(completeConfigWithout(missing...)), false)
			require.Error(t, err, "missing %v", missing)
			assert.Nil(t, updates)

			for _, key := range []string{keyMaster, keyPrivate, keyPublic} {
				assert.Equal(t, slices.Contains(missing, key), strings.Contains(err.Error(), key), "%s in %q", key, err)
			}
		}
	})

	t.Run("complete config needs no write", func(t *testing.T) {
		config := completeConfig()

		secret := &corev1.Secret{Data: map[string][]byte{}}
		for key, value := range config {
			secret.Data[key] = []byte(value)
		}

		for name, get := range map[string]func(string) string{
			"secret":    newFromSecret(secret, "hatchet-config").get,
			"configmap": newFromConfigMap(&corev1.ConfigMap{Data: config}, "hatchet-config").get,
		} {
			updates, err := quickstartUpdates(get, false)
			require.NoError(t, err, name)
			assert.Empty(t, updates, name)
		}
	})

	t.Run("overwrite regenerates cookies and keysets and keeps the admin login", func(t *testing.T) {
		updates, err := quickstartUpdates(getter(completeConfig()), true)
		require.NoError(t, err)

		assert.ElementsMatch(t, []string{keyCookies, keyMaster, keyPrivate, keyPublic}, updatedKeys(updates))
		assert.NotEqual(t, "existing-master", updates[keyMaster])
	})

	t.Run("overwrite regenerates partial keysets without an error", func(t *testing.T) {
		updates, err := quickstartUpdates(getter(completeConfigWithout(keyPublic)), true)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{keyCookies, keyMaster, keyPrivate, keyPublic}, updatedKeys(updates))
	})

	t.Run("overwrite generates a missing admin value", func(t *testing.T) {
		updates, err := quickstartUpdates(getter(completeConfigWithout(keyPass)), true)
		require.NoError(t, err)
		assert.Contains(t, updates, keyPass)
		assert.NotContains(t, updates, keyEmail)
	})
}
