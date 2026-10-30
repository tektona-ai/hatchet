package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewAPIProxyKeepsForwardedForChain(t *testing.T) {
	var gotXFF, gotHost string

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotXFF = r.Header.Get("X-Forwarded-For")
		gotHost = r.Host
	}))
	defer backend.Close()

	proxy, err := newAPIProxy(backend.URL)
	require.NoError(t, err)

	front := httptest.NewServer(proxy)
	defer front.Close()

	req, err := http.NewRequest(http.MethodGet, front.URL+"/api/v1/meta", nil)
	require.NoError(t, err)
	req.Host = "app.example.com"
	req.Header.Set("X-Forwarded-For", "203.0.113.7")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	assert.Equal(t, "203.0.113.7, 127.0.0.1", gotXFF)
	assert.Equal(t, "app.example.com", gotHost)
}

func TestNewAPIProxyRejectsURLWithoutScheme(t *testing.T) {
	for _, raw := range []string{"api:8080", "ftp://api:8080", "http://", "localhost"} {
		_, err := newAPIProxy(raw)
		assert.Error(t, err, raw)
	}

	_, err := newAPIProxy("http://localhost:8080")
	assert.NoError(t, err)
}
