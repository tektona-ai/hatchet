package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"time"

	"github.com/hatchet-dev/hatchet/cmd/hatchet-staticfileserver/staticfileserver"
)

func main() {
	port := flag.String("port", "80", "port to listen on")
	staticAssetDir := flag.String("static-asset-dir", ".", "directory to serve static assets from")
	basePath := flag.String("base-path", envOrDefault("BASE_PATH", "/"), "base path the app is served under (e.g. /hatchet); defaults to $BASE_PATH, or / if unset")
	apiProxy := flag.String("api-proxy", "", "if set, reverse-proxy /api requests to this URL (e.g. http://localhost:8080)")
	flag.Parse()

	c := staticfileserver.NewStaticFileServer(*staticAssetDir, *basePath)

	if *apiProxy != "" {
		proxy, err := newAPIProxy(*apiProxy)
		if err != nil {
			log.Printf("invalid -api-proxy url: %s", err.Error())
			os.Exit(1)
		}

		c.Handle("/api", proxy)
		c.Handle("/api/*", proxy)
	}

	s := &http.Server{
		Addr:              fmt.Sprintf(":%s", *port),
		Handler:           c,
		ReadHeaderTimeout: 5 * time.Second,
	}

	if err := s.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("static file server failure: %s", err.Error())
		os.Exit(1)
	}
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// newAPIProxy returns a reverse proxy to rawURL. It keeps the incoming
// X-Forwarded-For chain and appends the peer address, as nginx's
// $proxy_add_x_forwarded_for does. Rewrite strips the incoming chain by
// default, and without it the API sees every client behind an ingress as the
// ingress address.
func newAPIProxy(rawURL string) (*httputil.ReverseProxy, error) {
	target, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}

	// url.Parse accepts "api:8080" as scheme "api", which would fail on every
	// request instead of at startup.
	if (target.Scheme != "http" && target.Scheme != "https") || target.Host == "" {
		return nil, fmt.Errorf("%q must be an http:// or https:// URL with a host", rawURL)
	}

	return &httputil.ReverseProxy{
		Rewrite: func(r *httputil.ProxyRequest) {
			r.SetURL(target)
			r.Out.Host = r.In.Host
			r.Out.Header["X-Forwarded-For"] = r.In.Header["X-Forwarded-For"]
			r.SetXForwarded()
		},
	}, nil
}
