#!/bin/sh

# Trap SIGTERM and SIGINT signals to gracefully shut down
trap 'shutdown' SIGTERM SIGINT

# Function to handle shutdown
shutdown() {
  if [ -n "$HATCHET_API_PID" ]; then
    echo "Gracefully shutting down hatchet-api..."
    kill -SIGTERM "$HATCHET_API_PID"

    # Wait for hatchet-api to exit
    wait "$HATCHET_API_PID"
  fi

  echo "Shutting down NGINX..."
  nginx -s quit

  # Exit the script
  exit 0
}

# Set HATCHET_API_UPSTREAM to proxy /api to an API running somewhere else, and
# this image serves the frontend alone. Deployments that scale the API on its
# own need this: without it the API can only have as many replicas as the
# frontend. Unset keeps the built-in API, so an existing deployment is unchanged.
if [ -n "$HATCHET_API_UPSTREAM" ]; then
  echo "Proxying /api to ${HATCHET_API_UPSTREAM}, not starting the built-in hatchet-api"
else
  HATCHET_API_UPSTREAM="http://localhost:8080"

  # Start hatchet-api with any passed command line arguments in the background
  ./hatchet-api "$@" &
  HATCHET_API_PID=$!
fi

cat > /etc/nginx/conf.d/hatchet-api.conf <<EOF
location /api {
    proxy_pass ${HATCHET_API_UPSTREAM};
    proxy_set_header Host \$host;
    proxy_set_header X-Real-IP \$remote_addr;
    proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto \$scheme;
}
EOF

: "${BASE_PATH:=/}"
case "$BASE_PATH" in /*) ;; *) BASE_PATH="/$BASE_PATH" ;; esac
case "$BASE_PATH" in */) ;; *) BASE_PATH="$BASE_PATH/" ;; esac

sed -i "s|{{ .BasePath }}|${BASE_PATH}|g" /usr/share/nginx/html/index.html

if [ "$BASE_PATH" = "/" ]; then
  cat > /etc/nginx/conf.d/hatchet-app.conf <<EOF
location / {
    try_files \$uri /index.html;
}
EOF
else
  cat > /etc/nginx/conf.d/hatchet-app.conf <<EOF
location = ${BASE_PATH%/} {
    return 302 ${BASE_PATH};
}
location ${BASE_PATH} {
    rewrite ^${BASE_PATH}(.*)\$ /\$1 break;
    try_files \$uri /index.html;
}
location = /index.html {
    internal;
}
location / {
    return 404;
}
EOF
fi

# Start NGINX in the foreground
nginx -g "daemon off;"
