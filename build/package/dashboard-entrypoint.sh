#!/bin/sh

# Trap SIGTERM and SIGINT signals to gracefully shut down
trap 'shutdown' TERM INT

# Function to handle shutdown
shutdown() {
  if [ -n "$HATCHET_API_PID" ]; then
    echo "Gracefully shutting down hatchet-api..."
    kill -TERM "$HATCHET_API_PID"

    # Wait for hatchet-api to exit
    wait "$HATCHET_API_PID"
  fi

  echo "Shutting down static file server..."
  kill -TERM "$STATIC_PID"
  wait "$STATIC_PID"

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
  HATCHET_API_UPSTREAM="http://localhost:${SERVER_PORT:-8080}"

  # Start hatchet-api with any passed command line arguments in the background
  ./hatchet-api "$@" &
  HATCHET_API_PID=$!
fi

./hatchet-staticfileserver -port 80 -static-asset-dir ./html -api-proxy "$HATCHET_API_UPSTREAM" &
STATIC_PID=$!

if [ -z "$HATCHET_API_PID" ]; then
  wait "$STATIC_PID"; rc=$?
  echo "static file server exited with status $rc"
  exit "$rc"
fi

while kill -0 "$HATCHET_API_PID" 2>/dev/null && kill -0 "$STATIC_PID" 2>/dev/null; do
  sleep 1
done

if ! kill -0 "$HATCHET_API_PID" 2>/dev/null; then
  wait "$HATCHET_API_PID"; rc=$?
  echo "hatchet-api exited with status $rc"
  kill -TERM "$STATIC_PID" 2>/dev/null; wait "$STATIC_PID"
else
  wait "$STATIC_PID"; rc=$?
  echo "static file server exited with status $rc"
  kill -TERM "$HATCHET_API_PID" 2>/dev/null; wait "$HATCHET_API_PID"
fi

exit "$rc"
