# This expects the hatchet-api image to be built and available on the machine
# -------------------
ARG HATCHET_API_IMAGE

# Stage 1: copy from the existing Go built image
# Resolved per target platform, so an arm64 image takes the arm64 binary.
FROM $HATCHET_API_IMAGE AS api-binary-base

# Stage 2: build the frontend
# --platform=$BUILDPLATFORM: the bundle this stage produces is just files, the
# same for every target, so building it once on the builder's own architecture
# is both correct and the only thing that works. Emulated, pnpm dies with
# `qemu: uncaught target signal 4 (Illegal instruction)` and the build fails.
FROM --platform=$BUILDPLATFORM node:24-alpine AS frontend-build

WORKDIR /app

COPY ./frontend/app/package.json ./frontend/app/pnpm-lock.yaml ./
RUN corepack pnpm@10.16.1 --version
RUN corepack pnpm@10.16.1 install --frozen-lockfile && corepack pnpm@10.16.1 store prune

COPY ./frontend/app ./

RUN npm run build

# Stage 3: build the static fileserver
# --platform=$BUILDPLATFORM keeps the Go toolchain native on the builder, and
# GOOS/GOARCH cross-compile for the target. Without it a multi-arch build
# compiles the arm64 binary under emulation.
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS staticfileserver

WORKDIR /app

ENV CGO_ENABLED=0

ARG FIPS=false

COPY go.mod go.sum ./
COPY ./cmd/hatchet-staticfileserver/ ./cmd/hatchet-staticfileserver/

ARG TARGETOS
ARG TARGETARCH

RUN if [ "$FIPS" = "true" ]; then export GOFIPS140=v1.0.0; fi && \
    GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -ldflags="-w -s" -a -o hatchet-staticfileserver ./cmd/hatchet-staticfileserver && \
    if [ "$FIPS" = "true" ]; then \
      go version -m ./hatchet-staticfileserver | grep -q 'GOFIPS140=v1.0.0-c2097c7c$' && go version -m ./hatchet-staticfileserver | grep -Eq 'DefaultGODEBUG=.*fips140=on(,|$)' || { echo "hatchet-staticfileserver is not linked against the validated FIPS module"; exit 1; }; \
    fi

# Stage 4: deployment image
FROM alpine:3.24 AS deployment

ARG FIPS=false
LABEL run.hatchet.fips=${FIPS}

ENV BASE_PATH=/

WORKDIR /hatchet

RUN apk update && apk add --no-cache ca-certificates tzdata

COPY --from=api-binary-base /hatchet/hatchet-api ./hatchet-api
COPY --from=staticfileserver /app/hatchet-staticfileserver ./hatchet-staticfileserver
COPY --from=frontend-build /app/dist ./html
COPY ./build/package/dashboard-entrypoint.sh ./entrypoint.sh

RUN chmod +x ./entrypoint.sh

EXPOSE 80

CMD ["./entrypoint.sh"]
