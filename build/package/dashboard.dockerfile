# This expects the hatchet-lite image to be built and available on the machine
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
FROM --platform=$BUILDPLATFORM node:22-alpine AS frontend-build

WORKDIR /app

COPY ./frontend/app/package.json ./frontend/app/pnpm-lock.yaml ./
RUN corepack pnpm@10.16.1 --version
RUN corepack pnpm@10.16.1 install --frozen-lockfile && corepack pnpm@10.16.1 store prune

COPY ./frontend/app ./

RUN npm run build

# Stage 3: run in nginx alpine image
FROM nginx:alpine

ARG APP_TARGET=client
ENV BASE_PATH=/

COPY --from=api-binary-base /hatchet/hatchet-api ./hatchet-api
COPY ./build/package/dashboard-entrypoint.sh ./entrypoint.sh
COPY ./build/package/dashboard-nginx.conf /etc/nginx/nginx.conf

RUN rm -rf /usr/share/nginx/html/*
COPY --from=frontend-build /app/dist /usr/share/nginx/html

# Make entrypoint script executable
RUN chmod +x ./entrypoint.sh

EXPOSE 80

# Run the entrypoint script
CMD ["./entrypoint.sh"]
