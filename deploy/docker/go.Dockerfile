# Shared multi-stage Dockerfile for all three Go services.
# Build:  docker build -f deploy/docker/go.Dockerfile --build-arg SERVICE=orchestrator .
# Result: a non-root scratch image containing one static binary + CA certs.

# syntax=docker/dockerfile:1

FROM golang:1.26-alpine AS build
ARG SERVICE
WORKDIR /src

# Layer-cached dependency download.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build \
    -trimpath -ldflags="-s -w" \
    -o /out/service ./cmd/${SERVICE}

# Non-root user compiled into the final image's /etc/passwd.
RUN echo "app:x:10001:10001::/:/sbin/nologin" > /out/passwd

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/passwd /etc/passwd
COPY --from=build /out/service /service
USER app
ENTRYPOINT ["/service"]
