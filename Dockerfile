# Multi-stage build for AEGIS Runner Engine
# Stage 1: Build static Go binary
FROM golang:alpine AS builder

WORKDIR /src

# Install build tools & CA certificates
RUN apk add --no-cache git ca-certificates

# Copy source tree
COPY . .

# Compile static binary with zero external runtime dependencies
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w -extldflags '-static'" \
    -o /bin/aegis \
    ./cmd/aegis

# Stage 2: Production lean runtime container
FROM alpine:3.20

# Create unprivileged non-root service user (UID/GID 1001)
RUN addgroup -g 1001 -S aegis && \
    adduser -u 1001 -S aegis -G aegis && \
    apk add --no-cache ca-certificates tzdata

WORKDIR /app

# Copy binary from builder stage
COPY --from=builder /bin/aegis /app/aegis

# Set file permissions for non-root execution
RUN chown -R aegis:aegis /app

# Non-root user enforcement (Security Rubric Pillar 3)
USER 1001:1001

EXPOSE 8080

HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
    CMD wget --quiet --tries=1 --spider http://localhost:8080/healthz || exit 1

ENTRYPOINT ["/app/aegis"]
