# syntax=docker/dockerfile:1
# ---------------------------------------------------------------------------
# Stage 1 — build
# Uses a Debian-based Go image so CGO (required by go-sqlite3) works cleanly.
# ---------------------------------------------------------------------------
FROM golang:1.22-bookworm AS builder

# Install gcc/libc-dev for CGO
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc libc6-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Cache dependency downloads separately from source
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY . .

# Build a statically-linked binary.
# CGO_ENABLED=1 is required for go-sqlite3.
RUN CGO_ENABLED=1 GOOS=linux \
    go build \
        -ldflags="-s -w" \
        -o /cas \
        ./cmd/cas

# ---------------------------------------------------------------------------
# Stage 2 — runtime
# Debian slim keeps glibc and wget (for healthcheck); ~30 MB total image.
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        wget \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /data

COPY --from=builder /cas /usr/local/bin/cas
COPY healthcheck.sh /usr/local/bin/cas-healthcheck
RUN chmod +x /usr/local/bin/cas-healthcheck

# Data directory — mount a host path here to persist the SQLite database
VOLUME ["/data"]

EXPOSE 8080

# Graceful shutdown via SIGTERM
STOPSIGNAL SIGTERM

# Environment variable defaults (all overridable via docker run / Compose)
ENV CAS_PORT=8080 \
    CAS_DB_PATH=/data/cas.db \
    CAS_AGING_WEIGHT=1.0 \
    CAS_MAX_QUEUE=100

HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
    CMD ["/usr/local/bin/cas-healthcheck"]

ENTRYPOINT ["/usr/local/bin/cas"]
