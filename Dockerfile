# syntax=docker/dockerfile:1

# One image runs both processes: `ingestion serve` and `ingestion worker run`.
# The Go stage's tag is the toolchain go.mod names.
FROM golang:1.26.11 AS build
WORKDIR /src
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=bind,source=go.mod,target=go.mod \
    --mount=type=bind,source=go.sum,target=go.sum \
    go mod download
# The release names the version, commit and commit date it builds. A build
# that names none of them reports a dev build.
ARG VERSION=dev
ARG COMMIT=unknown
ARG DATE=unknown
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=bind,target=. \
    CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w -X github.com/datapointchris/ingestion/cmd.version=${VERSION} -X github.com/datapointchris/ingestion/cmd.commit=${COMMIT} -X github.com/datapointchris/ingestion/cmd.date=${DATE}" \
      -o /out/ingestion .

# Debian rather than a static base, because the worker runs extractors whose
# libraries are Python as programs in this image.
FROM debian:trixie-slim

LABEL org.opencontainers.image.source="https://github.com/datapointchris/ingestion" \
      org.opencontainers.image.description="ingestion: lands each item from an outside source once, and delivers it to the one place it belongs" \
      org.opencontainers.image.licenses="MIT"

# A uid of its own, so a file it writes to a mounted volume reads as
# ingestion's. The store and the blobs live on a volume mounted at
# /var/lib/ingestion.
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates tini \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10600 ingestion \
    && useradd --system --uid 10600 --gid ingestion --no-create-home --shell /usr/sbin/nologin ingestion \
    && install -d -o ingestion -g ingestion /var/lib/ingestion /etc/ingestion

COPY --from=build /out/ingestion /usr/local/bin/ingestion

# Numeric, so a pod with runAsNonRoot can verify it without reading /etc/passwd.
USER 10600:10600
ENV XDG_STATE_HOME=/var/lib \
    XDG_CONFIG_HOME=/etc
EXPOSE 8080

ENTRYPOINT ["/usr/bin/tini", "--", "ingestion"]
CMD ["serve"]
