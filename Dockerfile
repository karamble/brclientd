ARG BUILDKIT_CONTEXT_KEEP_GIT_DIR=1
ARG BR_VERSION=

FROM golang:1.25-alpine AS build
ARG BR_VERSION
RUN apk add --no-cache git
WORKDIR /src
COPY . .
RUN set -eu; \
    VERSION="$(git describe --tags --always --dirty 2>/dev/null || true)"; \
    if [ -z "$VERSION" ]; then VERSION="${BR_VERSION:-dev}"; fi; \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.Version=$VERSION" -o /out/brclientd ./cmd/brclientd

FROM alpine:latest
RUN apk add --no-cache ca-certificates tzdata
RUN adduser -D -u 1000 brclientd \
 && mkdir -p /home/brclientd/.brclientd /app-data/brclientd \
 && chown -R brclientd:brclientd /home/brclientd /app-data
COPY --from=build /out/brclientd /usr/local/bin/brclientd
USER brclientd
WORKDIR /home/brclientd
ENTRYPOINT ["/usr/local/bin/brclientd"]
