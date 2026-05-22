ARG BR_VERSION=v0.2.4

FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/brclientd ./cmd/brclientd

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates tzdata \
 && rm -rf /var/lib/apt/lists/*
RUN useradd --create-home --uid 1000 brclientd \
 && mkdir -p /home/brclientd/.brclientd \
 && chown -R brclientd:brclientd /home/brclientd
COPY --from=build /out/brclientd /usr/local/bin/brclientd
USER brclientd
WORKDIR /home/brclientd
ENTRYPOINT ["/usr/local/bin/brclientd"]
