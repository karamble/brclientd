ARG BR_VERSION=v0.2.4

FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/brclientd ./cmd/brclientd

FROM alpine:latest
RUN apk add --no-cache ca-certificates tzdata
RUN adduser -D -u 1000 brclientd \
 && mkdir -p /home/brclientd/.brclientd /app-data/brclientd \
 && chown -R brclientd:brclientd /home/brclientd /app-data
COPY --from=build /out/brclientd /usr/local/bin/brclientd
USER brclientd
WORKDIR /home/brclientd
ENTRYPOINT ["/usr/local/bin/brclientd"]
