# Build

## Requirements

- Go 1.26 or later

## Compile

```
go build ./cmd/brclientd
```

The binary is pure Go (no cgo). Cross-compile with the usual GOOS/GOARCH
environment variables.

## Version stamp

Releases stamp the version string from git:

```
go build -ldflags "-X main.Version=$(git describe --tags --always --dirty)" ./cmd/brclientd
```

`brclientd --version` prints the stamped value.

## Container image

```
docker build -t brclientd:dev .
```

The Dockerfile builds a static binary in a golang alpine stage, stamps the
version from `git describe`, and copies it into a minimal alpine runtime
image that runs as an unprivileged user.
