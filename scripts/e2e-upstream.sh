#!/bin/sh
# Runs the upstream bisonrelay end-to-end suite (internal/e2etests) against the
# module version this repo pins, or against a checkout given with -dir. All
# remaining arguments are passed to go test (e.g. -race -v -run TestCanPM);
# BR_E2E_LOG=1 enables the suite's client logs.
set -e

cd "$(dirname "$0")/.."

dir=""
if [ "$1" = "-dir" ]; then
	dir="$2"
	shift 2
fi

if [ -z "$dir" ]; then
	go mod download github.com/companyzero/bisonrelay
	src=$(go list -m -f '{{.Dir}}' github.com/companyzero/bisonrelay)
	if [ -z "$src" ]; then
		echo "cannot resolve the pinned bisonrelay module" >&2
		exit 1
	fi
	tmp=$(mktemp -d)
	trap 'rm -rf "$tmp"' EXIT
	cp -a "$src/." "$tmp/"
	chmod -R u+w "$tmp"
	dir="$tmp"
	echo "running the upstream e2e suite from $(basename "$src")"
fi

cd "$dir"
exec go test ./internal/e2etests/ -count=1 "$@"
