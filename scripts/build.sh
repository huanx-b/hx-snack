#!/bin/bash
set -e

# Cross-compile for all supported platforms
PLATFORMS=(
  "linux/amd64"
  "linux/arm64"
)

VERSION=${1:-dev}

for platform in "${PLATFORMS[@]}"; do
  GOOS=${platform%/*}
  GOARCH=${platform#*/}
  echo "Building: $GOOS/$GOARCH"

  GOOS=$GOOS GOARCH=$GOARCH go build -ldflags="-s -w -X main.Version=$VERSION" -o "dist/mother-${GOOS}-${GOARCH}" ./cmd/mother/
  GOOS=$GOOS GOARCH=$GOARCH go build -ldflags="-s -w -X child.Version=$VERSION" -o "dist/child-${GOOS}-${GOARCH}" ./cmd/child/
done

echo "All builds complete:"
ls -lh dist/