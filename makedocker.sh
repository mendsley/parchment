#!/bin/bash
#
# Usage: makedocker.sh <tag>
set -eu

if [ -z "${1:-}" ]; then
	echo "Usage: $0 <docker tag>" >&2
	exit 1
fi

docker rm -f temp-parchment-build || true
docker run -e CGO_ENABLED=0 -e GOOS=linux --name temp-parchment-build -i -v $(pwd):/go/src/github.com/mendsley/parchment golang:1.6.2 go install -v github.com/mendsley/parchment github.com/mendsley/parchment/cmd/...
docker cp temp-parchment-build:/go/bin/parchment docker/
docker cp temp-parchment-build:/go/bin/parchmentcat docker/
docker rm -f temp-parchment-build

docker build -t $1 docker
rm docker/parchment docker/parchmentcat
