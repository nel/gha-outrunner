#!/bin/bash
set -euo pipefail

# Builds the hoplum branch (upstream plus our carried patches) and publishes the
# binaries as a release on nel/gha-outrunner, tagged <upstream tag>-hoplum.<n>.
# Run from a clean checkout of the hoplum branch that is pushed to origin.

repo=nel/gha-outrunner
cd "$(git rev-parse --show-toplevel)"

[[ "$(git branch --show-current)" == hoplum ]] || { echo "check out the hoplum branch" >&2; exit 1; }
[[ -z "$(git status --porcelain)" ]] || { echo "working tree is not clean" >&2; exit 1; }
git fetch -q origin hoplum
[[ "$(git rev-parse HEAD)" == "$(git rev-parse origin/hoplum)" ]] || { echo "push hoplum to origin first" >&2; exit 1; }

base=$(git describe --tags --abbrev=0 --match 'v[0-9]*.[0-9]*.[0-9]*' --exclude '*-hoplum.*')
n=1
while git rev-parse -q --verify "refs/tags/$base-hoplum.$n" >/dev/null; do n=$((n + 1)); done
version="$base-hoplum.$n"

go test ./...

rm -rf dist && mkdir dist
for target in linux/amd64 linux/arm64 darwin/arm64; do
	CGO_ENABLED=0 GOOS=${target%/*} GOARCH=${target#*/} \
		go build -trimpath -ldflags "-s -w -X main.version=${version#v}" \
		-o "dist/outrunner-${target%/*}-${target#*/}" ./cmd/outrunner
done
(cd dist && shasum -a 256 outrunner-* > checksums.txt)

notes=$(printf 'Upstream %s plus:\n\n%s\n' "$base" \
	"$(git log --merges --format='- %s' "$base..HEAD")")
git tag -a "$version" -m "$version"
git push -q origin "$version"
gh release create "$version" -R "$repo" --target hoplum --title "$version" \
	--notes "$notes" dist/outrunner-* dist/checksums.txt
