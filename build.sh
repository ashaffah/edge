#!/usr/bin/env bash
# build.sh <target> [crate...]
#
# Arguments:
#   <target>   — platform target (required)
#   [crate...] — crate whose binary is copied to bin/ (optional, default: all)
#
# Available targets:
#   armv7-musl      — Raspberry Pi OS 32-bit, static binary (recommended)
#   armv7-glibc     — Raspberry Pi OS 32-bit, dynamic glibc
#   aarch64         — Raspberry Pi OS 64-bit or another ARM64 board
#   x86_64          — Linux x86_64, dynamic glibc
#   x86_64-musl     — Linux x86_64, static binary
#   win-x86_64-gnu  — Windows x86_64, cross-compile via mingw-w64
#   win-x86_64-msvc — Windows x86_64, cross-compile via cargo-xwin (MSVC ABI)
#
# Examples:
#   bash build.sh armv7-musl
#   bash build.sh armv7-musl edge-client
#   bash build.sh win-x86_64-gnu edge-client
#
# Run from the project root (the directory containing Cargo.toml).

set -euo pipefail

# ---------------------------------------------------------------------------
# Crate registry — add an entry here when the project gains a new binary.
# Not using declare -A so it stays compatible with bash 3.2 (macOS default).
# ---------------------------------------------------------------------------

ALL_CRATES=(edge-client)

# Return the binary name for a crate, or exit 1 if unknown.
crate_to_binary() {
    case "$1" in
        edge-client)    echo "edge-client" ;;
        *)              return 1 ;;
    esac
}

# ---------------------------------------------------------------------------
# Usage
# ---------------------------------------------------------------------------

usage() {
    echo "Usage: bash build.sh <target> [crate...]"
    echo ""
    echo "Target:"
    echo "  armv7-musl        Raspberry Pi OS 32-bit, static (recommended)"
    echo "  armv7-glibc       Raspberry Pi OS 32-bit, dynamic glibc"
    echo "  aarch64           Raspberry Pi OS 64-bit / ARM64"
    echo "  x86_64            Linux x86_64, dynamic glibc"
    echo "  x86_64-musl       Linux x86_64, static"
    echo "  win-x86_64-gnu    Windows x86_64, cross-compile via mingw-w64"
    echo "  win-x86_64-msvc   Windows x86_64, cross-compile via cargo-xwin (MSVC)"
    echo ""
    echo "Crate (optional, default: all):"
    for c in "${ALL_CRATES[@]}"; do echo "  $c"; done
    echo ""
    echo "Examples:"
    echo "  bash build.sh armv7-musl"
    echo "  bash build.sh armv7-musl edge-client"
    echo "  bash build.sh win-x86_64-gnu edge-client"
}

# ---------------------------------------------------------------------------
# Parse the target (first arg)
# ---------------------------------------------------------------------------

TARGET="${1:-}"
if [[ -z "$TARGET" ]]; then
    usage
    exit 1
fi
shift   # drop TARGET from $@; the rest is the crate list

case "$TARGET" in
  armv7-musl)
    DOCKERFILE="deploy/dockerfiles/armv7-musl.dockerfile"
    RUST_TARGET="armv7-unknown-linux-musleabihf"
    ;;
  armv7-glibc)
    DOCKERFILE="deploy/dockerfiles/armv7-glibc.dockerfile"
    RUST_TARGET="armv7-unknown-linux-gnueabihf"
    ;;
  aarch64)
    DOCKERFILE="deploy/dockerfiles/aarch64.dockerfile"
    RUST_TARGET="aarch64-unknown-linux-gnu"
    ;;
  x86_64)
    DOCKERFILE="deploy/dockerfiles/x86_64.dockerfile"
    RUST_TARGET="x86_64-unknown-linux-gnu"
    ;;
  x86_64-musl)
    DOCKERFILE="deploy/dockerfiles/x86_64-musl.dockerfile"
    RUST_TARGET="x86_64-unknown-linux-musl"
    ;;
  win-x86_64-gnu)
    DOCKERFILE="deploy/dockerfiles/win64-gnu.dockerfile"
    RUST_TARGET="x86_64-pc-windows-gnu"
    ;;
  win-x86_64-msvc)
    DOCKERFILE="deploy/dockerfiles/win64-msvc.dockerfile"
    RUST_TARGET="x86_64-pc-windows-msvc"
    ;;
  *)
    echo "error: unknown target '$TARGET'"
    echo "Valid targets: armv7-musl  armv7-glibc  aarch64  x86_64  x86_64-musl  win-x86_64-gnu  win-x86_64-msvc"
    exit 1
    ;;
esac

# Binary extension — Windows uses .exe, other platforms don't.
case "$TARGET" in
  win-*) BIN_EXT=".exe" ;;
  *)     BIN_EXT=""     ;;
esac

# ---------------------------------------------------------------------------
# Parse the crates (remaining args). Default: all.
# ---------------------------------------------------------------------------

if [[ $# -eq 0 ]]; then
    SELECTED_CRATES=("${ALL_CRATES[@]}")
else
    SELECTED_CRATES=("$@")
fi

# Validation: reject unknown crate names
for crate in "${SELECTED_CRATES[@]}"; do
    if ! crate_to_binary "$crate" > /dev/null 2>&1; then
        echo "error: unknown crate '$crate'"
        echo "Available: ${ALL_CRATES[*]}"
        exit 1
    fi
done

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

mkdir -p bin

IMAGE="rust-builder-$TARGET"
CONTAINER="rust-build-$TARGET"

cleanup() { podman rm -f "$CONTAINER" 2>/dev/null || true; }
trap cleanup EXIT

echo "==> Building workspace for target: $TARGET"
echo "    Crates to copy: ${SELECTED_CRATES[*]}"
podman build -t "$IMAGE" -f "$DOCKERFILE" .

echo "==> Extracting binaries..."
podman create --name "$CONTAINER" "$IMAGE"

for crate in "${SELECTED_CRATES[@]}"; do
    binary=$(crate_to_binary "$crate")
    src="/app/target/$RUST_TARGET/release/${binary}${BIN_EXT}"
    dst="./bin/${binary}-${TARGET}${BIN_EXT}"
    podman cp "$CONTAINER:$src" "$dst"
    echo "    $dst"
done

echo "==> Done."
