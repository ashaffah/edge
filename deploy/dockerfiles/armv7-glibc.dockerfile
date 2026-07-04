# Target: armv7-unknown-linux-gnueabihf
# For: Raspberry Pi OS 32-bit with glibc (dynamic linking).
# Note: the binary depends on the build host's glibc version. If the Pi has an
# older glibc than the build host, it errors with "GLIBC_x.xx not found".
# Use armv7-musl to avoid this problem.
FROM --platform=linux/amd64 rust:slim-trixie

RUN apt-get update && apt-get install -y \
    gcc-arm-linux-gnueabihf \
    pkg-config \
    cmake \
    perl \
    make \
    && rm -rf /var/lib/apt/lists/*

RUN rustup target add armv7-unknown-linux-gnueabihf

ENV CARGO_TARGET_ARMV7_UNKNOWN_LINUX_GNUEABIHF_LINKER=arm-linux-gnueabihf-gcc
ENV CC_armv7_unknown_linux_gnueabihf=arm-linux-gnueabihf-gcc


WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --target armv7-unknown-linux-gnueabihf
