# Target: aarch64-unknown-linux-gnu
# For: Raspberry Pi OS 64-bit, or another ARM64 board (Jetson, Orange Pi, etc.)
FROM --platform=linux/amd64 rust:slim-trixie

RUN apt-get update && apt-get install -y \
    gcc-aarch64-linux-gnu \
    pkg-config \
    cmake \
    perl \
    make \
    && rm -rf /var/lib/apt/lists/*

RUN rustup target add aarch64-unknown-linux-gnu

ENV CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc
ENV CC_aarch64_unknown_linux_gnu=aarch64-linux-gnu-gcc


WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --target aarch64-unknown-linux-gnu
