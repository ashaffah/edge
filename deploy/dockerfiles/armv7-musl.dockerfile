# Target: armv7-unknown-linux-musleabihf
# For: Raspberry Pi OS 32-bit (armhf userspace), static binary with no glibc dep.
# Suitable for a Pi whose uname -m = aarch64 but with 32-bit userspace.
FROM --platform=linux/amd64 rust:slim-trixie

RUN apt-get update && apt-get install -y \
    curl \
    pkg-config \
    cmake \
    perl \
    make \
    && rm -rf /var/lib/apt/lists/*

RUN curl -L https://musl.cc/arm-linux-musleabihf-cross.tgz | tar -xz -C /opt
ENV PATH="/opt/arm-linux-musleabihf-cross/bin:$PATH"

RUN rustup target add armv7-unknown-linux-musleabihf

ENV CARGO_TARGET_ARMV7_UNKNOWN_LINUX_MUSLEABIHF_LINKER=arm-linux-musleabihf-gcc
ENV CC_armv7_unknown_linux_musleabihf=arm-linux-musleabihf-gcc
ENV AR_armv7_unknown_linux_musleabihf=arm-linux-musleabihf-ar

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --target armv7-unknown-linux-musleabihf
