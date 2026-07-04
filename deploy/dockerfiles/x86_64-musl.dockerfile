# Target: x86_64-unknown-linux-musl
# For: Linux x86_64, static binary with no glibc dependency
FROM --platform=linux/amd64 rust:slim-trixie

RUN apt-get update && apt-get install -y \
    musl-tools \
    pkg-config \
    cmake \
    perl \
    make \
    && rm -rf /var/lib/apt/lists/*

RUN rustup target add x86_64-unknown-linux-musl

ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=musl-gcc
ENV CC_x86_64_unknown_linux_musl=musl-gcc


WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --target x86_64-unknown-linux-musl