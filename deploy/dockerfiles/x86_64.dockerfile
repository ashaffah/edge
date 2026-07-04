# Target: x86_64-unknown-linux-gnu
# For: Linux x86_64 native (server, desktop, CI)
FROM --platform=linux/amd64 rust:slim-trixie

RUN apt-get update && apt-get install -y \
    gcc \
    pkg-config \
    cmake \
    perl \
    make \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --target x86_64-unknown-linux-gnu