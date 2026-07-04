FROM --platform=linux/amd64 rust:slim-bookworm

RUN apt-get update && apt-get install -y \
    clang \
    lld \
    llvm \
    python3 \
    pkg-config \
    cmake \
    perl \
    make \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN rustup target add x86_64-pc-windows-msvc

RUN cargo install cargo-xwin

ENV CARGO_TARGET_X86_64_PC_WINDOWS_MSVC_LINKER=lld-link
ENV CC_x86_64_pc_windows_msvc=clang-cl
ENV CXX_x86_64_pc_windows_msvc=clang-cl

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo xwin build --release --target x86_64-pc-windows-msvc