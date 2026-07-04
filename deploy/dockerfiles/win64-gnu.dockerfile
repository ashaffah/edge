FROM --platform=linux/amd64 rust:slim-bookworm

RUN apt-get update && apt-get install -y \
    gcc-mingw-w64-x86-64 \
    pkg-config \
    cmake \
    perl \
    make \
    && rm -rf /var/lib/apt/lists/*

# Symlink Windows import libs with the casing the windows-sys crate expects.
RUN cd /usr/x86_64-w64-mingw32/lib && \
    for lib in User32 Kernel32 Ws2_32 Ntdll Userenv DbgHelp Crypt32 RpcRT4; do \
        lower=$(echo "$lib" | tr '[:upper:]' '[:lower:]'); \
        ln -sf "lib${lower}.a" "lib${lib}.a" 2>/dev/null || true; \
    done

RUN rustup target add x86_64-pc-windows-gnu

ENV CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER=x86_64-w64-mingw32-gcc
ENV CC_x86_64_pc_windows_gnu=x86_64-w64-mingw32-gcc

# -ladvapi32: Windows Advanced API — needed by some Windows crates
# (registry, crypto, event logging). The linker won't error if it's unused.
ENV RUSTFLAGS="-C link-arg=-ladvapi32"

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --target x86_64-pc-windows-gnu