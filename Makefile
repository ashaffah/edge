.PHONY : build

build:
	@echo "Building edge-client..."
	./build.sh aarch64 && ./build.sh armv7-musl && ./build.sh x86_64-musl && ./build.sh win-x86_64-msvc