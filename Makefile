.PHONY : build

build:
	@echo "Building edge-client..."
	./build.sh aarch64 edge-client && ./build.sh armv7-musl edge-client && ./build.sh x86_64-musl edge-client && ./build.sh win-x86_64-msvc edge-client
	@echo "Building iot-middleware..."
	./build.sh x86_64-musl iot-middleware
