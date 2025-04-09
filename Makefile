# Makefile for threefoldtech/tfstor project

.PHONY: all build test clippy clean fmt

# Default target
all: build test

# Build the project
build:
	cargo build --workspace

# Build with release profile
release:
	cargo build --workspace --release

# Run tests
test:
	cargo test --workspace

# Run clippy lints with warnings as errors
clippy:
	cargo clippy --workspace --all-features -- -Dwarnings

# Run clippy without treating warnings as errors
clippy-check:
	cargo clippy --workspace --all-features

# Format code
fmt:
	cargo fmt --all

# Check formatting without modifying files
fmt-check:
	cargo fmt --all -- --check

# Clean build artifacts
clean:
	cargo clean

# Run the application (respcas)
run-respcas:
	cargo run -p respcas

# Run the application (s3cas)
run-s3cas:
	cargo run -p s3cas
