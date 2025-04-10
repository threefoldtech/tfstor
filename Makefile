# Makefile for threefoldtech/tfstor project

.PHONY: all build test test-respd-integration test-s3cas-integration clippy clean fmt run-respd run-s3cas

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

# Run respd integration tests
test-respd-integration:
	cargo test --test integration_test -p respd -- --test-threads=1

# Run s3cas integration tests
test-s3cas-integration:
	cargo test --test it_s3 -p s3cas -- --test-threads=1

# Clean build artifacts
clean:
	cargo clean

# Run the application (respd)
run-respd:
	cargo run -p respd

# Run the application (s3cas)
run-s3cas:
	cargo run -p s3cas
