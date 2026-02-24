.PHONY: build build-release build-ui build-all test test-all lint fmt fmt-check \
       check run run-release run-ui demo docker docker-run bench clean help

FINDB_PORT ?= 3001

# Default target
help: ## Show available targets
	@echo Available targets:
	@echo   build          Build the Rust backend (debug)
	@echo   build-release  Build the Rust backend (release)
	@echo   build-ui       Build the web UI for production
	@echo   build-all      Build both backend and UI
	@echo   test           Run Rust tests
	@echo   test-all       Run all tests including Postgres
	@echo   lint           Run clippy linter
	@echo   fmt            Format Rust code
	@echo   fmt-check      Check Rust code formatting
	@echo   check          Run fmt-check + lint + test
	@echo   run            Run findb server (debug)
	@echo   run-release    Run findb server (release)
	@echo   run-ui         Start the Vite dev server
	@echo   demo           Build and run backend + UI (Unix only; use make.ps1 on Windows)
	@echo   docker         Build Docker image
	@echo   docker-run     Build and run Docker container
	@echo   bench          Run benchmarks
	@echo   clean          Remove build artifacts

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

build:
	cargo build

build-release:
	cargo build --release

build-ui:
	cd ui && npm install && npm run build

build-all: build build-ui

# ---------------------------------------------------------------------------
# Test & Lint
# ---------------------------------------------------------------------------

test:
	cargo test

test-all:
	cargo test -- --include-ignored

lint:
	cargo clippy --all-targets -- -D warnings

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all -- --check

check: fmt-check lint test

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

run:
	cargo run -- --port $(FINDB_PORT)

run-release:
	cargo run --release -- --port $(FINDB_PORT)

run-ui:
	cd ui && npm run dev

# ---------------------------------------------------------------------------
# Demo (Unix only — use ./make.ps1 demo on Windows)
# ---------------------------------------------------------------------------

demo: build build-ui
	@echo ""
	@echo "============================================"
	@echo "  Starting FinanceDB demo"
	@echo "  Backend: http://localhost:$(FINDB_PORT)"
	@echo "  UI:      http://localhost:5173"
	@echo "  Press Ctrl+C to stop"
	@echo "============================================"
	@echo ""
	@trap 'kill 0' EXIT; \
		cargo run -- --port $(FINDB_PORT) & \
		sleep 2 && cd ui && npm run dev & \
		wait

# ---------------------------------------------------------------------------
# Docker
# ---------------------------------------------------------------------------

docker:
	docker build -t findb:latest .

docker-run: docker
	docker run --rm -p $(FINDB_PORT):3000 findb:latest

# ---------------------------------------------------------------------------
# Bench & Clean
# ---------------------------------------------------------------------------

bench:
	cargo bench

clean:
	cargo clean
