.PHONY: build build-release build-ui test test-all lint fmt check clean \
       run run-ui demo docker help

# Default target
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

build: ## Build the Rust backend (debug)
	cargo build

build-release: ## Build the Rust backend (release, optimized)
	cargo build --release

build-ui: ## Build the web UI for production
	cd ui && npm install && npm run build

build-all: build build-ui ## Build both backend and UI

# ---------------------------------------------------------------------------
# Test & Lint
# ---------------------------------------------------------------------------

test: ## Run Rust tests (excludes ignored/Postgres tests)
	cargo test

test-all: ## Run all Rust tests including Postgres (requires Docker)
	cargo test -- --include-ignored

lint: ## Run clippy linter with strict warnings
	cargo clippy --all-targets -- -D warnings

fmt: ## Format Rust code
	cargo fmt --all

fmt-check: ## Check Rust code formatting
	cargo fmt --all -- --check

check: fmt-check lint test ## Run all checks (format, lint, test)

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

FINDB_PORT ?= 3001

run: ## Run the findb server (debug build, port=$(FINDB_PORT))
	cargo run -- --port $(FINDB_PORT)

run-release: ## Run the findb server (release build, port=$(FINDB_PORT))
	cargo run --release -- --port $(FINDB_PORT)

run-ui: ## Start the Vite dev server for the UI
	cd ui && npm install && FINDB_API_URL=http://localhost:$(FINDB_PORT) npm run dev

# ---------------------------------------------------------------------------
# Demo — builds and runs everything
# ---------------------------------------------------------------------------

demo: build build-ui ## Build everything, then run backend + UI together
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
		sleep 2 && cd ui && FINDB_API_URL=http://localhost:$(FINDB_PORT) npm run dev & \
		wait

# ---------------------------------------------------------------------------
# Docker
# ---------------------------------------------------------------------------

docker: ## Build Docker image
	docker build -t findb:latest .

docker-run: docker ## Build and run Docker container
	docker run --rm -p $(FINDB_PORT):3000 findb:latest

# ---------------------------------------------------------------------------
# Bench & Clean
# ---------------------------------------------------------------------------

bench: ## Run benchmarks
	cargo bench

clean: ## Remove build artifacts
	cargo clean
	rm -rf ui/node_modules ui/dist
