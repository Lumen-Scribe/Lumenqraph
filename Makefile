.PHONY: help db db-down build test fmt lint indexer api webhooks backfill up down

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

db: ## Start local Postgres
	docker compose up -d

db-down: ## Stop local Postgres
	docker compose down

build: ## Build the workspace
	cargo build --workspace

test: ## Run tests
	cargo test --workspace

fmt: ## Format
	cargo fmt --all

lint: ## Clippy (deny warnings)
	cargo clippy --workspace --all-targets -- -D warnings

indexer: ## Run the indexer (live)
	cargo run -p lumenqraph-indexer

backfill: ## Run backfill from START_LEDGER (make backfill LEDGER=123)
	cargo run -p lumenqraph-indexer -- backfill $(LEDGER)

api: ## Run the API
	cargo run -p lumenqraph-api

webhooks: ## Run the webhooks service
	cargo run -p lumenqraph-webhooks

up: ## Full stack in Docker
	docker compose -f docker-compose.full.yml up --build -d

down: ## Stop the full stack
	docker compose -f docker-compose.full.yml down
