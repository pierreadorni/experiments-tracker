.PHONY: setup setup-demo check format audit demo

setup-demo:
	uv sync --locked
	npm ci --prefix web
	npm run build --prefix web

setup:
	uv sync --locked
	npm ci --prefix web
	npm ci --prefix gateway
	npm ci --prefix tools/codex

check:
	uv run --locked ruff check src tests scripts
	uv run --locked ruff format --check src tests scripts
	uv run --locked pytest -q
	npm run format:check --prefix web
	npm test --prefix web
	npm run build --prefix gateway
	npm run typecheck --prefix gateway
	npm test --prefix gateway

format:
	uv run --locked ruff format src tests scripts
	npm run format --prefix web

audit:
	npm audit --prefix web
	npm audit --prefix gateway

demo:
	uv run --locked experimenttracker --config .local/demo.yaml demo --max-seconds 60
