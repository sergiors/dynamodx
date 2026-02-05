pytest:
	uv run pytest

htmlcov: pytest
	uv run python -m http.server 80 -d htmlcov

up:
	docker compose up -d

down:
	docker compose down