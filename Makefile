pytest:
	uv run pytest

htmlcov: pytest
	uv run python -m http.server 80 -d htmlcov

build:
	uv build

check:
	uvx twine check dist/*

publish:
	uvx twine upload dist/*

publish-test:
	uvx twine upload --repository testpypi dist/*


up:
	docker compose up -d

down:
	docker compose down