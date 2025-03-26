.PHONY: help setup fetch-notion-data clean format test

help:
	@echo "Available commands:"
	@echo "  setup           - Install project dependencies using Poetry"
	@echo "  fetch-notion-data - Run the Notion data fetch pipeline"
	@echo "  fetch-notion-data-no-cache - Run the Notion data fetch pipeline without cache"
	@echo "  clean           - Remove temporary files and artifacts"
	@echo "  format          - Format code using Black"
	@echo "  test            - Run tests using pytest"

setup:
	poetry install

fetch-notion-data:
	poetry run python -m brain_ai_assistant.tools.run --run-fetch-notion-data-pipeline

fetch-notion-data-no-cache:
	poetry run python -m brain_ai_assistant.tools.run --run-fetch-notion-data-pipeline --no-cache

clean:
	rm -rf .pytest_cache
	rm -rf .coverage
	rm -rf htmlcov
	rm -rf dist
	rm -rf build
	rm -rf *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type d -name "*.pyc" -delete

format:
	poetry run black .

test:
	poetry run pytest