# Gemini Workspace Context

This file helps Gemini understand the project's context, conventions, and configurations.

## Project Overview

*   **Description:** Model Context Protocol server for Base dos Dados (Brazilian open data platform)
*   **Technology Stack:** Python, FastAPI, Pydantic, Google Cloud BigQuery
*   **Team Conventions:**
    *   Code formatting: `black`
    *   Linting: `ruff`
    *   Commit message format: Conventional Commits

## Development Environment

*   **Setup:** `pip install -e .[dev]`
*   **Run:** `uvicorn server:app --reload`
*   **Test:** `pytest`
*   **Lint:** `ruff check .` and `black .`

## Important Notes

*   The application uses a `.env` file for environment variables.
*   The main application logic is in the `server.py` file.