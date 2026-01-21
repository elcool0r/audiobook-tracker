#!/usr/bin/env bash
set -euo pipefail

# Run the project's tests in a way that avoids scanning the mongo-data folder
# Primary: run tests in `tests` and `tracker` directories (fast and reliable)
pytest tests tracker -q || pytest -q --ignore=mongo-data
