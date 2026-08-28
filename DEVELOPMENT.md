# Development

This document contains development-specific information. For general usage and installation, see [README.md](README.md).

## Project Structure

- `tracker/`: FastAPI backend with Jinja2 templates
- `tracker/static/`: CSS and static assets
- `tracker/templates/`: HTML templates with Bootstrap components
- `docs/`: Screenshots used by the README
- `lib/`: Audible API integration and utilities
- `tests/`: `unit/`, `security/`, `operations/` and `integration/` suites

## Key Features

- **Interactive Charts**: Chart.js integration for series statistics
- **Collapsible UI**: Bootstrap collapse components for better UX
- **Rate Limiting**: Account lockout protection for login security
- **Developer Tools**: Advanced testing controls when developer mode is enabled

## Running Tests

The project includes comprehensive tests covering unit tests, integration tests, and build verification.

### Local Testing

Run all tests locally with the provided script:

```bash
./test.sh
```

This will:
- Start a test MongoDB instance
- Run unit tests
- Run integration tests
- Test Docker build
- Clean up test resources

### Test Categories

- **Unit Tests**: `tests/unit/` - pure helpers (date intervals, proxy config, rate
  limiting, route registration, version resolution)
- **Security Tests**: `tests/security/` - authorization, secret exposure, log
  escaping, database configuration and user lifecycle invariants. These run
  against a real in-memory database and real session cookies rather than
  dependency overrides, so a missing role check actually fails the suite.
- **Operations Tests**: `tests/operations/` - API behaviour with mocked collections
- **Sweep Tests**: `tracker/test_release_sweep.py` - release notification sweep
- **Integration Tests**: `tests/integration/` - Tests full application functionality including:
  - App startup and health checks
  - Authentication (login/logout)
  - Page access (all main pages)
  - API endpoints
  - Metrics endpoint
  - Static file serving
  - Error handling

### CI/CD Testing

Tests are automatically run in GitHub Actions on:
- Push to `master` or `dev` branches
- Tag pushes (v* and dev-*)
- Manual workflow dispatch

The CI pipeline includes:
- Unit and integration test execution
- Docker build verification
- Image building and pushing to GHCR

### Running the suite directly

```bash
pip install -r requirements-dev.txt
python -m pytest
```

Tests need a database. Without `MONGO_URI` the suite uses an in-memory backend,
enabled by `ALLOW_IN_MEMORY_DB=1` in the root `conftest.py`. Production refuses to
start without a reachable `MONGO_URI` — it will not silently fall back.

## Docker Compose for Development

For development with local code changes, use the provided `docker-compose.dev.yml`:

```bash
docker compose -f docker-compose.dev.yml up -d
```

This will build the image locally and mount the source code for live reloading.

## API Documentation

Full API documentation is available at `/config/docs` when the application is running and developer mode is enabled in user settings.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Built with FastAPI, MongoDB, and Bootstrap
- Audible integration for series data
- Inspired by the need for better audiobook tracking tools