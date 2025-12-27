# Development

This document contains development-specific information. For general usage and installation, see [README.md](README.md).

## Project Structure

- `tracker/`: FastAPI backend with Jinja2 templates
- `tracker/static/`: CSS and static assets
- `tracker/templates/`: HTML templates with Bootstrap components
- `tool/`: Utility scripts for maintenance
- `docs/`: Static output directory
- `lib/`: Audible API integration and utilities

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

- **Unit Tests**: `tracker/test_release_flow.py`, `tracker/test_release_sweep.py`
- **Integration Tests**: `tracker/test_integration.py` - Tests full application functionality including:
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