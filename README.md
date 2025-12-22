# Audiobook Tracker v2

Audiobook Tracker is a web application designed to help users track their favorite audiobook series and receive notifications about new releases. It integrates with Audible to monitor series and provides both public frontpages for sharing and an admin interface for management.

## Features

- **Series Tracking**: Monitor audiobook series from Audible with automatic updates
- **Release Notifications**: Get notified when new books are released in tracked series
- **Public Frontpages**: Shareable public pages showing your tracked series and upcoming releases
- **Admin Interface**: Web-based admin panel for managing users, series, and jobs
- **API Access**: RESTful API for programmatic interaction
- **User Management**: Support for multiple users with customizable settings
- **Date Formatting**: Configurable date formats (US, DE, ISO)
- **Prometheus Metrics**: Built-in metrics for monitoring API usage and system performance
- **Responsive Design**: Mobile-friendly interface using Bootstrap

## Installation

### Prerequisites

- Docker and Docker Compose
- MongoDB (included in Docker setup)

### Quick Start with Docker

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/audiobook-tracker-v2.git
   cd audiobook-tracker-v2
   ```

2. Start the application:
   ```bash
   docker compose up -d
   ```

3. Access the application:
   - Admin interface: http://localhost:8000/config
   - Public pages: http://localhost:8000

### Manual Installation

1. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Set up MongoDB:
   ```bash
   # Using Docker
   docker run -d -p 27017:27017 --name mongodb mongo:7
   ```

3. Configure environment variables:
   ```bash
   export MONGO_URI=mongodb://localhost:27017
   export MONGO_DB=audiobook_tracker
   ```

4. Run the application:
   ```bash
   uvicorn tracker.app:app --host 0.0.0.0 --port 8000
   ```

## Usage

### Admin Interface

1. Navigate to `/config` and log in with admin credentials
2. Add users and configure their settings
3. Import series by ASIN or search Audible
4. Monitor background jobs for updates

### Public Frontpages

- Access public pages at `/` (if default slug is set) or `/home/{slug}`
- Share your tracking page with others
- View upcoming releases and series statistics

### API Usage

The application provides a REST API for integration:

- `GET /config/docs` - API documentation
- `GET /config/series` - List series
- `POST /config/series` - Add series
- `GET /config/users` - List users
- `POST /config/users` - Create user

### Monitoring

Prometheus metrics are available at `/metrics`:

```
# HELP audible_api_calls_total Total number of calls to Audible API
# TYPE audible_api_calls_total counter
audible_api_calls_total 42

# HELP series_count Total number of series
# TYPE series_count gauge
series_count 15

# HELP user_count Total number of users
# TYPE user_count gauge
user_count 3
```


## Configuration

### Environment Variables

- `MONGO_URI`: MongoDB connection string (default: mongodb://mongo:27017)
- `MONGO_DB`: Database name (default: audiobook_tracker)

### User Settings

- Date format: ISO, US (MM/DD/YYYY), or DE (DD.MM.YYYY)
- Frontpage slug: Custom URL slug for public page
- Notification preferences

## Development

### Project Structure

- `tracker/`: FastAPI backend with Jinja2 templates
- `tracker/static/`: CSS and static assets
- `tracker/templates/`: HTML templates
- `tool/`: Utility scripts for maintenance
- `docs/`: Static output directory

### Running in Development

```bash
# Install dependencies
pip install -r requirements.txt

# Start MongoDB
docker run -d -p 27017:27017 mongo:7

# Run with auto-reload
uvicorn tracker.app:app --reload
```

### Building Docker Image

```bash
docker build -t audiobook-tracker .
```

## API Documentation

Full API documentation is available at `/config/docs` when the application is running.

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