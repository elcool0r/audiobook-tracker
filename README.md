# Audiobook Tracker

Audiobook Tracker is a web application designed to help users track their favorite Audible audiobook series and receive notifications about new releases. 
It uses the public Audible API to monitor series and provides both public frontpages for sharing and an admin interface for management. 

It does not pirate or download any actual audiobooks. Get an Audible subscription ffs.
## Features

- **Series Tracking**: Monitor audiobook series from Audible with automatic updates
- **Release Notifications**: Get notified when new books are released in tracked series
- **User Frontpages**: Shareable pages showing your tracked series and upcoming releases
- **Admin Interface**: Web-based admin panel for managing users, series, and jobs
- **Library Management**: Browse and search your personal audiobook collection
- **Series Statistics**: Interactive charts showing release dates, runtime trends, and ratings over time
- **API Access**: RESTful API for programmatic interaction
- **User Management**: Support for multiple users with customizable settings
- **Date Formatting**: Configurable date formats (US, DE, ISO)
- **Developer Mode**: Advanced testing controls
- **Security Features**: Account lockout after failed login attempts, rate limiting
- **Prometheus Metrics**: Built-in metrics for monitoring API usage and system performance
- **Versioning**: Application and Docker images are versioned for easy deployment and rollback

## Screenshots

Here are screenshots of the public frontpage and the series admin view (files included in the repo under `docs/screenshots/`).

Frontpage

![Frontpage screenshot](docs/screenshots/frontpage.png)

Series admin view

![Series admin screenshot](docs/screenshots/series-admin.png)

## Installation

### Prerequisites

- Docker and Docker Compose
- MongoDB (included in Docker setup)

### Quick Start with Docker

Create a `docker-compose.yml` file with the following content (this uses the production image):

```yaml
services:
  mongo:
    image: mongo:7
    restart: unless-stopped
    environment:
      MONGO_INITDB_DATABASE: audiobook_tracker
      MONGO_INITDB_ROOT_USERNAME: root
      MONGO_INITDB_ROOT_PASSWORD: changeme123
    volumes:
      - ./mongo-data:/data/db
    healthcheck:
      test: ["CMD", "mongosh", "--eval", "db.adminCommand('ping')"]
      interval: 10s
      timeout: 5s
      retries: 5

  tracker:
    image: ghcr.io/elcool0r/audiobook-tracker:latest
    depends_on:
      mongo:
        condition: service_healthy
    environment:
      MONGO_URI: mongodb://root:changeme123@mongo:27017
      MONGO_DB: audiobook_tracker
      SECRET_KEY: supersecretkey123456789
    ports:
      - "8000:8000"
```

Then run:

```bash
docker compose up -d
```

Access the application:
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
   - Default username: `admin`
   - Default password: `admin`
2. Add users and configure their settings
3. Import series by ASIN or search Audible
4. Monitor background jobs for updates
5. Browse your library
6. View detailed statistics and charts for each series

### Library Management

- Access your personal library at `/config/library`
- Search and filter your audiobook collection
- View series statistics with interactive charts

### Public Frontpages

- Access public pages at `/` (if default slug is set) or `/home/{slug}`
- Share your tracking page with others
- View upcoming releases and series statistics

### API Usage

The application provides a REST API for integration. Enable developer mode in settings to access API documentation.

- `GET /config/docs` - API documentation (requires developer mode)
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
- `ADMIN_USERNAME`: Default admin username (default: admin)
- `ADMIN_PASSWORD`: Default admin password (default: admin)

### User Settings

- Date format: ISO, US (MM/DD/YYYY), or DE (DD.MM.YYYY)
- Frontpage slug: Custom URL slug for public page
- Notification preferences
- Developer mode: Enable advanced testing controls and API access

### Notification Setup (Apprise)

The application uses Apprise for sending notifications. Configure your notification URLs in the user settings. Here are examples for popular services:

#### Discord
```
discord://webhook_id/webhook_token
```
*Get webhook URL from Server Settings → Integrations → Webhooks → New Webhook*

#### Slack
```
slack://token_a/token_b/token_c
```
*Get tokens from Slack App settings (OAuth & Permissions)*

#### Telegram
```
tgram://bot_token/chat_id
```
*Create a bot with @BotFather, get bot token, then send /start to bot to get chat_id*

#### Email
```
mailto://user:password@example.com
```
*Use your SMTP server details*

#### Pushover
```
pover://user_key@token
```
*Get user key and create an app for the token from Pushover.net*

#### Gotify
```
gotify://hostname/token
```
*Create an app in Gotify web interface to get the token*

#### Matrix
```
matrix://user:password@hostname?webhook=room_alias
```
*Use your Matrix server details and room alias*

#### Microsoft Teams
```
msteams://webhook_url
```
*Create a webhook connector in Teams channel settings*

#### Webhook (Generic)
```
json://https://your-webhook-url.com
```
*Any HTTP endpoint that accepts JSON POST requests*

Multiple notification URLs can be combined with commas. Test your configuration using the "Test Notification" button in user settings.

## Development

For development information, see [DEVELOPMENT.md](DEVELOPMENT.md).

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