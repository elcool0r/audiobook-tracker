# tracker package

FastAPI application package. See [../README.md](../README.md) for installation and
configuration, and [../DEVELOPMENT.md](../DEVELOPMENT.md) for development setup.

## Layout

- `app.py` — application factory, page routes, login/logout, metrics
- `api.py` — JSON API, mounted at `/config/api`
- `auth.py` — password hashing, session JWTs, account lockout, auth logging
- `db.py` — MongoDB accessors
- `settings.py` — global settings model, persisted in the `settings` collection
- `library.py` — series/book documents and the Audible fetch pipeline
- `tasks.py` — background job worker, refresh scheduler, notification sweepers
- `frontpage.py` — public frontpage rendering
- `inactive_series.py` — release-activity classification and interval arithmetic
- `audiobookshelf.py` — Audiobookshelf client used for series suggestions

## Notes

- Settings live in MongoDB, not on disk. `SECRET_KEY`, `ADMIN_USERNAME` and
  `ADMIN_PASSWORD` come from the environment.
- The UI and API are served under `/config`; public pages are at `/` and
  `/home/{slug}`.
- `tasks.py` runs three daemon threads inside the web process, so the app must be
  run with a single worker. Running multiple uvicorn workers would give each one
  its own scheduler and notifier, producing duplicate notifications and duplicate
  Audible traffic.
