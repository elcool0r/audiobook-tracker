# Tracker Admin (FastAPI)

Run the development server:

```bash
pip install -r requirements.txt
uvicorn tracker.app:app --reload
```

- Login at http://127.0.0.1:8000/login using the default username `admin` and password `admin`.
- Update settings in the UI; settings are stored in `tracker_settings.json` in the repo root (you can change the admin password by editing the `admin_user.password_hash` value or by creating a new hashed password using the `tracker.auth.get_password_hash()` helper).
- The API exposes `/api/search` and `/api/product/{asin}` which use the project's `lib.audible_api_search` functions.
- Library page (`/library`) lets you search for series (similar to `--series` CLI) and add them to a per-user library stored in `library.json`.
- When adding a series, the tracker automatically fetches its books (like `--series-books <ASIN>`) and stores them; view them at `/series-books`.
