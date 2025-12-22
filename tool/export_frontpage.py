#!/usr/bin/env python3
"""
Export the public frontpage as a standalone static HTML file suitable for GitHub Pages.

Usage:
  MONGO_URI=mongodb://localhost:27017 MONGO_DB=audiobook_tracker \
  python tool/export_frontpage.py --slug <frontpage-slug-or-username> --out docs/index.html

Notes:
- Uses tracker/templates/frontpage_export.html (self-contained CSS, no external assets required)
- Reads data from MongoDB using the same logic as the app's frontpage
- If --out is a directory, writes index.html inside it
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path
from datetime import datetime

from jinja2 import Environment, FileSystemLoader, select_autoescape

# Ensure repository root is on sys.path when running as a script (python tool/export_frontpage.py)
import sys
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tracker.db import get_users_collection
from tracker.library import get_user_library, visible_books

TEMPLATES_DIR = Path(__file__).resolve().parent.parent / "tracker" / "templates"
STATIC_DIR = Path(__file__).resolve().parent.parent / "tracker" / "static"
EXPORT_TEMPLATE = "frontpage.html"


def _parse_date(s: str | None) -> datetime | None:
    try:
        return datetime.fromisoformat((s or "").split("T")[0])
    except Exception:
        return None


def _format_dt(dt: datetime | None, date_format: str) -> str:
    if not dt:
        return "—"
    def pad(n: int) -> str:
        return str(n).zfill(2)
    if date_format == "de":
        return f"{pad(dt.day)}.{pad(dt.month)}.{dt.year} {pad(dt.hour)}:{pad(dt.minute)}"
    if date_format == "us":
        return f"{pad(dt.month)}/{pad(dt.day)}/{dt.year} {pad(dt.hour)}:{pad(dt.minute)}"
    return f"{dt.date().isoformat()} {pad(dt.hour)}:{pad(dt.minute)}"


def _format_d(dt: datetime | None, date_format: str) -> str:
    if not dt:
        return "—"
    def pad(n: int) -> str:
        return str(n).zfill(2)
    if date_format == "de":
        return f"{pad(dt.day)}.{pad(dt.month)}.{dt.year}"
    if date_format == "us":
        return f"{pad(dt.month)}/{pad(dt.day)}/{dt.year}"
    return dt.date().isoformat()


def _format_runtime(val) -> str | None:
    try:
        m = int(val or 0)
    except Exception:
        return None
    if m <= 0:
        return None
    h = m // 60
    mins = m % 60
    return f"{h}h {mins}m" if h else f"{mins}m"


def build_frontpage_context(slug: str) -> dict:
    if not slug:
        raise ValueError("slug/username is required")

    users_col = get_users_collection()
    user_doc = users_col.find_one({"$or": [{"frontpage_slug": slug}, {"username": slug}]})
    if not user_doc:
        raise RuntimeError(f"User for slug '{slug}' not found")

    username = user_doc.get("username")
    date_format = user_doc.get("date_format", "iso")
    library = get_user_library(username)

    now = datetime.utcnow()
    upcoming_cards = []
    latest_cards = []
    series_rows = []
    total_books = 0
    last_refresh_dt = None

    for it in library:
        books = it.books if isinstance(it.books, list) else []
        visible = visible_books(books)
        total_books += len(visible)
        if it.fetched_at:
            dt = _parse_date(it.fetched_at)
            if dt and (not last_refresh_dt or dt > last_refresh_dt):
                last_refresh_dt = dt
        series_last_release = None
        for b in visible:
            rd = _parse_date(getattr(b, "release_date", None))
            if not rd:
                continue
            if rd <= now and (not series_last_release or rd > series_last_release):
                series_last_release = rd
            book_url = getattr(b, "url", None)
            if not book_url and getattr(b, "asin", None):
                book_url = f"https://www.audible.com/pd/{getattr(b, 'asin', '')}"
            if rd > now:
                days = (rd - now).days + (1 if (rd - now).seconds > 0 else 0)
                runtime_str = _format_runtime(getattr(b, "runtime", None))
                upcoming_cards.append({
                    "title": getattr(b, "title", None) or it.title,
                    "series": it.title,
                    "narrators": getattr(b, "narrators", None) or "",
                    "runtime": getattr(b, "runtime", None) or "",
                    "runtime_str": runtime_str,
                    "release_dt": rd,
                    "release_str": _format_d(rd, date_format),
                    "days_left": days,
                    "image": getattr(b, "image", None),
                    "url": book_url,
                })
            else:
                days_ago = (now - rd).days
                runtime_str = _format_runtime(getattr(b, "runtime", None))
                latest_cards.append({
                    "title": getattr(b, "title", None) or it.title,
                    "series": it.title,
                    "narrators": getattr(b, "narrators", None) or "",
                    "runtime": getattr(b, "runtime", None) or "",
                    "runtime_str": runtime_str,
                    "release_dt": rd,
                    "release_str": _format_d(rd, date_format),
                    "days_ago": days_ago,
                    "image": getattr(b, "image", None),
                    "url": book_url,
                })
        narr_set = set()
        runtime_mins = 0
        for b in visible:
            if getattr(b, "narrators", None):
                for n in str(getattr(b, "narrators", "")).split(","):
                    n = n.strip()
                    if n:
                        narr_set.add(n)
            try:
                runtime_mins += int(getattr(b, "runtime", None) or 0)
            except Exception:
                pass
        hours = runtime_mins // 60
        mins = runtime_mins % 60
        runtime_str = f"{hours}h {mins}m" if hours else f"{mins}m"
        cover = None
        for b in visible:
            if getattr(b, "image", None):
                cover = getattr(b, "image", None)
                break
        if not cover:
            for b in books:
                if getattr(b, "image", None):
                    cover = getattr(b, "image", None)
                    break
        last_release_str = _format_d(series_last_release, date_format)
        last_release_ts = series_last_release.isoformat() if series_last_release else None
        series_rows.append({
            "title": it.title,
            "asin": it.asin,
            "narrators": ", ".join(sorted(narr_set)),
            "book_count": len(visible),
            "runtime": runtime_str,
            "cover": cover,
            "last_release": last_release_str,
            "last_release_ts": last_release_ts,
            "duration_minutes": runtime_mins,
            "url": it.url,
        })

    upcoming_cards.sort(key=lambda x: x["release_dt"])
    latest_cards.sort(key=lambda x: x["release_dt"], reverse=True)
    latest_cards = latest_cards[:4]
    series_rows.sort(key=lambda x: (x["title"] or ""))

    stats = {
        "series_count": len(library),
        "books_count": total_books,
        "last_refresh": _format_dt(last_refresh_dt, date_format),
        "slug": user_doc.get("frontpage_slug") or username,
        "username": username,
    }

    return {
        "public_nav": True,
        "brand_title": "Audiobook Tracker",
        "stats": stats,
        "upcoming": upcoming_cards,
        "latest": latest_cards,
        "series": series_rows,
    }


def render_export_html(context: dict) -> str:
    """Render frontpage.html with inlined CSS to make it standalone."""
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATES_DIR)),
        autoescape=select_autoescape(["html", "xml"]),
    )
    
    # Read custom CSS
    custom_css_path = STATIC_DIR / "css" / "custom.css"
    custom_css = custom_css_path.read_text(encoding="utf-8") if custom_css_path.exists() else ""
    
    # Render just the frontpage content block (not the full base.html)
    tmpl = env.get_template(EXPORT_TEMPLATE)
    # Set hide_nav and public_nav to avoid rendering nav from base template
    content_context = context.copy()
    content_context["hide_nav"] = True
    content_context["base_path"] = ""
    rendered_content = tmpl.render(**content_context)
    
    bootstrap_css_url = "https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css"
    bootstrap_js_url = "https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"
    
    # Build a standalone HTML with navbar
    standalone_html = f"""<!doctype html>
<html lang="en" data-bs-theme="dark">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Audiobook Tracker - Frontpage</title>
    <link href="{bootstrap_css_url}" rel="stylesheet">
    <style>
{custom_css}
    </style>
  </head>
  <body>
    <script>
      window.BASE_PATH = "";
      window.apiPath = (p) => p;
    </script>
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark">
      <div class="container-fluid">
        <a class="navbar-brand" href="#">Audiobook Tracker</a>
        <div class="collapse navbar-collapse">
          <ul class="navbar-nav ms-auto align-items-center">
            <li class="nav-item">
              <a class="nav-link" href="#" id="darkModeToggle" title="Toggle dark mode">
                <span id="darkModeIcon">☀️</span>
              </a>
            </li>
          </ul>
        </div>
      </div>
    </nav>
    <main class="container py-4">
{rendered_content}
    </main>
    <script src="{bootstrap_js_url}"></script>
    <script>
      // Dark mode toggle
      const htmlEl = document.documentElement;
      const toggleBtn = document.getElementById('darkModeToggle');
      const icon = document.getElementById('darkModeIcon');
      
      // Load saved preference or default to dark
      const savedTheme = localStorage.getItem('theme') || 'dark';
      htmlEl.setAttribute('data-bs-theme', savedTheme);
      updateIcon(savedTheme);
      
      toggleBtn.addEventListener('click', (e) => {{
        e.preventDefault();
        const currentTheme = htmlEl.getAttribute('data-bs-theme');
        const newTheme = currentTheme === 'dark' ? 'light' : 'dark';
        htmlEl.setAttribute('data-bs-theme', newTheme);
        localStorage.setItem('theme', newTheme);
        updateIcon(newTheme);
      }});
      
      function updateIcon(theme) {{
        icon.textContent = theme === 'dark' ? '☀️' : '🌙';
      }}
    </script>
  </body>
</html>
"""
    return standalone_html


def main() -> None:
    parser = argparse.ArgumentParser(description="Export the public frontpage as static HTML for GitHub Pages")
    parser.add_argument("--slug", required=True, help="Frontpage slug or username to export")
    parser.add_argument("--out", default="docs/index.html", help="Output file path (or directory)")
    args = parser.parse_args()

    out_path = Path(args.out)
    if out_path.exists() and out_path.is_dir():
        out_path = out_path / "index.html"
    elif not out_path.suffix:
        # If no suffix is provided and path doesn't exist, assume directory
        out_path.mkdir(parents=True, exist_ok=True)
        out_path = out_path / "index.html"
    else:
        out_path.parent.mkdir(parents=True, exist_ok=True)

    context = build_frontpage_context(args.slug)
    html = render_export_html(context)
    out_path.write_text(html, encoding="utf-8")
    print(f"Exported frontpage for '{args.slug}' to {out_path}")


if __name__ == "__main__":
    main()
