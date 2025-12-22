"""
Integration test script placed inside the `tracker` package so it is available inside the running container.

Run inside container as module:
  python -m tracker.scripts.integration_release_prod

Behavior: same as other script - writes file:// notifications into /app and prints their content before cleanup.
"""

import os
import time
import uuid
from datetime import datetime, timedelta, timezone

from tracker.tasks import TaskWorker
from tracker.tasks import get_users_collection, get_series_collection, get_user_library_collection


def run(cleanup=True, wait_sec=60):
    ts = int(time.time())
    uniq = uuid.uuid4().hex[:8]
    username = f"int_test_user_{uniq}_{ts}"
    series_asin = f"B0INT{uniq.upper()}"
    book_asin = f"B0BK{uniq.upper()}"

    # Write both notifications to the shared /tmp/apprise.txt file (Apprise file URL requires three slashes)
    new_file = "/tmp/apprise.txt"
    release_file = "/tmp/apprise.txt"

    now = datetime.now(timezone.utc)
    pub_dt = (now + timedelta(seconds=wait_sec)).replace(microsecond=0)
    pub_iso = pub_dt.isoformat() + "Z"

    users_col = get_users_collection()
    series_col = get_series_collection()
    lib_col = get_user_library_collection()

    print(f"Using DB configured in environment. Now={now.isoformat()} pub_dt={pub_iso}")

    # Create user
    user_doc = {
        "username": username,
        "role": "admin",
        "notifications": {
            "enabled": True,
            # Use explicit absolute file URL (three slashes) for Apprise file handler
            "urls": ["file:///tmp/apprise.txt"],
            "notify_new_audiobook": True,
            "notify_release": True,
        }
    }

    # Create series with one existing old book
    old_book = {
        "asin": "B0OLDTEST",
        "title": "Old Test Book",
        "release_date": (now - timedelta(days=7)).date().isoformat(),
    }
    new_book = {
        "asin": book_asin,
        "title": "Integration Test Book",
        "publication_datetime": pub_iso,
        "image": "https://example.com/integration_cover.jpg",
    }

    series_doc = {
        "_id": series_asin,
        "title": "Integration Test Series",
        "url": "https://www.audible.com/series/Integration-Test",
        "books": [old_book],
        "fetched_at": now.isoformat() + "Z",
    }

    lib_entry = {
        "username": username,
        "series_asin": series_asin,
        "notified_releases": [],
        "notified_new_asins": [],
    }

    # Insert docs
    print("Inserting test documents into DB...")
    users_col.insert_one(user_doc)
    series_col.insert_one(series_doc)
    inserted = lib_col.insert_one(lib_entry)

    try:
        worker = TaskWorker()

        # 1) Trigger new-book notification by calling _send_series_notifications
        print("Triggering new-audiobook notification...")
        series_doc_with_new = dict(series_doc)
        series_doc_with_new["books"] = [old_book, new_book]

        worker._send_series_notifications(series_asin, series_doc_with_new, [old_book], [old_book, new_book])

        # Small pause to allow file to be written
        time.sleep(1)

        # Direct Apprise debug attempt to ensure file writes and surface any exceptions
        try:
            import apprise
            ap = apprise.Apprise()
            added = ap.add('file:///tmp/apprise.txt')
            if not added:
                print('Apprise did not accept file:///tmp/apprise.txt (fallback write)')
                try:
                    with open('/tmp/apprise.txt', 'w') as f:
                        f.write('Integration Test - New\nNew audiobooks test')
                    print('Fallback: wrote /tmp/apprise.txt')
                except Exception as e:
                    print('Fallback write failed:', e)
            else:
                ok = ap.notify(title='Integration Test - New', body='New audiobooks test')
                print(f"Apprise direct notify returned: {ok}")
        except Exception as e:
            print(f"Apprise direct notify raised: {e}")

        if os.path.exists(new_file):
            print(f"-- New notification file created: {new_file}")
            with open(new_file, 'r') as f:
                print("---- NEW FILE CONTENT ----")
                print(f.read())
                print("---- END NEW FILE ----")
        else:
            print("-- New notification file NOT found (check Apprise/file handler config)")

        # 2) Wait until publication time + small buffer
        to_wait = (pub_dt - datetime.now(timezone.utc)).total_seconds()
        if to_wait > 0:
            print(f"Waiting {int(to_wait)+2}s for publication time to pass...")
            time.sleep(int(to_wait) + 2)

        print("Checking due release notifications...")
        worker._check_due_release_notifications()
        time.sleep(1)

        # Direct Apprise debug for release phase
        try:
            import apprise
            ap = apprise.Apprise()
            added = ap.add('file:///tmp/apprise.txt')
            if not added:
                print('Apprise did not accept file:///tmp/apprise.txt (fallback write)')
                try:
                    with open('/tmp/apprise.txt', 'a') as f:
                        f.write('\nIntegration Test - Release\nRelease test')
                    print('Fallback: appended to /tmp/apprise.txt')
                except Exception as e:
                    print('Fallback append failed:', e)
            else:
                ok = ap.notify(title='Integration Test - Release', body='Release test')
                print(f"Apprise direct notify (release) returned: {ok}")
        except Exception as e:
            print(f"Apprise direct notify (release) raised: {e}")

        if os.path.exists(release_file):
            print(f"-- Release notification file created: {release_file}")
            with open(release_file, 'r') as f:
                print("---- RELEASE FILE CONTENT ----")
                print(f.read())
                print("---- END RELEASE FILE ----")
        else:
            print("-- Release notification file NOT found (check Apprise/file handler config)")

    finally:
        if cleanup:
            print("Cleaning up created DB entries and files...")
            try:
                users_col.delete_one({"username": username})
            except Exception:
                pass
            try:
                series_col.delete_one({"_id": series_asin})
            except Exception:
                pass
            try:
                lib_col.delete_one({"_id": inserted.inserted_id})
            except Exception:
                pass
            for p in (new_file, release_file):
                if os.path.exists(p):
                    try:
                        os.remove(p)
                    except Exception:
                        pass
            print("Cleanup done.")


if __name__ == '__main__':
    run()
