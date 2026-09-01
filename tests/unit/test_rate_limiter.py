"""The Audible rate limiter must not hold a blocking lock across an await.

Two event loops share this module: uvicorn's, and the private background loop
used by run_coro_sync. Holding threading.Lock across `await` meant one loop could
stop the other's thread outright for the length of an HTTP request.
"""
import asyncio
import threading
import time

import pytest

import lib.audible_api_search as api


@pytest.fixture(autouse=True)
def restore_rate():
    previous_interval = api._min_interval
    previous_last = api._last_request_time
    yield
    api._min_interval = previous_interval
    api._last_request_time = previous_last


def test_lock_is_not_held_while_waiting():
    """_reserve_request_slot must return the delay rather than sleep under the lock."""
    api.set_rate(1)  # 1s interval
    api._last_request_time = time.monotonic()

    wait = api._reserve_request_slot()
    assert wait > 0, "expected the caller to be told to wait"
    # The lock must be free the instant the reservation returns.
    assert api._rate_lock.acquire(blocking=False), "rate lock was still held"
    api._rate_lock.release()


def test_concurrent_callers_get_distinct_slots():
    """Reservations must be spaced, not all handed the same instant."""
    api.set_rate(10)  # 0.1s interval
    api._last_request_time = time.monotonic()

    waits = [api._reserve_request_slot() for _ in range(4)]
    assert waits == sorted(waits)
    assert len(set(waits)) == len(waits), f"callers collided on the same slot: {waits}"


def test_a_second_event_loop_is_not_blocked_by_an_in_flight_request():
    """Regression: a slow request on one loop must not stall another loop."""
    api.set_rate(1000)  # effectively no throttling

    def slow_get(*args, **kwargs):
        time.sleep(0.5)
        return "response"

    api._last_request_time = 0.0
    background_done = threading.Event()

    def run_background():
        asyncio.run(_do_request())
        background_done.set()

    async def _do_request():
        # Mimic api_get's structure with a slow blocking call.
        wait = api._reserve_request_slot()
        if wait > 0:
            await asyncio.sleep(wait)
        await asyncio.to_thread(slow_get)

    thread = threading.Thread(target=run_background)
    thread.start()
    time.sleep(0.1)  # let the background request get in flight

    started = time.monotonic()
    api._reserve_request_slot()
    elapsed = time.monotonic() - started

    thread.join(timeout=5)
    assert background_done.is_set()
    assert elapsed < 0.2, f"reserving a slot blocked for {elapsed:.2f}s behind an in-flight request"
