import time
from datetime import timedelta


def safe_api_call(fetch_fn, max_retries=5, initial_wait=60):
    """Call an API with basic exponential backoff."""
    retries = 0
    while retries <= max_retries:
        try:
            return fetch_fn()
        except Exception as e:
            if "rate" in str(e).lower():
                wait_time = initial_wait * (2 ** retries)
                print(f"Rate limit hit. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                retries += 1
            else:
                raise
    raise Exception("Max retries exceeded due to rate limits.")


def get_dates_between(start_date, end_date):
    """Return a list of dates between `start_date` and `end_date` inclusive."""
    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current)
        current += timedelta(days=1)
    return dates
