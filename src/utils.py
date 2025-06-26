import time
import logging
import sys
from datetime import timedelta


def setup_logging(level=logging.INFO):
    """Configure logging to stdout with a standard format."""
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        stream=sys.stdout,
    )


logger = logging.getLogger(__name__)


def safe_api_call(fetch_fn, max_retries=5, initial_wait=60):
    """Call an API with basic exponential backoff."""
    retries = 0
    while retries <= max_retries:
        try:
            return fetch_fn()
        except Exception as e:
            if "rate" in str(e).lower():
                wait_time = initial_wait * (2 ** retries)
                logger.warning("Rate limit hit. Retrying in %s seconds...", wait_time)
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
