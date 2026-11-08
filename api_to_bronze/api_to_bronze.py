import json
import os
import random
import time
import requests
from databricks import sql

SOURCE = "open-meteo"
URL = "https://api.open-meteo.com/v1/forecast"
PARAMS = {"latitude": 40.71, "longitude": -74.01, "hourly": "temperature_2m"}

RETRYABLE_STATUS = {429, 500, 502, 503, 504}


def fetch_with_retry(url, params, max_attempts=5, base_delay=1.0, timeout=10):
    for attempt in range(1, max_attempts + 1):
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            if resp.status_code in RETRYABLE_STATUS:
                raise requests.HTTPError(f"retryable status {resp.status_code}")
            resp.raise_for_status()  # other 4xx fail immediately, no retry
            return resp.json()
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as exc:
            # don't retry non-retryable client errors
            if isinstance(exc, requests.HTTPError) and exc.response is not None:
                if exc.response.status_code not in RETRYABLE_STATUS:
                    raise
            if attempt == max_attempts:
                raise
            delay = base_delay * 2 ** (attempt - 1) + random.uniform(0, 1)  # backoff + jitter
            print(f"attempt {attempt} failed ({exc}); retrying in {delay:.1f}s")
            time.sleep(delay)


def land_to_bronze(payload):
    with sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    ) as conn, conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS workspace.bronze")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS workspace.bronze.weather_raw (
                raw_payload STRING,
                _source     STRING,
                _load_date  DATE,
                _loaded_at  TIMESTAMP
            )
        """)
        cur.execute(
            """
            INSERT INTO workspace.bronze.weather_raw
            VALUES (:payload, :source, current_date(), current_timestamp())
            """,
            {"payload": json.dumps(payload), "source": SOURCE},
        )


if __name__ == "__main__":
    data = fetch_with_retry(URL, PARAMS)
    land_to_bronze(data)
    print("landed 1 row")