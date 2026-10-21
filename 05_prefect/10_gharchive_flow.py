import gzip
import json
from collections import Counter
import requests
from prefect import flow, task


URL = "https://data.gharchive.org/2025-01-01-{hour}.json.gz"


@task(retries=3)
def count_events_for_hour(hour: int) -> dict[str, int]:
    """Stream one hourly GH Archive file and count events by type.

    Args:
        hour: Hour of the day (0-23) to download for 2025-01-01.

    Returns:
        Dict mapping event type (e.g. "PushEvent") to its count.
    """
    with requests.get(URL.format(hour=hour), stream=True, timeout=60) as r:
        # Fail loudly on bad HTTP status so Prefect's retries kick in
        r.raise_for_status()
        counts = Counter()
        # Decompress the gzip stream as it arrives instead of loading it all into memory
        with gzip.GzipFile(fileobj=r.raw) as gz:
            for line in gz:
                counts[json.loads(line)["type"]] += 1
    print(f"hour {hour}: {counts.total()} events")
    return dict(counts)


@task
def combine_counts(hourly_counts: list[dict[str, int]]) -> dict[str, int]:
    """Sum per-hour event counts into overall totals, ordered by count.

    Args:
        hourly_counts: List of {event_type: count} dicts, one per hour.

    Returns:
        Dict of {event_type: total_count}, sorted highest count first.
    """
    totals = Counter()
    for counts in hourly_counts:
        totals.update(counts)  # update() adds counts instead of replacing them
    # most_common() returns (event_type, count) pairs sorted by count, descending
    ordered = dict(totals.most_common())
    for event_type, count in ordered.items():
        print(f"{event_type}: {count}")
    return ordered


@flow(log_prints=True)
def gharchive_flow(hours: list[int] = list(range(1, 4))):
    # take the range and create a prefect .map over the hours
    hourly = count_events_for_hour.map(hours).result()
    totals = combine_counts(hourly)
    for k, v in totals.items():
        print(k, v)


if __name__ == "__main__":
    gharchive_flow()
