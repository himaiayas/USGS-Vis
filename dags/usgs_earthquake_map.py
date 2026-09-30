"""On-demand earthquake dataset + map: trigger with filters, get every matching event.

Trigger from the UI ("Trigger DAG" -> fill the form) and pick:
  - region          world or one of the boxes in usgs_common.REGIONS
  - start_date / end_date   UTC days, inclusive (empty = last 7 days)
  - min_magnitude / max_magnitude

USGS refuses any single query matching more than 20000 events, so the run first asks
the /count endpoint and bisects the time range until every window fits, then fetches
the windows in parallel (dynamic task mapping) and merges them. Nothing is dropped.

Outputs land in logs/earthquake_maps/ on the host (mounted from /opt/airflow/logs):
  <name>.csv    the complete dataset, one row per event
  <name>.html   interactive report: map (size = magnitude, color = depth), events over
                time, magnitude histogram, strongest-events table, in-page filters
  latest.html / latest.csv   copies of the newest run
Chunks travel between tasks as files on that shared mount, not through XCom.
"""

import csv
import json
import logging
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pendulum
import requests
from airflow.sdk import Param, dag, task
from usgs_common import QUERY_URL, REGIONS

log = logging.getLogger(__name__)

COUNT_URL = QUERY_URL.replace("/query", "/count")
MAX_PER_QUERY = 20000  # USGS hard cap per request
OUTPUT_DIR = Path("/opt/airflow/logs/earthquake_maps")
CHUNK_DIR = OUTPUT_DIR / "_chunks"
TEMPLATE = Path(__file__).parent / "templates" / "earthquake_map.html"
FIELDS = ["time_ms", "mag", "lat", "lon", "depth_km", "tsunami", "id", "place"]


def _base_query(filters: dict) -> dict:
    query = {
        "format": "geojson",
        "minmagnitude": filters["min_mag"],
        "maxmagnitude": filters["max_mag"],
        "eventtype": "earthquake",
    }
    if filters["bbox"]:
        query.update(zip(("minlatitude", "maxlatitude", "minlongitude", "maxlongitude"), filters["bbox"]))
    return query


def _get(url: str, params: dict) -> requests.Response:
    resp = requests.get(url, params=params, timeout=300)
    resp.raise_for_status()
    return resp


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["usgs", "earthquake", "viz"],
    params={
        "region": Param("world", enum=["world", *REGIONS], description="Area to map"),
        "start_date": Param(
            None, type=["null", "string"], format="date",
            description="First day (UTC). Empty = 6 days before end_date.",
        ),
        "end_date": Param(
            None, type=["null", "string"], format="date",
            description="Last day, inclusive (UTC). Empty = today.",
        ),
        "min_magnitude": Param(2.5, type="number", minimum=-1, maximum=10),
        "max_magnitude": Param(10.0, type="number", minimum=-1, maximum=10),
    },
)
def usgs_earthquake_map():

    @task
    def resolve_filters(params=None, run_id=None) -> dict:
        end = pendulum.parse(params["end_date"]).date() if params["end_date"] else pendulum.today("UTC").date()
        start = pendulum.parse(params["start_date"]).date() if params["start_date"] else end.subtract(days=6)
        if start > end:
            raise ValueError(f"start_date {start} is after end_date {end}")
        if params["min_magnitude"] > params["max_magnitude"]:
            raise ValueError("min_magnitude is larger than max_magnitude")

        region = params["region"]
        filters = {
            "region": region,
            "region_label": region.replace("_", " / ").title() if region != "world" else "World",
            "bbox": list(REGIONS[region]) if region != "world" else None,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "min_mag": params["min_magnitude"],
            "max_mag": params["max_magnitude"],
            "name": f"{region}_{start}_{end}_{re.sub(r'[^A-Za-z0-9_-]', '-', run_id)}",
        }
        log.info("Filters: %s", filters)
        return filters

    @task(retries=3, retry_delay=pendulum.duration(minutes=1))
    def plan_windows(filters: dict) -> list[dict]:
        """Split [start, end] into time windows that each match <= MAX_PER_QUERY events."""
        base = _base_query(filters)

        def count(start: pendulum.DateTime, end: pendulum.DateTime) -> int:
            return _get(COUNT_URL, {**base, "starttime": start.isoformat(), "endtime": end.isoformat()}).json()["count"]

        def split(start, end, n):
            if n <= MAX_PER_QUERY or (end - start).in_seconds() < 60:
                return [(start, end, n)]
            mid = start.add(seconds=(end - start).in_seconds() // 2)
            return split(start, mid, count(start, mid)) + split(mid, end, count(mid, end))

        start = pendulum.from_format(filters["start"], "YYYY-MM-DD", tz="UTC")
        end = pendulum.from_format(filters["end"], "YYYY-MM-DD", tz="UTC").add(days=1)  # end_date is inclusive
        total = count(start, end)
        windows = [
            {"i": i, "start": s.isoformat(), "end": e.isoformat(), "expected": n}
            for i, (s, e, n) in enumerate(split(start, end, total))
        ]
        log.info("%d events in total -> %d window(s)", total, len(windows))
        return windows

    @task(retries=3, retry_delay=pendulum.duration(minutes=1), max_active_tis_per_dag=4)
    def fetch_window(filters: dict, window: dict) -> str:
        resp = _get(QUERY_URL, {**_base_query(filters), "starttime": window["start"], "endtime": window["end"]})
        rows = []
        for f in resp.json()["features"]:
            p = f["properties"]
            if p["mag"] is None:
                continue
            lon, lat, depth = f["geometry"]["coordinates"]
            rows.append([p["time"], round(p["mag"], 1), round(lat, 3), round(lon, 3),
                         round(depth or 0, 1), int(bool(p["tsunami"])), f["id"], p["place"] or "unknown"])

        out = CHUNK_DIR / filters["name"] / f"{window['i']:04d}.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(rows, separators=(",", ":")), encoding="utf-8")
        log.info("Window %s -> %s: %d events (count said %d)", window["start"], window["end"], len(rows), window["expected"])
        return str(out)

    @task
    def merge(filters: dict, chunk_paths: list[str]) -> str:
        rows, seen = [], set()
        for path in sorted(chunk_paths):
            for row in json.loads(Path(path).read_text(encoding="utf-8")):
                # window edges are inclusive on both sides, so an event can show up twice
                if row[6] not in seen:
                    seen.add(row[6])
                    rows.append(row)
        rows.sort(key=lambda r: r[0])

        out = OUTPUT_DIR / f"{filters['name']}.csv"
        with out.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(FIELDS)
            writer.writerows(rows)
        shutil.copyfile(out, OUTPUT_DIR / "latest.csv")
        shutil.rmtree(CHUNK_DIR / filters["name"], ignore_errors=True)

        log.info("Merged %d unique events -> logs/earthquake_maps/%s", len(rows), out.name)
        return str(out)

    @task
    def report(filters: dict, csv_path: str) -> str:
        with open(csv_path, newline="", encoding="utf-8") as fh:
            reader = csv.reader(fh)
            next(reader)
            rows = [[int(t), float(m), float(la), float(lo), float(d), int(ts), i, pl]
                    for t, m, la, lo, d, ts, i, pl in reader]

        data = {
            "filters": filters,
            "fields": FIELDS,
            "rows": rows,
            "generated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        }
        # "</" would close the <script> tag early if a place name ever contained it
        payload = json.dumps(data, separators=(",", ":")).replace("</", "<\\/")
        html = TEMPLATE.read_text(encoding="utf-8").replace("__DATA__", payload)

        out = OUTPUT_DIR / f"{filters['name']}.html"
        out.write_text(html, encoding="utf-8")
        shutil.copyfile(out, OUTPUT_DIR / "latest.html")
        log.info("Wrote report: logs/earthquake_maps/%s (%d events, %.1f MB)", out.name, len(rows), len(html) / 1e6)
        return str(out)

    filters = resolve_filters()
    chunks = fetch_window.partial(filters=filters).expand(window=plan_windows(filters))
    report(filters, merge(filters, chunks))


usgs_earthquake_map()
