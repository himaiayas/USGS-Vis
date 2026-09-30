"""Daily per-region earthquake report using the USGS FDSN event query API.

Each run queries its own data interval, so it can be backfilled for past days.
Regions are fanned out with dynamic task mapping, then compared in one report.
API docs: https://earthquake.usgs.gov/fdsnws/event/1/
"""

import logging

import pendulum
import requests
from airflow.sdk import dag, task
from usgs_common import QUERY_URL, REGIONS

log = logging.getLogger(__name__)


@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    max_active_runs=2,
    tags=["usgs", "earthquake"],
)
def usgs_earthquake_regions():

    @task
    def list_regions() -> list[dict]:
        return [{"name": name, "bbox": bbox} for name, bbox in REGIONS.items()]

    @task(retries=3, retry_delay=pendulum.duration(minutes=1), max_active_tis_per_dag=4)
    def query_region(region: dict, data_interval_start=None, data_interval_end=None) -> dict:
        # Manual runs have an empty (start == end) or missing interval; use the 24h before it.
        end = data_interval_end or pendulum.now("UTC")
        start = data_interval_start
        if start is None or start >= end:
            start = end.subtract(days=1)
        min_lat, max_lat, min_lon, max_lon = region["bbox"]

        resp = requests.get(
            QUERY_URL,
            params={
                "format": "geojson",
                "starttime": start.isoformat(),
                "endtime": end.isoformat(),
                "minlatitude": min_lat,
                "maxlatitude": max_lat,
                "minlongitude": min_lon,
                "maxlongitude": max_lon,
                "minmagnitude": 1.0,
                "orderby": "magnitude",
            },
            timeout=60,
        )
        resp.raise_for_status()
        props = [f["properties"] for f in resp.json()["features"] if f["properties"]["mag"] is not None]
        mags = [round(p["mag"], 1) for p in props]

        result = {
            "region": region["name"],
            "start": start.isoformat(),
            "end": end.isoformat(),
            "count": len(props),
            "max_mag": max(mags, default=None),
            "avg_mag": round(sum(mags) / len(mags), 2) if mags else None,
            "m4_plus": sum(m >= 4 for m in mags),
            "strongest_place": props[0]["place"] if props else None,
            # Gutenberg-Richter style bucket counts: M1, M2, M3, ...
            "histogram": {f"M{b}": sum(int(m) == b for m in mags) for b in range(1, 10) if any(int(m) == b for m in mags)},
        }
        log.info("%s: %s", region["name"], result)
        return result

    @task
    def report(results: list[dict]):
        results = sorted(results, key=lambda r: r["count"], reverse=True)
        if results:
            log.info("Window %s -> %s", results[0]["start"], results[0]["end"])

        header = f"{'region':<18}{'count':>7}{'M4+':>6}{'max':>7}{'avg':>7}  strongest"
        lines = [header, "-" * len(header)]
        for r in results:
            lines.append(
                f"{r['region']:<18}{r['count']:>7}{r['m4_plus']:>6}"
                f"{r['max_mag'] if r['max_mag'] is not None else '-':>7}"
                f"{r['avg_mag'] if r['avg_mag'] is not None else '-':>7}"
                f"  {r['strongest_place'] or '-'}"
            )
        log.info("Regional earthquake report\n%s", "\n".join(lines))

        most_active = results[0] if results and results[0]["count"] else None
        if most_active:
            log.info("Most active region: %s (%d events)", most_active["region"], most_active["count"])

    report(query_region.expand(region=list_regions()))


usgs_earthquake_regions()
