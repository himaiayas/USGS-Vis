"""Shared USGS constants for the earthquake DAGs (not a DAG itself)."""

QUERY_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"

# name -> (minlatitude, maxlatitude, minlongitude, maxlongitude)
REGIONS = {
    "japan": (24, 46, 122, 146),
    "indonesia": (-11, 6, 95, 141),
    "california": (32, 42, -125, -114),
    "alaska": (51, 72, -180, -129),
    "chile": (-56, -17, -76, -66),
    "mediterranean": (30, 46, -6, 36),
    "thailand_myanmar": (5, 28, 92, 106),
}
