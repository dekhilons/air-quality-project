#!/usr/bin/env python3
"""Download historical hourly weather for a selected set of OpenAQ stations."""

import argparse
import csv
import json
import logging
import math
import os
import random
import re
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

API_URL = "https://archive-api.open-meteo.com/v1/archive"
WEATHER_VARIABLES = (
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
)
OUTPUT_FIELDS = (
    "location_id",
    "station",
    "station_latitude",
    "station_longitude",
    "datetime_utc",
    *WEATHER_VARIABLES,
)
LOGGER = logging.getLogger("weather")


def parse_date(value):
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Date must be YYYY-MM-DD") from exc


def load_stations(csv_path):
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        expected = {"location_id", "station", "latitude", "longitude"}
        missing_columns = expected - set(reader.fieldnames or [])
        if missing_columns:
            raise ValueError(f"Missing station columns: {sorted(missing_columns)}")

        stations = {}
        for row_number, row in enumerate(reader, start=2):
            try:
                station_id = int(row["location_id"])
                latitude = float(row["latitude"])
                longitude = float(row["longitude"])
            except (ValueError, TypeError) as exc:
                raise ValueError(f"Invalid station fields on row {row_number}") from exc
            if station_id <= 0 or not all(map(math.isfinite, (latitude, longitude))):
                raise ValueError(f"Invalid identifier or coordinates on row {row_number}")
            if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
                raise ValueError(f"Coordinates outside WGS84 bounds on row {row_number}")
            if station_id in stations:
                raise ValueError(f"Duplicate station location_id: {station_id}")
            stations[station_id] = {
                "location_id": station_id,
                "station": (row["station"] or "").strip(),
                "latitude": latitude,
                "longitude": longitude,
            }
        if not stations:
            raise ValueError("Station CSV contains no stations")
        return stations


def load_station_ids(file_path):
    """Read either a text file (one ID per line) or a CSV with location_id."""
    with file_path.open("r", encoding="utf-8-sig", newline="") as handle:
        content = handle.read()
    if file_path.suffix.lower() == ".csv":
        reader = csv.DictReader(content.splitlines())
        if "location_id" not in (reader.fieldnames or []):
            raise ValueError("The station selection CSV requires a location_id column")
        values = [row["location_id"] for row in reader]
    else:
        values = [line.strip() for line in content.splitlines() if line.strip() and not line.startswith("#")]
    try:
        selected = {int(value) for value in values}
    except ValueError as exc:
        raise ValueError("Station selection contains an invalid location_id") from exc
    if not selected:
        raise ValueError("Station selection is empty")
    return selected


def annual_windows(start, end):
    year = start.year
    while year <= end.year:
        left = max(start, date(year, 1, 1))
        right = min(end, date(year, 12, 31))
        if left <= right:
            yield left, right
        year += 1


def request_params(station, start, end):
    return {
        "latitude": f"{station['latitude']:.7f}",
        "longitude": f"{station['longitude']:.7f}",
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "hourly": ",".join(WEATHER_VARIABLES),
        "models": "era5",
        "timezone": "GMT",
        "timeformat": "iso8601",
        "temperature_unit": "celsius",
        "wind_speed_unit": "kmh",
        "precipitation_unit": "mm",
    }


def fetch_json(params, retries=4, timeout=120):
    url = f"{API_URL}?{urlencode(params)}"
    for attempt in range(retries + 1):
        try:
            request = Request(url, headers={"User-Agent": "DISS-DPA-Student-Project/1.0"})
            with urlopen(request, timeout=timeout) as response:
                result = json.load(response)
            if result.get("error"):
                raise ValueError(f"Open-Meteo API error: {result.get('reason')}")
            return result
        except HTTPError as exc:
            if exc.code not in {429, 500, 502, 503, 504} or attempt == retries:
                detail = exc.read(300).decode("utf-8", errors="replace")
                raise RuntimeError(f"HTTP {exc.code}: {detail}") from exc
            wait = min(60, 2 ** attempt * 3) + random.random()
            LOGGER.warning("HTTP %s; retrying in %.1f seconds", exc.code, wait)
        except (URLError, TimeoutError, OSError) as exc:
            if attempt == retries:
                raise RuntimeError(f"Open-Meteo request failed: {exc}") from exc
            wait = min(60, 2 ** attempt * 3) + random.random()
            LOGGER.warning("Connection problem; retrying in %.1f seconds", wait)
        time.sleep(wait)
    raise RuntimeError("Unreachable retry state")


def extract_rows(payload, station, start, end):
    if payload.get("utc_offset_seconds") != 0:
        raise ValueError("Unexpected Open-Meteo offset: expected GMT/UTC")
    hourly = payload.get("hourly")
    if not isinstance(hourly, dict) or not isinstance(hourly.get("time"), list):
        raise ValueError("Missing hourly time data in the API response")
    times = hourly["time"]
    expected_count = ((end - start).days + 1) * 24
    if len(times) != expected_count:
        raise ValueError(f"Expected {expected_count} hourly records, got {len(times)}")
    for variable in WEATHER_VARIABLES:
        if variable not in hourly or len(hourly[variable]) != expected_count:
            raise ValueError(f"Missing or incorrectly sized hourly array: {variable}")

    rows = []
    missing_counts = {variable: 0 for variable in WEATHER_VARIABLES}
    expected_start = datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc)
    for index, time_value in enumerate(times):
        timestamp = datetime.fromisoformat(time_value).replace(tzinfo=timezone.utc)
        if timestamp != expected_start + timedelta(hours=index):
            raise ValueError(f"Missing/duplicate/out-of-order weather timestamp: {time_value}")
        row = {
            "location_id": station["location_id"],
            "station": station["station"],
            "station_latitude": station["latitude"],
            "station_longitude": station["longitude"],
            "datetime_utc": timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        for variable in WEATHER_VARIABLES:
            value = hourly[variable][index]
            if value is None:
                missing_counts[variable] += 1
                row[variable] = ""
            elif not isinstance(value, (int, float)) or not math.isfinite(value):
                raise ValueError(f"Non-numeric weather value for {variable} at {time_value}")
            else:
                row[variable] = value
        rows.append(row)
    return rows, missing_counts


def output_paths(out_dir, station_id, start, end):
    folder = out_dir / f"location_id={station_id}" / f"year={start.year}"
    basename = f"weather_{start:%Y%m%d}_{end:%Y%m%d}"
    return folder / f"{basename}.csv", folder / f"{basename}.json"


def already_complete(csv_path, metadata_path, params, expected_rows):
    if not csv_path.is_file() or not metadata_path.is_file():
        return False
    try:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        if metadata.get("request_params") != params or metadata.get("row_count") != expected_rows:
            return False
        if metadata.get("bytes_written") != csv_path.stat().st_size:
            return False
        with csv_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            if tuple(reader.fieldnames or []) != OUTPUT_FIELDS:
                return False
            return sum(1 for _ in reader) == expected_rows
    except (ValueError, OSError, json.JSONDecodeError):
        return False


def write_output(csv_path, metadata_path, rows, metadata):
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    temp_csv = csv_path.with_suffix(".csv.tmp")
    temp_json = metadata_path.with_suffix(".json.tmp")
    try:
        with temp_csv.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=OUTPUT_FIELDS)
            writer.writeheader()
            writer.writerows(rows)
        metadata["bytes_written"] = temp_csv.stat().st_size
        temp_json.write_text(json.dumps(metadata, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        os.replace(temp_csv, csv_path)
        os.replace(temp_json, metadata_path)
    finally:
        temp_csv.unlink(missing_ok=True)
        temp_json.unlink(missing_ok=True)


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stations", type=Path, default=Path("config/openaq_station_candidates.csv"))
    parser.add_argument("--station-ids-file", type=Path, help="Restrict to OpenAQ IDs already acquired")
    parser.add_argument("--start-date", type=parse_date, default=date(2018, 1, 1))
    parser.add_argument("--end-date", type=parse_date, default=date(2026, 10, 10))
    parser.add_argument("--out-dir", type=Path, default=Path("data/raw/weather"))
    parser.add_argument("--limit", type=int, help="Only use the first N selected stations for testing")
    parser.add_argument("--delay", type=float, default=0.5, help="Minimum pause between API requests in seconds")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--dry-run", action="store_true", help="Show the planned downloads without making API requests")
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    if args.start_date > args.end_date:
        raise ValueError("--start-date must be on or before --end-date")
    if args.end_date >= date.today():
        raise ValueError("Use historical dates strictly before today")
    if args.limit is not None and args.limit < 1:
        raise ValueError("--limit must be a positive integer")
    if args.delay < 0:
        raise ValueError("--delay cannot be negative")
    stations_by_id = load_stations(args.stations)
    if args.station_ids_file:
        selected = load_station_ids(args.station_ids_file)
        absent = selected - stations_by_id.keys()
        if absent:
            missing_preview = ", ".join(str(value) for value in sorted(absent)[:25])
            raise ValueError(
                f"{len(absent)} selected OpenAQ station IDs have no coordinates in "
                f"{args.stations}: {missing_preview}. Add station metadata before downloading."
            )
        stations = [stations_by_id[station_id] for station_id in sorted(selected)]
    else:
        stations = list(stations_by_id.values())
    if args.limit:
        stations = stations[:args.limit]
    windows = list(annual_windows(args.start_date, args.end_date))
    tasks = [(station, start, end) for station in stations for start, end in windows]
    LOGGER.info("Selected %d stations, %d station-year periods", len(stations), len(tasks))
    if args.dry_run:
        for station, start, end in tasks[:12]:
            LOGGER.info("PLAN location_id=%s %s to %s", station["location_id"], start, end)
        if len(tasks) > 12:
            LOGGER.info("... %d additional periods", len(tasks) - 12)
        return 0

    failures = []
    complete = 0
    skipped = 0
    total_rows = 0
    for index, (station, start, end) in enumerate(tasks, start=1):
        params = request_params(station, start, end)
        csv_path, metadata_path = output_paths(args.out_dir, station["location_id"], start, end)
        expected = ((end - start).days + 1) * 24
        if not args.overwrite and already_complete(csv_path, metadata_path, params, expected):
            skipped += 1
            LOGGER.info("[%d/%d] SKIP location_id=%s year=%s", index, len(tasks), station["location_id"], start.year)
            continue
        try:
            payload = fetch_json(params)
            rows, missing = extract_rows(payload, station, start, end)
            write_output(
                csv_path,
                metadata_path,
                rows,
                {
                    "source": API_URL,
                    "model": "era5",
                    "license": "CC BY 4.0 (Open-Meteo; verify underlying data attribution)",
                    "location_id": station["location_id"],
                    "request_params": params,
                    "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
                    "row_count": len(rows),
                    "missing_values": missing,
                    "weather_grid_latitude": payload.get("latitude"),
                    "weather_grid_longitude": payload.get("longitude"),
                    "weather_elevation_m": payload.get("elevation"),
                },
            )
            complete += 1
            total_rows += len(rows)
            LOGGER.info(
                "[%d/%d] OK location_id=%s %s: %d hourly rows (%s)",
                index, len(tasks), station["location_id"], start.year, len(rows), csv_path,
            )
        except Exception as exc:
            failures.append((station["location_id"], start.year, str(exc)))
            LOGGER.error("[%d/%d] FAILED location_id=%s %s: %s", index, len(tasks), station["location_id"], start.year, exc)
        if index < len(tasks):
            time.sleep(args.delay)

    LOGGER.info("Complete: %d; skipped: %d; failed: %d; new hourly rows: %d", complete, skipped, len(failures), total_rows)
    return 1 if failures else 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        sys.exit(main())
    except (OSError, ValueError) as error:
        LOGGER.error("%s", error)
        sys.exit(2)
