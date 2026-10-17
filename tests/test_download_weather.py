import csv
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from download_weather import (annual_windows, already_complete, extract_rows,
                              load_stations, output_paths, request_params, write_output)


class WeatherDownloadTests(unittest.TestCase):
    def setUp(self):
        self.station = {"location_id": 2707, "station": "Evreux Centre", "latitude": 49.02134, "longitude": 1.148319}

    def test_leap_year_windows(self):
        periods = list(annual_windows(date(2024, 12, 30), date(2025, 1, 2)))
        self.assertEqual(periods, [(date(2024, 12, 30), date(2024, 12, 31)), (date(2025, 1, 1), date(2025, 1, 2))])

    def test_hourly_rows(self):
        payload = {"utc_offset_seconds": 0, "hourly": {
            "time": [f"2024-01-01T{h:02}:00" for h in range(24)],
            "temperature_2m": [10.0] * 24,
            "relative_humidity_2m": [75] * 24,
            "precipitation": [0.0] * 24,
            "wind_speed_10m": [12.5] * 24,
        }}
        rows, missing = extract_rows(payload, self.station, date(2024, 1, 1), date(2024, 1, 1))
        self.assertEqual(len(rows), 24)
        self.assertEqual(rows[0]["datetime_utc"], "2024-01-01T00:00:00Z")
        self.assertEqual(rows[-1]["datetime_utc"], "2024-01-01T23:00:00Z")
        self.assertEqual(missing["temperature_2m"], 0)

    def test_missing_measurement_is_empty(self):
        payload = {"utc_offset_seconds": 0, "hourly": {
            "time": [f"2024-01-01T{h:02}:00" for h in range(24)],
            "temperature_2m": [None] + [10.0] * 23,
            "relative_humidity_2m": [75] * 24,
            "precipitation": [0.0] * 24,
            "wind_speed_10m": [12.5] * 24,
        }}
        rows, missing = extract_rows(payload, self.station, date(2024, 1, 1), date(2024, 1, 1))
        self.assertEqual(rows[0]["temperature_2m"], "")
        self.assertEqual(missing["temperature_2m"], 1)

    def test_resume_with_valid_metadata(self):
        with tempfile.TemporaryDirectory() as tmp:
            csv_path, meta_path = output_paths(Path(tmp), 2707, date(2024, 1, 1), date(2024, 1, 1))
            params = request_params(self.station, date(2024, 1, 1), date(2024, 1, 1))
            rows = [{"location_id": 2707, "station": "Evreux Centre", "station_latitude": 49.02134, "station_longitude": 1.148319,
                     "datetime_utc": f"2024-01-01T{hour:02}:00:00Z", "temperature_2m": 10.0,
                     "relative_humidity_2m": 75, "precipitation": 0.0, "wind_speed_10m": 12.5} for hour in range(24)]
            write_output(csv_path, meta_path, rows, {"row_count": 24, "request_params": params})
            self.assertTrue(already_complete(csv_path, meta_path, params, 24))
            self.assertFalse(already_complete(csv_path, meta_path, {**params, "models": "best_match"}, 24))

    def test_station_csv(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "stations.csv"
            with path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=["location_id", "station", "latitude", "longitude"])
                writer.writeheader()
                writer.writerow({"location_id": 2707, "station": "Evreux Centre", "latitude": 49.02134, "longitude": 1.148319})
            self.assertEqual(len(load_stations(path)), 1)


if __name__ == "__main__":
    unittest.main()
