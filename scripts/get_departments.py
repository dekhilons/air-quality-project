import csv
import time

import requests

API_URL = "https://geo.api.gouv.fr/communes"
STATIONS_CSV = "config/openaq_station_candidates.csv"
OUTPUT_CSV = "config/station_departments.csv"

rows = []
with open(STATIONS_CSV, encoding="utf-8") as f:
    for station in csv.DictReader(f):
        params = {
            "lat": station["latitude"],
            "lon": station["longitude"],
            "fields": "nom,code,codeDepartement"
        }
        response = requests.get(API_URL, params=params, timeout=30)
        response.raise_for_status()
        communes = response.json()
        if communes:
            commune = communes[0]
            rows.append([station["location_id"], commune["nom"], commune["code"], commune["codeDepartement"]])
        else:
            rows.append([station["location_id"], "", "", ""])
        print(rows[-1])
        time.sleep(0.2)

with open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["location_id", "commune", "insee_code", "department"])
    writer.writerows(rows)

print("Stations:", len(rows))
print("Stations without commune:", sum(1 for row in rows if row[1] == ""))
