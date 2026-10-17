import os
import requests
import pandas as pd
import subprocess
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

# --- CONFIGURATION ---
REPO_ROOT = Path(__file__).resolve().parents[1]
CANDIDATES_CSV = REPO_ROOT / "config" / "openaq_station_candidates.csv"
RAW_OUT_DIR = REPO_ROOT / "data" / "raw" / "openaq"

# --- SECTION 1: Exploration and Fetching Candidate Stations ---
def fetch_and_filter_stations():
    if CANDIDATES_CSV.exists():
        print(f"Found existing '{CANDIDATES_CSV}' locally! Loading stations from file to bypass API calls...")
        return pd.read_csv(CANDIDATES_CSV)

    print("Fetching stations in France from OpenAQ API...")
    url = "https://api.openaq.org/v3/locations"

    api_key = os.getenv("OPENAQ_API_KEY", "")

    headers = {}
    if api_key:
        headers["X-API-Key"] = api_key
    else:
        print("\n[WARNING] No OPENAQ_API_KEY environment variable set.")
        print("The request might fail with a 401 Unauthorized if the OpenAQ API requires authentication.\n")
        
    params = {
        "iso": "FR",
        "bbox": "-0.5,48.0,4.0,49.5",
        "limit": 1000,
        "page": 1
    }
    
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    locations = response.json()["results"]
    
    rows = []
    for loc in locations:
        sensors = loc.get("sensors") or []
        pollutants = sorted(set(
            s.get("parameter", {}).get("name")
            for s in sensors
            if s.get("parameter", {}).get("name")
        ))
        
        rows.append({
            "location_id": loc["id"],
            "station": loc.get("name"),
            "locality": loc.get("locality"),
            "latitude": (loc.get("coordinates") or {}).get("latitude"),
            "longitude": (loc.get("coordinates") or {}).get("longitude"),
            "pollutants": ", ".join(pollutants),
            "sensor_count": len(sensors)
        })
        
    df_stations = pd.DataFrame(rows)
    
    # Filtering candidates matching requirements
    target_pollutants = {"pm25", "pm10", "no2", "o3"}
    df_stations["pollutant_set"] = df_stations["pollutants"].apply(
        lambda x: set(x.split(", ")) if x else set()
    )
    df_stations["target_count"] = df_stations["pollutant_set"].apply(
        lambda x: len(x & target_pollutants)
    )
    
    df_candidates = df_stations[
        df_stations["latitude"].notna()
        & df_stations["longitude"].notna()
        & (df_stations["target_count"] > 0)
    ].sort_values(["target_count", "sensor_count"], ascending=False)
    
    CANDIDATES_CSV.parent.mkdir(parents=True, exist_ok=True)
    df_candidates.to_csv(CANDIDATES_CSV, index=False)
    print(f"Saved {len(df_candidates)} matching candidate stations to {CANDIDATES_CSV}")
    return df_candidates

# --- SECTION 2: S3 Parallel Downloader ---
def download_station_year(loc_id, year):
    target_dir = str(RAW_OUT_DIR / f"locationid={loc_id}" / f"year={year}") + os.sep
    cmd = [
        "aws", "s3", "cp",
        "--no-sign-request",
        "--recursive",
        f"s3://openaq-data-archive/records/csv.gz/locationid={loc_id}/year={year}/",
        target_dir
    ]
    try:
        result = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
        if result.returncode == 0:
            if os.path.exists(target_dir) and os.listdir(target_dir):
                print(f"[SUCCESS] Downloaded: Station {loc_id} | Year {year}")
    except Exception as e:
        print(f"[ERROR] Failed command for Station {loc_id}, Year {year}: {e}")

def main():
    df_candidates = fetch_and_filter_stations()
    station_ids = df_candidates['location_id'].unique().tolist()
    years = list(range(2018, 2027)) 
    
    tasks = [(loc_id, year) for loc_id in station_ids for year in years]
    print(f"Starting download loop for {len(tasks)} tasks (parallel execution)... ")
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(lambda t: download_station_year(*t), tasks)
    
    print("All processes completed locally!")

if __name__ == "__main__":
    main()