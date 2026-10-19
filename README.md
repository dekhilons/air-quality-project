# Air-quality-project

Project for the Data Processing and Analytics course (DISS-DPA), in three parts: Part 1 Volume (Spark SQL), Part 2 Velocity (Structured Streaming), Part 3 Variety (GraphFrames).

**Research question:** How do weather conditions and geographical location relate to urban air pollution, and how can we identify monitoring stations that are important for detecting pollution episodes?

## Part 1 - Volume (Spark SQL)

**Sub-question:** How do PM2.5, PM10, NO2 and O3 concentrations vary with location (Paris, inner suburbs, outer suburbs, outside Ile-de-France) and over time, and how are they related to temperature, wind and precipitation?

### Data

| Source | Content | Licence |
|---|---|---|
| [OpenAQ archive](https://registry.opendata.aws/openaq/) | Hourly air-quality measurements, 2018-2026 (about 1.13 GB) | ODC-BY |
| [Open-Meteo Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api) | Hourly weather at each station (about 0.96 GB) | CC BY 4.0 |
| [API Géo](https://geo.api.gouv.fr) | Department of each station | Licence Ouverte |

The data is not in the repository. The scripts below download it.

### Requirements

- Python 3.12 ,Java 17 and the AWS CLI (public archive, no account needed) -- for the download scripts, on any OS.
- For the Spark steps (`src/ingestion.py` and the three notebooks): **Java 17 on Mac/Linux**, or **Docker Desktop on Windows**. Native PySpark on Windows needs a native helper (`winutils.exe`) that most setups don't have installed . Docker sidesteps the problem entirely by running Spark inside Linux. `docker-compose.yml` at the repo root defines the `spark` and `jupyter` services used below.

### How to reproduce

The OpenAQ download needs a free API key: create an account on [OpenAQ](https://explore.openaq.org), copy your key, then export it (the variable name is also in `.env.example`, never commit the key):

```bash
export OPENAQ_API_KEY=<your key>
```

**Step 1 -- download the data.** No Spark involved, works the same on any OS:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

python scripts/download_openaq.py
python scripts/decompress_openaq.py
python scripts/download_weather.py
python scripts/get_departments.py
```

**Step 2 -- run the Spark pipeline** (`src/ingestion.py`, then `src/cleaning.ipynb`, `src/join.ipynb`, `src/analysis.ipynb` in order).

#### If you're on Mac/Linux

```bash
python src/ingestion.py
```

Then run, in order, with the `.venv` kernel: `src/cleaning.ipynb`, `src/join.ipynb`, `src/analysis.ipynb`.

#### If you're on Windows

Install [Docker Desktop]

`docker-compose.yml` defines two services, both pulled automatically the first time you use them :
- `spark` -- runs `src/ingestion.py` via `spark-submit`, one command, then exits.
- `jupyter` -- a notebook server for the three `.ipynb` steps, stays running until you stop it.

```powershell
docker compose run spark /opt/spark/bin/spark-submit --driver-memory 6g src/ingestion.py
docker compose up jupyter
```

Then open http://localhost:8888/lab and run, in order: `src/cleaning.ipynb`, `src/join.ipynb`, `src/analysis.ipynb`.

One thing to know: `ingestion.py` sets its Spark driver memory *inside* the script (`spark.driver.memory = 4g`), which only takes effect when run as plain `python src/ingestion.py`. 

### Pipeline

| Step | File | What it does | Runs on |
|---|---|---|---|
| Download | `scripts/` | Downloads OpenAQ, weather, and department data | Any OS, no Spark |
| Ingestion | `src/ingestion.py` | Raw CSV to typed Parquet | Mac/Linux: native &middot; Windows: Docker |
| Cleaning | `src/cleaning.ipynb` | Removes invalid stations and values | Mac/Linux: native Jupyter &middot; Windows: Docker Jupyter |
| Join | `src/join.ipynb` | Joins pollution, weather and stations | Mac/Linux: native Jupyter &middot; Windows: Docker Jupyter |
| Analysis | `src/analysis.ipynb` | Spark SQL analysis and figures | Mac/Linux: native Jupyter &middot; Windows: Docker Jupyter |

