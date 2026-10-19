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

Python 3.12, Java 17, AWS CLI (public archive, no account needed).

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### How to reproduce

The OpenAQ download needs a free API key: create an account on [OpenAQ](https://explore.openaq.org), copy your key, then export it (the variable name is also in `.env.example`, never commit the key):

```bash
export OPENAQ_API_KEY=<your key>
```

Run from the root of the repository:

```bash
python scripts/download_openaq.py
python scripts/decompress_openaq.py
python scripts/download_weather.py
python src/ingestion.py
```

Then run the notebooks in this order with the `.venv` kernel: `src/cleaning.ipynb`, `src/join.ipynb`, `src/analysis.ipynb`.
### Pipeline

| Step | File | What it does |
|---|---|---|
| Download | `scripts/` | Downloads OpenAQ and weather data |
| Ingestion | `src/ingestion.py` | Raw CSV to typed Parquet |
| Cleaning | `src/cleaning.ipynb` | Removes invalid stations and values |
| Join | `src/join.ipynb` | Joins pollution, weather and stations |
| Analysis | `src/analysis.ipynb` | Spark SQL analysis and figures |

