# 🌐 Geospatial Data Engineering Pipeline for Public Infrastructure

This project demonstrates a **production-grade GIS data pipeline** inspired by real-world utility operations (e.g., water, energy, urban infrastructure). It extracts, transforms, and loads open-source spatial data using cloud-compatible formats and modern orchestration with Apache Airflow.

The pipeline is designed for **modular deployment** via Docker Compose and includes logging, validation, scheduling, and optional cloud exports.  
✅ *Planned API services and spatial analysis tasks will simulate full-stack GIS engineering workflows used in enterprise settings.*

---

## 📌 Table of Contents

- [Project Goals](#project-goals)
- [Tech Stack](#tech-stack)
- [Pipeline Architecture](#pipeline-architecture)
- [USGS API Endpoints](#usgs-api-endpoints)
- [Testing](#testing)
- [Mocked / Proprietary Replacements](#mocked--proprietary-replacements)
- [Future Enhancements](#future-enhancements)
- [Example Data Sources](#example-data-sources)
- [Getting Started](#getting-started)
- [Running the Pipeline](#running-the-pipeline)
- [Notes](#notes)

---

## Project Goals

- Demonstrate GIS-related ETL skills aligned with industry roles
- Handle open geospatial data in modern formats (GeoJSON, Shapefile, Parquet)
- Replace proprietary ArcGIS tools with open-source equivalents
- Serve as a learning sandbox for spatial processing, cloud integration, and orchestration
- Simulate enterprise-level REST APIs for geospatial data querying
- Demonstrate basic spatial modeling (joins, buffers, proximity analysis)
- Practice object-oriented design in Python data pipeline components

---

## Tech Stack

- Language: Python, SQL
- Containerization/Orchestration: Docker/Airflow
- Data Store: PostgreSQL
- Cloud Provider: AWS 
- API endpoints: api.waterdata.usgs.gov
  - Latest Continuous Values, Daily Values, Monitoring Locations, Time Series Metadata, USGS Water Data Statistics

---

## Pipeline Architecture

```plaintext
┌──────────────────┐
│ Public Geo APIs  │  ← (USGS)
└────────┬─────────┘
         ▼
┌──────────────────┐
│  Extract (Python)│
└────────┬─────────┘
         ▼
┌────────────────────────────────────────┐
│ Transform (GeoPandas, Shapely, Pyproj) │
│ - CRS normalization                    │
│ - Geometry cleanup                     │
│ - Filtering (e.g., pipelines, roads)   │
│ - ✅ Spatial operations (joins/buffers)│
└────────┬───────────────────────────────┘
         ▼
┌────────────────────────────┐
│ Load to PostGIS (PostgreSQL) │
└────────┬───────────────────┘
         ▼
┌────────────────────────────┐
│ REST API (FastAPI - WIP) ✅ │
│ Serve spatial queries      │
└────────┬───────────────────┘
         ▼
┌────────────────────────────┐
│ Export to S3 as Parquet    │ (Optional)
└────────────────────────────┘

```

--- 

## USGS API Endpoints

This project integrates five key endpoints from the USGS Water Data API.
They share common identifiers (e.g., id, monitoring_location_id, agency_code) that enable table joins for advanced analysis.

1. Monitoring Locations

Endpoint: /collections/monitoring-locations/items

Purpose: Master list of water monitoring stations with metadata (coordinates, agency, site type, HUC).

Key Fields:

id (Primary Key)

agency_code

Use Cases: Base reference table for joins with measurement datasets.

2. Daily

Endpoint: /collections/daily/items

Purpose: Historical daily aggregated values (streamflow, gage height, temperature).

Key Fields:

monitoring_location_id (FK → monitoring-locations.id)

observed_property_id

Use Cases: Seasonal trend analysis, long-term water resource planning.

3. Latest Continuous

Endpoint: /collections/latest-continuous/items

Purpose: Real-time continuous sensor data (15–60 min intervals).

Key Fields:

monitoring_location_id (FK → monitoring-locations.id)

observed_property_id

Use Cases: Flood alerts, operational dashboards, drought monitoring.

4. Site Observations

Endpoint: /collections/site-observations/items

Purpose: Inventory of available parameters and time ranges per site.

Key Fields:

monitoring_location_id (FK → monitoring-locations.id)

observed_property_id

Use Cases: Filter sites by available measurement types before pulling data.

5. Statistics

Endpoint: /collections/statistics/items

Purpose: Statistical summaries (percentiles, medians, counts) for observed variables.

Key Fields:

monitoring_location_id (FK → monitoring-locations.id)

observed_property_id

Use Cases: Identify extreme values, variability patterns, baseline comparisons.

Example Spatial Analysis Tasks

Buffer Zones: Identify infrastructure within risk zones

Spatial Joins: Merge site metadata with time series measurements

Filtering: Select only stations measuring specific parameters

PostGIS Queries: Aggregate water levels by watershed and season

---

## Testing

Tests are written with pytest and include both unit, integration and e2e coverage:

pytest tests/

    Validate schema mappings and geometry correctness

    Test API fallback behavior

    Integration test from extract → load using dummy data

    (Planned) API response tests and spatial query validation

---

## Mocked / Proprietary Replacements

| Proprietary Tool      | Open-Source Equivalent               |
| --------------------- | ------------------------------------ |
| ArcGIS / ArcMap       | GeoPandas + PostGIS                  |
| FME Workbench         | Custom Python ETL scripts            |
| ESRI Feature Services | REST APIs (FastAPI)                  |
| ArcGIS Online         | Static hosting / S3 + Leaflet        |
| ArcPy                 | Shapely, Pyproj (Geo-alternatives)   |

---

## Future Enhancements

    Add support for raster layers (e.g., elevation, satellite imagery)

    Integrate Leaflet or Folium-based web viewer for spatial outputs

    Add schema versioning and change tracking

    CI/CD integration for deployment (GitHub Actions or Jenkins)

    Add Dockerized FastAPI + NGINX service for live API access

    Upload to AWS RDS (PostGIS) to simulate real cloud architecture

---

## Example Data Sources

    USGS National Map

    OpenStreetMap Overpass API

    Natural Earth

    NYC OpenData

---

## Getting Started

This project uses Poetry to manage dependencies and virtual environments. Poetry ensures that everyone working on this project installs exactly the same package versions, avoiding “works on my machine” issues and simplifying setup.

More about poetry: https://python-poetry.org/docs/

To get started:

```bash
git clone https://github.com/coromanillos/etl_project_02.git
cd etl_project_02
poetry --version || curl -sSL https://install.python-poetry.org | python3 -
poetry install
poetry shell
```

---

## Notes

## USGS APIs (Water Infrastructure & Monitoring)

The United States Geological Survey (USGS) provides water-related data through various APIs:

- **Monitoring Locations API**  
  Identifies active water monitoring stations. Useful for mapping infrastructure like pump stations and treatment plants in specific watersheds.

- **Latest Continuous Values API**  
  Provides live water condition data (e.g., streamflow, gage height). Ideal for real-time alerts on flooding, droughts, or infrastructure stress.

- **Daily Values API**  
  Delivers historical daily measurements. Supports trend analysis, regulatory reporting, and modeling seasonal or long-term changes in water behavior.

- **Time Series Metadata API**  
  Lists available data types and quality before extraction. Helps determine what data streams are available and valid for use.

- **Water Quality Portal / USGS Samples API**  
  Supplies detailed water quality sample data. Supports environmental compliance, public health evaluations, and source water analysis.

- **Real-Time Flood Impacts (RTFI) API**  
  Offers GIS-ready flood impact overlays. Useful for mapping risk zones and planning resilient infrastructure.


- OpenStreetMap

- Natural Earth

---

# More notes:
    - Cloud first approach for all DAG ETL scripts (obviously) 
    - Data should flow through memory objects (dicts, lists, ORM objects etc.)
    - Writing should happen at the end, directly into the database
    - This approach prevents I/O, making pipeline clean, fast and cloud native.
---

# Testing:

  >> APP_ENV=test poetry run pytest
  
  pyproject.toml # Poetry dependencies manager (runtime + dev)
  poetry.lock # Locked versions for reproducibility
---

# Notes:

08/12/25

- **Notes**  
  Before extracting data from a dataset, you should understand what the
  data access pattern is. Is it exhaustible data? Is it a recent-window 
  API that shows data from right now? What parameters are offered that 
  allow you to specifiy what data you pull? What data do you WANT to pull?
  What does the raw data look like, and what do you expect it to look like
  after being processed? 

  Go back and identify what you want to get from the USGS monitoring-locations endpoint. Adjust the API_URL, then refactor the extract, transform and schema scripts as needed. Only after that, can you move on to unit tests, integration tests, and Github Action CI/CD implementation.

  - review API and extracted data
  - extract.py
  - http_client and .env | API_URL= in .env
  - transform.py 
  - model/ ORM schema
  - unit tests
  - integration tests
  - Github Action CI/CD
---

docker exec -it airflow_postgis psql -U airflow -d airflow
