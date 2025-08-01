# 🌐 Geospatial Data Engineering Pipeline for Public Infrastructure

This project demonstrates a **production-grade GIS data pipeline** inspired by real-world utility operations (e.g., water, energy, urban infrastructure). It extracts, transforms, and loads open-source spatial data using cloud-compatible formats and modern orchestration with Apache Airflow.

The pipeline is designed for **modular deployment** via Docker Compose and includes logging, validation, scheduling, and optional cloud exports.

---

## 📌 Table of Contents

- [Project Goals](#project-goals)
- [Tech Stack](#tech-stack)
- [Pipeline Architecture](#pipeline-architecture)
- [Key Features](#key-features)
- [Getting Started](#getting-started)
- [Running the Pipeline](#running-the-pipeline)
- [Testing](#testing)
- [Mocked/Proprietary Replacements](#mockedproprietary-replacements)
- [Future Enhancements](#future-enhancements)
- [Example Data Sources](#example-data-sources)


---

## Project Goals

- Demonstrate GIS-related ETL skills aligned with industry roles
- Handle open geospatial data in modern formats (GeoJSON, Shapefile, Parquet)
- Replace proprietary ArcGIS tools with open-source equivalents
- Serve as a learning sandbox for spatial processing, cloud integration, and orchestration

---

## Tech Stack

| Layer               | Technology                            | Purpose                                  |
|---------------------|----------------------------------------|------------------------------------------|
| Language            | Python 3.10+                           | Core scripting                           |
| Orchestration       | Apache Airflow                         | DAGs and scheduling                      |
| Containerization    | Docker, Docker Compose                 | Local dev & deployment                   |
| Spatial ETL         | GeoPandas, Shapely, Fiona, Pyproj      | Geospatial parsing & processing          |
| Data Store          | PostgreSQL + PostGIS                   | Spatially-aware relational DB            |
| Cloud Storage       | AWS S3 (optional)                      | Data lake export                         |
| Public APIs         | USGS, OpenStreetMap, Natural Earth     | Geo data sources                         |
| Alerting            | Slack Webhooks (Free Tier)             | Pipeline status notifications            |
| Testing             | pytest                                 | Unit and integration testing             |
| Monitoring          | Airflow Logs + Slack Alerts            | Observability                            |

---

## Pipeline Architecture

```plaintext
┌──────────────────┐
│ Public Geo APIs  │  ← (e.g., USGS, OSM, Natural Earth)
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
└────────┬───────────────────────────────┘
         ▼
┌────────────────────────────┐
│ Load to PostGIS (PostgreSQL) │
└────────┬───────────────────┘
         ▼
┌────────────────────────────┐
│ Export to S3 as Parquet    │ (Optional)
└────────────────────────────┘
```

--- 

## Key Features

    Modular DAGs: Each stage of the ETL process is encapsulated in separate Airflow tasks for flexibility.

    Geospatial Data Validation: Ensures valid geometries, CRS consistency, and attribute quality before loading.

    CRS Handling: Normalizes all geometries to EPSG:4326 for interoperability.

    Local & Cloud Storage: Supports both local storage for dev and optional S3 exports for production.

    Logging & Alerts: Integrated with Slack for failure alerts and Airflow for detailed DAG-level logs.

    Mocked Endpoints: Included for testing pipeline stages without hitting real APIs.

---

## Testing

Tests are written with pytest and include both unit and integration coverage:

pytest tests/

    Validate schema mappings and geometry correctness

    Test API fallback behavior

    Integration test from extract → load using small dummy datasets

---

## Mocked / Proprietary Replacements
Proprietary Tool	Open-Source Equivalent
ArcGIS / ArcMap	GeoPandas + PostGIS
FME Workbench	Custom Python ETL Scripts
ESRI Feature Services	REST APIs (e.g., USGS, OSM)
ArcGIS Online	S3 or local static hosting

---

## Future Enhancements

    Add support for raster layers (e.g., elevation, satellite imagery)

    Integrate Mapbox or Leaflet-based front-end visualization

    Add schema versioning and change tracking

    Implement advanced geometry operations (e.g., buffering, spatial joins)

    CI/CD integration for pipeline testing and deployment

---

## Example Data Sources

    USGS National Map

    OpenStreetMap Overpass API

    Natural Earth

    NYC OpenData

---

## Getting Started

git clone https://github.com/your-username/gis-data-pipeline.git
cd gis-data-pipeline
cp .env.example .env
docker-compose up --build

To initialize Airflow:

docker exec -it airflow-webserver airflow db init
docker exec -it airflow-webserver airflow users create \
  --username admin --password admin --firstname Data --lastname Engineer --role Admin --email admin@example.com

Access the Airflow UI at http://localhost:8080.
🏃 Running the Pipeline

Once Airflow is running, trigger the DAG manually or wait for the scheduled run:

    DAG ID: geospatial_pipeline

    Schedule: @daily (customizable via dags/config.py)

    Logs and task statuses are available in the Airflow UI
