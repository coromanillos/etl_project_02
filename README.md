# 🌐 Geospatial Data Engineering Pipeline for Public Infrastructure

This project demonstrates a **production-grade GIS data pipeline** inspired by real-world utility operations (e.g., water, energy, urban infrastructure). It extracts, transforms, and loads open-source spatial data using cloud-compatible formats and modern orchestration with Apache Airflow.

The pipeline is designed for **modular deployment** via Docker Compose and includes logging, validation, scheduling, and optional cloud exports.  
✅ *Planned API services and spatial analysis tasks will simulate full-stack GIS engineering workflows used in enterprise settings.*

---

## 📌 Table of Contents

- [Project Goals](#project-goals)
- [Tech Stack](#tech-stack)
- [Pipeline Architecture](#pipeline-architecture)
- [Key Features](#key-features)
- [Spatial API Server (Planned)](#spatial-api-server-planned)
- [Example Spatial Analysis Tasks (Planned)](#example-spatial-analysis-tasks-planned)
- [Testing](#testing)
- [Mocked / Proprietary Replacements](#mocked--proprietary-replacements)
- [Future Enhancements](#future-enhancements)
- [Example Data Sources](#example-data-sources)
- [Getting Started](#getting-started)
- [Running the Pipeline](#running-the-pipeline)

---

## Project Goals

- Demonstrate GIS-related ETL skills aligned with industry roles
- Handle open geospatial data in modern formats (GeoJSON, Shapefile, Parquet)
- Replace proprietary ArcGIS tools with open-source equivalents
- Serve as a learning sandbox for spatial processing, cloud integration, and orchestration
- ✅ Simulate enterprise-level REST APIs for geospatial data querying
- ✅ Demonstrate basic spatial modeling (joins, buffers, proximity analysis)
- ✅ Practice object-oriented design in Python data pipeline components

---

## Tech Stack

| Layer               | Technology                            | Purpose                                  |
|---------------------|----------------------------------------|------------------------------------------|
| Language            | Python 3.10+                           | Core scripting (OOP structured) ✅        |
| Orchestration       | Apache Airflow                         | DAGs and scheduling                      |
| Containerization    | Docker, Docker Compose                 | Local dev & deployment                   |
| Spatial ETL         | GeoPandas, Shapely, Fiona, Pyproj      | Geospatial parsing & processing          |
| Data Store          | PostgreSQL + PostGIS                   | Spatially-aware relational DB            |
| REST API Server     | FastAPI or Flask (Planned) ✅          | Serve spatial queries via endpoints      |
| Spatial Analysis    | Shapely, PostGIS functions ✅           | Buffers, joins, intersections, etc.      |
| Cloud Storage       | AWS S3 (optional)                      | Data lake export                         |
| Public APIs         | USGS, OpenStreetMap, Natural Earth     | Geo data sources                         |
| Alerting            | Slack Webhooks (Free Tier)             | Pipeline status notifications            |
| Testing             | pytest                                 | Unit and integration testing             |
| Monitoring          | Airflow Logs + Slack Alerts            | Observability                            |
| Front-End Mapping   | Leaflet.js or Folium (Planned) ✅       | Visualize output data                    |

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

└────────────────────────────┘
```

--- 

Key Features

    Modular Airflow DAGs for staging ETL steps

    Geospatial data validation: CRS, geometry integrity, schema

    CRS Handling: Normalize geometries to EPSG:4326

    Local + optional cloud storage (S3)

    Logging: Airflow logs + Slack alerts

    ✅ Object-oriented Python modules used for pipeline stages

    ✅ Simulated API server to access spatial datasets via REST endpoints

    ✅ Support for spatial joins (e.g., pipelines near roads) and buffering tasks

---

## Spatial API Server (Planned)

The FastAPI service (under /api/) will expose endpoints for:

    /roads-near-pipelines?distance=100: returns all roads within 100 meters of pipeline features

    /assets/geojson: return full feature sets in GeoJSON format for download or mapping

    /metrics: returns spatial quality checks (e.g., invalid geometries, CRS mismatches)

---

## Example Spatial Analysis Tasks (Planned)

As part of the transformation phase:

    Buffer Zones: Identify assets within 100m of critical infrastructure

    Spatial Joins: Merge attributes between overlapping layers (e.g., pipes + roads)

    Filtering by Geometry Type: Extract only LineString or Polygon features

    PostGIS Spatial Queries: Perform joins and analytics directly in SQL

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
