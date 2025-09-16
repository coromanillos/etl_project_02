# 🌐 Geospatial Data Engineering Pipeline for Public Infrastructure

This project demonstrates a **production-grade GIS data pipeline** inspired by real-world utility operations (e.g., water, energy, urban infrastructure). It extracts, transforms, and loads open-source spatial data using cloud-compatible formats and modern orchestration with Apache Airflow.

The pipeline is designed for **modular deployment** via Docker Compose and includes logging, validation, scheduling, and optional cloud exports.  
✅ *Planned API services and spatial analysis tasks will simulate full-stack GIS engineering workflows used in enterprise settings.*

---

## 📌 Table of Contents

- [Project Goals](#project-goals)
- [Tech Stack](#tech-stack)
- [USGS API Endpoints](#usgs-api-endpoints)
- [Testing](#testing)
- [Future Enhancements](#future-enhancements)
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
- API endpoints: api.waterdata.usgs.gov
  - Parameter Codes, Daily Values, Monitoring Locations
- Dependency Management: Poetry

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

3. Parameter Codes

Endpoint: /collections/parameter-codes/items

Purpose: Parameter codes are 5-digit codes used to identify the constituent measured and the units of measure.

Key Fields:

monitoring_location_id (FK → monitoring-locations.id)

observed_property_id

Use Cases: Flood alerts, operational dashboards, drought monitoring.

---

## Testing

Tests are written with pytest and include both unit, integration and e2e coverage:

pytest tests/

    Validate schema mappings and geometry correctness

    Test API fallback behavior

    Integration test from extract → load using dummy data

    (Planned) API response tests and spatial query validation

---

## Future Enhancements

    CI/CD integration for deployment (GitHub Actions or Jenkins)

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


# TODO
- usgs_etl_dag.py can be a dynamic DAG that reads endpoints from config.yaml automatically, this way I do not have to update the DAG whenever I add
a new pipeline for an endpoint (more config driven).