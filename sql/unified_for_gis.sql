-- ##########################################################
-- Unified GIS View for USGS Data
-- Author: Christopher O. Romanillos
-- Description: Combines monitoring locations, daily values,
--              and parameter codes into a spatially rich dataset
-- ##########################################################

-- Drop the view if it exists (safe re-run)
DROP VIEW IF EXISTS unified_usgs_gis;

-- Create the unified GIS view
CREATE VIEW unified_usgs_gis AS
SELECT
    dv.uuid,
    dv.time,
    dv.value::DOUBLE PRECISION AS value,
    dv.unit_of_measure AS value_unit,
    dv.approval_status,
    dv.qualifier,
    dv.last_modified,

    -- Monitoring location attributes
    ml.id AS location_id,
    ml.monitoring_location_name,
    ml.state_name,
    ml.county_name,
    ml.site_type,
    ml.hydrologic_unit_code,
    ml.altitude,
    ml.drainage_area,
    ml.contributing_drainage_area,

    -- Parameter metadata
    pc.id AS parameter_code,
    pc.parameter_name,
    pc.unit_of_measure AS parameter_unit,
    pc.parameter_description,
    pc.parameter_group_code,
    pc.medium,

    -- Geometry: prefer daily_values geometry, fall back to monitoring location
    COALESCE(dv.geometry, ml.geometry) AS geom

FROM daily_values dv
JOIN monitoring_locations ml
    ON dv.monitoring_location_id = ml.id
JOIN parameter_codes pc
    ON dv.parameter_code = pc.id;

-- Spatial index recommendation (optional, if materialized later)
-- CREATE INDEX idx_unified_usgs_gis_geom ON unified_usgs_gis USING GIST(geom);

-- ##########################################################
-- Usage:
-- SELECT * FROM unified_usgs_gis LIMIT 100;
-- Export with ogr2ogr:
-- ogr2ogr -f GPKG unified_usgs.gpkg PG:"dbname=airflow user=airflow" unified_usgs_gis
-- ##########################################################
