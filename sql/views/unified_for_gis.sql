-- ##########################################################
-- Unified GIS Specialist View
-- Author: Christopher O. Romanillos
-- Purpose:
--   Create a spatially rich unified view for GIS specialists.
--   Includes monitoring site attributes, parameter metadata,
--   and daily values, anchored to monitoring_locations geometry.
--
-- Usage:
--   Run in PostGIS after schema is loaded.
--   GIS specialists can query or export to GeoPackage:
--     ogr2ogr -f GPKG output.gpkg \
--       PG:"dbname=airflow user=airflow" unified_gis_view
--
-- Notes:
--   - Geometry is POINT in EPSG:4326 (WGS84)
--   - Only one geometry column (ml.geometry) for ArcGIS/QGIS compatibility
-- ##########################################################

DROP VIEW IF EXISTS unified_gis_view;

CREATE VIEW unified_gis_view AS
SELECT
    -- ================================
    -- Daily values
    -- ================================
    dv.uuid AS record_id,
    dv.time AS observation_date,
    NULLIF(dv.value, '')::FLOAT AS observed_value,
    dv.unit_of_measure AS observed_unit,
    dv.approval_status,
    dv.qualifier,
    dv.last_modified,

    -- ================================
    -- Parameter metadata
    -- ================================
    pc.id AS parameter_code,
    pc.parameter_name,
    pc.parameter_description,
    pc.parameter_group_code,
    pc.unit_of_measure AS parameter_unit,
    pc.medium,
    pc.statistical_basis,
    pc.time_basis,
    pc.sample_fraction,

    -- ================================
    -- Monitoring site attributes
    -- ================================
    ml.id AS monitoring_location_id,
    ml.monitoring_location_name,
    ml.monitoring_location_number,
    ml.agency_code,
    ml.agency_name,
    ml.state_code,
    ml.state_name,
    ml.county_code,
    ml.county_name,
    ml.hydrologic_unit_code,
    ml.drainage_area,
    ml.contributing_drainage_area,
    ml.altitude,
    ml.vertical_datum_name,
    ml.aquifer_code,
    ml.national_aquifer_code,
    ml.aquifer_type_code,

    -- ================================
    -- Anchor geometry for GIS use
    -- ================================
    ml.geometry AS geom
FROM daily_values dv
JOIN monitoring_locations ml
    ON dv.monitoring_location_id = ml.id
JOIN parameter_codes pc
    ON dv.parameter_code = pc.id;
