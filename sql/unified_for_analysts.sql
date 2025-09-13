-- ##########################################################
-- Unified GIS Analyst-Friendly View
-- Author: Christopher O. Romanillos
-- This script creates a single queryable view that combines
-- daily values, monitoring locations, and parameter codes
-- into one analyst-ready dataset.
--
-- Usage:
--   Run in PostGIS after loading data
--   Analysts can query SELECT * FROM unified_gis_data;
--   Or export using ogr2ogr, psql \copy, etc.
-- ##########################################################

-- Drop view if it exists to avoid conflicts
DROP VIEW IF EXISTS unified_gis_data;

-- Create the unified view
CREATE VIEW unified_gis_data AS
SELECT
    dv.uuid AS record_id,
    dv.time AS observation_date,
    dv.value::FLOAT AS observed_value,
    dv.unit_of_measure AS value_unit,
    dv.approval_status,
    dv.qualifier,

    -- Parameter info
    pc.id AS parameter_code,
    pc.parameter_name,
    pc.parameter_description,
    pc.parameter_group_code,
    pc.unit_of_measure AS parameter_unit,
    pc.medium,

    -- Monitoring location info
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

    -- Timestamps
    dv.last_modified,

    -- GIS field (point geometry in EPSG:4326)
    dv.geometry AS geom
FROM daily_values dv
JOIN monitoring_locations ml
    ON dv.monitoring_location_id = ml.id
JOIN parameter_codes pc
    ON dv.parameter_code = pc.id;

-- Index for faster querying of the view
-- (actually indexes apply to underlying tables, but materializing could help)
-- Optionally: create materialized view for performance
