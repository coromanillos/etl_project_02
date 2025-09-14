-- ##########################################################
-- Unified Analyst-Friendly View
-- Author: Christopher O. Romanillos
-- Purpose: Provide a clean, denormalized dataset for analysts
--          without GIS/geometry fields.
--
-- Usage:
--   SELECT * FROM unified_analyst_data;
-- ##########################################################

-- Drop view if it exists to avoid conflicts
DROP VIEW IF EXISTS unified_analyst_data;

-- Create the unified view
CREATE VIEW unified_analyst_data AS
SELECT
    -- Daily values
    dv.uuid AS record_id,
    dv.time AS observation_date,
    dv.value::FLOAT AS observed_value,
    dv.unit_of_measure AS value_unit,
    dv.approval_status,
    dv.qualifier,
    dv.last_modified,

    -- Parameter info
    pc.id AS parameter_code,
    pc.parameter_name,
    pc.parameter_description,
    pc.parameter_group_code,
    pc.unit_of_measure AS parameter_unit,
    pc.medium,
    pc.statistical_basis,
    pc.time_basis,
    pc.sample_fraction,
    pc.temperature_basis,
    pc.epa_equivalence,

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
    ml.altitude,
    ml.altitude_accuracy,
    ml.vertical_datum,
    ml.vertical_datum_name,
    ml.aquifer_code,
    ml.national_aquifer_code,
    ml.aquifer_type_code,
    ml.well_constructed_depth,
    ml.hole_constructed_depth

FROM daily_values dv
JOIN monitoring_locations ml
    ON dv.monitoring_location_id = ml.id
JOIN parameter_codes pc
    ON dv.parameter_code = pc.id;
