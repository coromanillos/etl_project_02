-- ##########################################################
-- PostgreSQL / PostGIS SQL Script Generated from ORM Schemas
-- Author: Christopher O. Romanillos
-- bash command: cat sql/schema.sql | docker exec -i airflow_postgis psql -U airflow -d airflow
-- don't forget to run the postgis container, haha
-- I should have never used alembic, I don't plan to have this
-- schema update regularly, it was far too much overhead and 
-- boilerplate compared to plain old SQL...
-- ##########################################################

-- Enable the PostGIS extension if not already enabled
CREATE EXTENSION IF NOT EXISTS postgis;

-- ##########################################################
-- Table: monitoring_locations
-- ##########################################################
CREATE TABLE monitoring_locations (
    id VARCHAR(50) PRIMARY KEY,
    agency_code VARCHAR(5) NOT NULL,
    agency_name VARCHAR(50),
    monitoring_location_number TEXT,
    monitoring_location_name TEXT,
    district_code TEXT,
    country_code TEXT,
    country_name TEXT,
    state_code TEXT,
    state_name TEXT,
    county_code TEXT,
    county_name TEXT,
    minor_civil_division_code TEXT,
    site_type_code TEXT,
    site_type TEXT,
    hydrologic_unit_code TEXT,
    basin_code TEXT,
    altitude FLOAT,
    altitude_accuracy FLOAT,
    altitude_method_code TEXT,
    altitude_method_name TEXT,
    vertical_datum TEXT,
    vertical_datum_name TEXT,
    horizontal_positional_accuracy_code TEXT,
    horizontal_positional_accuracy TEXT,
    horizontal_position_method_code TEXT,
    horizontal_position_method_name TEXT,
    original_horizontal_datum TEXT,
    original_horizontal_datum_name TEXT,
    drainage_area FLOAT,
    contributing_drainage_area FLOAT,
    time_zone_abbreviation TEXT,
    uses_daylight_savings BOOLEAN,
    construction_date TEXT,
    aquifer_code TEXT,
    national_aquifer_code TEXT,
    aquifer_type_code TEXT,
    well_constructed_depth FLOAT,
    hole_constructed_depth FLOAT,
    depth_source_code TEXT,
    geometry GEOMETRY(POINT, 4326)
);

-- Indexes for monitoring_locations
CREATE INDEX idx_state_code ON monitoring_locations(state_code);
CREATE INDEX idx_agency_code ON monitoring_locations(agency_code);
CREATE INDEX idx_geometry_monitoring_locations ON monitoring_locations USING GIST(geometry);

-- ##########################################################
-- Table: parameter_codes
-- ##########################################################
CREATE TABLE parameter_codes (
    id VARCHAR(10) PRIMARY KEY,
    parameter_name VARCHAR,
    unit_of_measure VARCHAR,
    parameter_group_code VARCHAR,
    parameter_description VARCHAR,
    medium TEXT,
    statistical_basis VARCHAR,
    time_basis VARCHAR,
    weight_basis VARCHAR,
    particle_size_basis VARCHAR,
    sample_fraction VARCHAR,
    temperature_basis VARCHAR,
    epa_equivalence VARCHAR,
    geometry GEOMETRY(GEOMETRY, 4326)
);

-- Indexes for parameter_codes
CREATE INDEX idx_parameter_group_code ON parameter_codes(parameter_group_code);
CREATE INDEX idx_geometry_parameter_codes ON parameter_codes USING GIST(geometry);

-- ##########################################################
-- Table: daily_values
-- ##########################################################
CREATE TABLE daily_values (
    uuid UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id VARCHAR(50) NOT NULL,
    time_series_id VARCHAR(50) NOT NULL,
    monitoring_location_id VARCHAR(50) NOT NULL REFERENCES monitoring_locations(id) ON DELETE CASCADE,
    parameter_code VARCHAR(50) NOT NULL REFERENCES parameter_codes(id) ON DELETE CASCADE,
    statistic_id VARCHAR(50) NOT NULL,
    time DATE NOT NULL,
    last_modified TIMESTAMP,
    value TEXT,
    unit_of_measure TEXT,
    approval_status TEXT,
    qualifier TEXT,
    geometry GEOMETRY(POINT, 4326)
);

-- Indexes for daily_values
CREATE INDEX idx_time_series_id ON daily_values(time_series_id);
CREATE INDEX idx_monitoring_location_id ON daily_values(monitoring_location_id);
CREATE INDEX idx_parameter_code ON daily_values(parameter_code);
CREATE INDEX idx_geometry_daily_values ON daily_values USING GIST(geometry);
