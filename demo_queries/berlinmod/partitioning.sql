-----------------------------------------------------------------------------------------------------------------------
-- BerlinMOD setup: distribute Trips as a spatiotemporal-tiled table, and the
-- reference tables the BerlinMOD/R queries join against (Vehicles, Licences,
-- Points, Regions, Instants, Periods) as Citus reference tables -- both via
-- create_spatiotemporal_distributed_table(), using its is_reference_table
-- flag for the latter.
--
-- Assumes the BerlinMOD data has already been generated/loaded, e.g. via
-- https://github.com/MobilityDB/MobilityDB-BerlinMOD (berlinmod_datagenerator.sql
-- or berlinmod_load.sql), so tables Trips, Vehicles, Licences, Points,
-- Regions, Instants and Periods already exist and are populated.
--
-- Each source table is renamed to lowercase first (e.g. Trips -> trips) so
-- create_spatiotemporal_distributed_table() can create its distributed
-- output under a new, descriptive name: trips_16t for the tiled table
-- (matching the "_Nt" convention used elsewhere in this repo's demos), and
-- <table>_ref for each reference table.
-----------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------
-- Trips
-- The one distributed spatiotemporal table.
-----------------------------------------------------------------------------------------------------------------------
SELECT create_spatiotemporal_distributed_table(table_name_in => 'trips', table_name_out => 'trips_16t',
  num_tiles => 16, tiling_method => 'crange', tiling_type => 'spatiotemporal');

-----------------------------------------------------------------------------------------------------------------------
-- Reference tables
-- Replicated to every node (num_tiles is omitted -- it's not meaningful for
-- a reference table, and defaults to the only value is_reference_table
-- accepts), so they can be joined against trips_16t without any
-- repartitioning.
-----------------------------------------------------------------------------------------------------------------------
SELECT create_spatiotemporal_distributed_table(table_name_in => 'vehicles', table_name_out => 'vehicles_ref',
  is_reference_table => true);

SELECT create_spatiotemporal_distributed_table(table_name_in => 'licences', table_name_out => 'licences_ref',
  is_reference_table => true);

SELECT create_spatiotemporal_distributed_table(table_name_in => 'points', table_name_out => 'points_ref',
  is_reference_table => true);

SELECT create_spatiotemporal_distributed_table(table_name_in => 'regions', table_name_out => 'regions_ref',
  is_reference_table => true);

SELECT create_spatiotemporal_distributed_table(table_name_in => 'instants', table_name_out => 'instants_ref',
  is_reference_table => true);

SELECT create_spatiotemporal_distributed_table(table_name_in => 'periods', table_name_out => 'periods_ref',
  is_reference_table => true);

-----------------------------------------------------------------------------------------------------------------------
-- Sample views
-- The standard queries restrict several reference tables to a small sample
-- (suffix 1/2) to keep query result sizes reasonable.
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW Licences1 (LicenceId, Licence, VehicleId) AS
  SELECT LicenceId, Licence, VehicleId FROM licences_ref LIMIT 10;
CREATE OR REPLACE VIEW Licences2 (LicenceId, Licence, VehicleId) AS
  SELECT LicenceId, Licence, VehicleId FROM licences_ref LIMIT 10 OFFSET 10;
CREATE OR REPLACE VIEW Points1 (PointId, Geom) AS
  SELECT PointId, Geom FROM points_ref LIMIT 10;
CREATE OR REPLACE VIEW Regions1 (RegionId, Geom) AS
  SELECT RegionId, Geom FROM regions_ref LIMIT 10;
CREATE OR REPLACE VIEW Instants1 (InstantId, Instant) AS
  SELECT InstantId, Instant FROM instants_ref LIMIT 10;
CREATE OR REPLACE VIEW Periods1 (PeriodId, Period) AS
  SELECT PeriodId, Period FROM periods_ref LIMIT 10;
