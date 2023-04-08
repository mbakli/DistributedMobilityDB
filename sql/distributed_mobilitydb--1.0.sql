--complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION distributed_mobilitydb" to load this file. \quit

CREATE SCHEMA dist_mobilitydb;

SET search_path = 'pg_catalog';

---------------------------------------------------------------------------------
-- Spatiotemporal tiling methods
---------------------------------------------------------------------------------
CREATE TYPE dist_mobilitydb.spatiotemporal_tiling_method AS ENUM (
    'crange',
    'hierarchical',
    'str',
    'quadtree',
    'octree'
);

ALTER TYPE dist_mobilitydb.spatiotemporal_tiling_method
SET SCHEMA pg_catalog;

CREATE TYPE dist_mobilitydb.tiling AS (
    type text,
    method text,
    numTiles int,
    groupCol text,
    isMobilityDB boolean,
    disjointTiles boolean,
    internalType text,
    distColType text,
    tileKey text,
    granularity text,
    segmentation boolean,
    srid int,
    distCol text
);

-- The other variables will be added during the execution of some tasks
CREATE TYPE dist_mobilitydb.tileSize AS (
    numShapes int,
    numPoints  int
    );

ALTER TYPE dist_mobilitydb.tileSize
SET SCHEMA pg_catalog;
---------------------------------------------------------------------------------
-- Catalog Tables
---------------------------------------------------------------------------------
CREATE TABLE dist_mobilitydb.pg_dist_spatiotemporal_tables(
    id serial,
    tblOid oid,
    tableName varchar(100) unique,
    numTiles integer,
    tilingMethod varchar(100), /* TODO: Change it to ENUM*/
    tilingType varchar(100), /* TODO: Change it to ENUM*/
    granularity varchar(100), /* TODO: Change it to ENUM*/
    disjoint boolean,
    isMobilityDB boolean,
    distcol varchar(20),
    distcoltype varchar(10),
    tilekey varchar(10),
    shapeSegmented boolean,
    srid int
);

ALTER TABLE dist_mobilitydb.pg_dist_spatiotemporal_tables
SET SCHEMA pg_catalog;

SET search_path = 'public';

CREATE TABLE dist_mobilitydb.pg_dist_spatiotemporal_tiles(
    id serial,
    table_id integer,
    tile_key integer,
    mobdb_bbox stbox,
    postgis_bbox geometry,
    num_shapes bigint,
    num_points bigint
);

ALTER TABLE dist_mobilitydb.pg_dist_spatiotemporal_tiles
SET SCHEMA pg_catalog;

-- Table for understanding if this operation is a distance or not
CREATE TABLE dist_mobilitydb.pg_spatiotemporal_join_operations(
    id serial,
    op text,
    opid integer,
    distance boolean
);

-- Add the OID for the distance and intersection query operations
INSERT INTO dist_mobilitydb.pg_spatiotemporal_join_operations(op, opid, distance)
SELECT proname,oid,true
FROM pg_proc
WHERE proname like ANY(ARRAY['%dwithin%', '%distance%'])
union all
SELECT proname,oid,false
FROM pg_proc
WHERE proname like ANY(ARRAY['%intersects%', '%contains%', '%disjoint%']);

ALTER TABLE dist_mobilitydb.pg_spatiotemporal_join_operations
SET SCHEMA pg_catalog;

CREATE TABLE dist_mobilitydb.pg_spatiotemporal_join_operations_desc(
    id serial primary key,
    op text,
    opid integer,
    requireFullTrip boolean
);

ALTER TABLE dist_mobilitydb.pg_spatiotemporal_join_operations_desc SET SCHEMA pg_catalog;
-- Create distributed functions for the MobilityDB query operations
CREATE TABLE dist_mobilitydb.pg_dist_spatiotemporal_dist_functions(
    id serial,
    op_id_fk integer REFERENCES pg_spatiotemporal_join_operations_desc(id),
    worker text default NULL,
    combiner text default NULL,
    final text default NULL
);
ALTER TABLE dist_mobilitydb.pg_dist_spatiotemporal_dist_functions
SET SCHEMA pg_catalog;
ALTER TYPE dist_mobilitydb.tiling
SET SCHEMA pg_catalog;
