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
    srid int,
    groupcol varchar(50)
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
--
-- GetPredicateOidAndArgs (src/planner/predicate_management.c) reads an
-- OpExpr predicate's *operator* oid (opExpr->opno), not the oid of the
-- function implementing it -- registering only pg_proc.oid values (as the
-- two SELECTs below do) means an operator-written predicate like
-- `t.Trip && p.Period` (the `&&` bbox/temporal-overlap operator,
-- implemented by temporal_overlaps/span_overlaps/etc., but a *different*
-- oid than those functions') never matched, even after the function
-- itself was registered. For queries that use `&&` as their *only*
-- spatiotemporal predicate (no accompanying eintersects/ST_Intersects/
-- eDwithin, e.g. BerlinMOD Q8's "was this vehicle active during this
-- period" check), that meant this extension's planner never engaged at
-- all -- no tile pruning, none of its tile-boundary deduplication --
-- reproduced as genuinely wrong (duplicated) results. The third SELECT
-- below registers the `&&` *operator*'s own oid (from pg_operator) for
-- every temporal/spatiotemporal type combination it's defined over.
INSERT INTO dist_mobilitydb.pg_spatiotemporal_join_operations(op, opid, distance)
SELECT proname,oid,true
FROM pg_proc
WHERE proname like ANY(ARRAY['%dwithin%', '%distance%'])
union all
SELECT proname,oid,false
FROM pg_proc
WHERE proname like ANY(ARRAY['%intersects%', '%contains%', '%disjoint%', '%overlaps%'])
union all
SELECT '&&', o.oid, false
FROM pg_operator o
WHERE o.oprname = '&&'
  AND (o.oprleft::regtype::text ~ 'tgeompoint|tgeogpoint|tbool|tint|tfloat|ttext|tnpoint|tstz'
       OR o.oprright::regtype::text ~ 'tgeompoint|tgeogpoint|tbool|tint|tfloat|ttext|tnpoint|tstz');

ALTER TABLE dist_mobilitydb.pg_spatiotemporal_join_operations
SET SCHEMA pg_catalog;

CREATE TABLE dist_mobilitydb.pg_spatiotemporal_join_operations_desc(
    id serial primary key,
    op text,
    opid integer,
    requireFullShape boolean
);

ALTER TABLE dist_mobilitydb.pg_spatiotemporal_join_operations_desc SET SCHEMA pg_catalog;
-- Create distributed functions for the MobilityDB query operations
CREATE TABLE dist_mobilitydb.pg_execution_run(
    id serial primary key,
    sExec text default 'all'
);

INSERT INTO dist_mobilitydb.pg_execution_run (sExec) values('run-once'), ('all');
ALTER TABLE dist_mobilitydb.pg_execution_run
SET SCHEMA pg_catalog;

CREATE TABLE dist_mobilitydb.pg_dist_spatiotemporal_dist_functions(
    id serial,
    worker text default NULL,
    combiner text default NULL,
    final text default NULL,
    sExec_id integer REFERENCES pg_execution_run(id) default 2
);
ALTER TABLE dist_mobilitydb.pg_dist_spatiotemporal_dist_functions
SET SCHEMA pg_catalog;

-- Tracks, per (base table, distance threshold), the row count the
-- reshuffled/colocated table (built by createReshuffledTable/ColocateRte in
-- multi_phase_executor.c) was last copied from. A real (non-EXPLAIN)
-- NonColocation-strategy query used to drop/recreate/re-copy/re-index that
-- whole table on every single execution, even back-to-back against an
-- unchanged base table -- measured as 48s of a 175s total run against a
-- 69,839-row table. Before paying that cost again, the executor re-counts
-- the base table (cheap: a parallel per-shard COUNT(*)) and compares it
-- against base_row_count here; a match means the base table hasn't
-- grown/shrunk since, so the existing reshuffled table is reused as-is.
-- This is a row-count fingerprint, not a full content hash: it catches
-- size-changing INSERT/DELETE workloads (this project's actual usage
-- pattern -- a table built once via create_spatiotemporal_distributed_table,
-- then queried repeatedly) but not a same-row-count in-place UPDATE.
--
-- distance is part of the key, not an afterthought: for a distance
-- predicate (eDwithin/etc), DistanceReshufflingPlan (planner_strategies.c)
-- bakes the query's own distance threshold directly into the tile-pairing
-- query that decides which rows get duplicated across tile boundaries --
-- two queries against the same base table with different thresholds need
-- genuinely different reshuffled tables. Keying the cache on base_table_oid
-- alone (as a first version of this cache did) would let a later query with
-- a larger threshold silently reuse a table built for a smaller one and
-- miss cross-tile matches it should have found. -1 is the sentinel for "not
-- a distance predicate" (OtherReshufflingPlan, e.g. plain intersection --
-- its reshuffling doesn't depend on any distance value at all), kept
-- distinct from a real, legitimate eDwithin(..., 0) threshold.
CREATE TABLE dist_mobilitydb.pg_dist_spatiotemporal_reshuffle_cache(
    base_table_oid oid NOT NULL,
    distance double precision NOT NULL DEFAULT -1,
    reshuffled_table text NOT NULL,
    base_row_count bigint NOT NULL,
    cached_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (base_table_oid, distance)
);

ALTER TABLE dist_mobilitydb.pg_dist_spatiotemporal_reshuffle_cache
SET SCHEMA pg_catalog;

ALTER TYPE dist_mobilitydb.tiling
SET SCHEMA pg_catalog;
