# Distributed MobilityDB
Distributed MobilityDB is a PostgreSQL extension that extends the open source databases PostGIS and MobilityDB to distribute spatial and spatiotemporal data and queries. It contains the following features:
* Spatial and Spatiotemporal Data Partitioning
  * It partitions the input relation into shards that preserve spatiotemporal data locality and load balancing.
  * It provides a two-level (global and local) distributed indexing scheme to reduce the global transmission cost and local computation cost.
* Spatial and Spatiotemporal Processing
  * It provides an execution engine that transforms a SQL query into a distributed plan that is executed on a cluster.
* Declarative Query Language
  * It provides declarative SQL functions for data partitioning as well as mapping declarative SQL queries into distributed execution strategies.

# Prerequisites
- PostgreSQL
- Citus
- PostGIS or MobilityDB

# How to install it?
For Linux

	git clone https://github.com/mbakli/DistributedMobilityDB
    make build
	cd build
	cmake  ..
	make
	sudo make install

Postgresql

	psql -c 'CREATE EXTENSION DistributedMobilityDB CASCADE'

-----------------------------------------------------------------------------------------------------------------------
## Using Distributed MobilityDB

### Creating Distributed Tables

The create_spatiotemporal_distributed_table () function is utilized to define a distributed table that is partitioned using one of the Multidimensional Tiling methods. It splits the input table into several tiles that are stored in separate PostgreSQL tables.

func: create_spatiotemporal_distributed_table
#### Arguments:
- <ins>table_name_in</ins>: Name of the input table
- <ins>num_tiles</ins>: Number of generated tiles
- <ins>table_name_out</ins>: Name of the distributed table
- <ins>partitioning_method</ins>: Name of the tiling method: <ins>crange</ins>, <ins>hierarchical</ins>
- <ins>partitioning_type</ins> (Optional): The partitioning granularity of the partitioned method. It can be one of the following: temporal, spatial, spatiotemporal. The default value depends on the given column type.
- <ins>colocation_table</ins> (Optional): Colocate the input table with another table
- <ins>colocation_column</ins> (Optional): The colocation column
- <ins>physical_partitioning</ins> (Optional): Determine whether or not to physically partition data.
- <ins>object_segmentation</ins> (Optional): Determine whether or not to segment the input spatiotemporal column.
-----------------------------------------------------------------------------------------------------------------------
# Use Cases

### OSM Data
```sql
CREATE TABLE planet_osm_polygon (
  osm_id bigint,
  way geometry(polygon),
  ...
);

-- Distribute the planet_osm_polygon table into 50 tiles using the spatial column: geometry(polygon)
SELECT create_spatiotemporal_distributed_table(table_name_in => 'planet_osm_polygon', num_tiles =>50, table_name_out=>'planet_osm_point_50t', partitioning_method => 'crange');
```
### AIS Trajectory Data

```sql
CREATE TABLE ships (
  mmsi int,
  trip tgeompoint(sequence),
  ...
);

-- Distribute the planet_osm_polygon table into 50 tiles using the spatiotemporal column: tgeompoint(sequence)
-- Spatiotemporal tiling
SELECT create_spatiotemporal_distributed_table(table_name_in => 'planet_osm_polygon', num_tiles =>50, table_name_out=>'planet_osm_point_50t', partitioning_method => 'crange', partitioning_type =>'spatiotemporal');
-- Spatial tiling
SELECT create_spatiotemporal_distributed_table(table_name_in => 'planet_osm_polygon', num_tiles =>50, table_name_out=>'planet_osm_point_50t', partitioning_method => 'crange', partitioning_type =>'spatial');
-- Temporal tiling
SELECT create_spatiotemporal_distributed_table(table_name_in => 'planet_osm_polygon', num_tiles =>50, table_name_out=>'planet_osm_point_50t', partitioning_method => 'crange', partitioning_type =>'temporal');
```
### BerlinMOD Benchmark