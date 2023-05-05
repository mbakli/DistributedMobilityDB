# Distributed MobilityDB
Distributed MobilityDB is a PostgreSQL extension that extends the open source databases PostGIS and MobilityDB to distribute spatial and spatiotemporal data and queries. It contains the following features:
* Spatial and Spatiotemporal Data Partitioning
  * It partitions the input relation into shards that preserve spatiotemporal data locality and load balancing.
  * It provides a two-level (global and local) distributed indexing scheme to reduce the global transmission cost and local computation cost.
* Spatial and Spatiotemporal Processing
  * It provides an adaptive execution engine that transforms a SQL query into a distributed plan that is executed on a cluster.
* Declarative Query Language
  * It provides declarative SQL functions for data partitioning as well as mapping declarative SQL queries into distributed execution strategies.

🚧 **Please note that the extension is still under heavy development, so stay tuned for more updates and features.** 🚧

# Prerequisites
- PostgreSQL >= 13
- Citus >= 10
- PostGIS >= 3
- MobilityDB >= 1.1

# How to install it?
For Linux

	git clone https://github.com/mbakli/DistributedMobilityDB
    make build
	cd build
	cmake  ..
	make
	sudo make install

Postgresql

	psql -c 'CREATE EXTENSION Distributed_MobilityDB CASCADE'

-----------------------------------------------------------------------------------------------------------------------
## Using Distributed MobilityDB

### Creating Distributed Tables

The create_spatiotemporal_distributed_table () function is utilized to define a distributed table that is partitioned using one of the Multidimensional Tiling methods. It splits the input table into several tiles stored in separate PostgreSQL tables.

func: create_spatiotemporal_distributed_table
#### Arguments:
- <ins>table_name_in</ins>: Name of the input table
- <ins>num_tiles</ins>: Number of generated tiles
- <ins>table_name_out</ins>: Name of the distributed table
- <ins>tiling_method</ins>: Name of the tiling method: <ins>crange</ins>, <ins>hierarchical</ins>
- <ins>tiling_granularity</ins> (Optional): The tiling granularity. The default value depends on the granularity selection process of the tiling method that chooses between shape- and point-based strategies to create load-balanced tiles. The user can set this parameter to customize the tiling granularity.
- <ins>tiling_type</ins> (Optional): The tiling type of the tiling method. It can be one of the following: temporal, spatial, or spatiotemporal. The default value depends on the given column type.
- <ins>colocation_table</ins> (Optional): This argument allows you to colocate the input table with another table.  For example, you can use this feature to create tiles based on given boundaries such as province borders. By specifying the colocation_table and colocation_column arguments, you can ensure that your data is organized and managed in a way that suits your specific needs.
- <ins>colocation_column</ins> (Optional): Specify the colocation column.
- <ins>physical_partitioning</ins> (Optional): Determine whether or not to physically partition data.
- <ins>object_segmentation</ins> (Optional): Determine whether or not to segment the input spatiotemporal column.

By utilizing the create_spatiotemporal_distributed_table() function with these arguments, you can easily create a distributed table that suits your data management needs. 

-----------------------------------------------------------------------------------------------------------------------
# Use Cases

### OSM Data
```sql
CREATE TABLE planet_osm_polygon (
  osm_id bigint,
  way geometry(polygon),
  ...
);

CREATE TABLE planet_osm_roads (
  osm_id bigint,
  way geometry(linestring),
  ...
);

-- Distribute the planet_osm_polygon table into 50 tiles using the spatial column: geometry(polygon)
SELECT create_spatiotemporal_distributed_table(table_name_in => 'planet_osm_polygon', num_tiles =>50, table_name_out=>'planet_osm_polygon_50t', tiling_method => 'crange');

-- Distribute the planet_osm_roads table into 30 tiles using the spatial column: geometry(polygon)
SELECT create_spatiotemporal_distributed_table(table_name_in => 'planet_osm_roads', num_tiles =>30, table_name_out=>'planet_osm_roads_30t', tiling_method => 'crange');

-- Distance Query: Find buildings that are built within 1km of the primary highways.
SELECT distinct t1.name
FROM planet_osm_polygon_50t t1, planet_osm_roads_30t t2
WHERE t1.building = 'yes'
  AND t2.highway = 'primary'
  AND ST_dwithin(t1.way,t2.way, 1000);
```
### AIS Trajectory Data

```sql
CREATE TABLE ships (
  mmsi int,
  trip tgeompoint(sequence),
  ...
);

-- Distribute the ships table into 50 tiles using the spatiotemporal column: tgeompoint(sequence)
-- Spatiotemporal tiling
SELECT create_spatiotemporal_distributed_table(table_name_in => 'ships', num_tiles =>50, table_name_out=>'ships_50t', partitioning_method => 'crange', tiling_type =>'spatiotemporal');
-- Spatial tiling
SELECT create_spatiotemporal_distributed_table(table_name_in => 'ships', num_tiles =>50, table_name_out=>'ships_50t', partitioning_method => 'crange', tiling_type =>'spatial');
-- Temporal tiling
SELECT create_spatiotemporal_distributed_table(table_name_in => 'ships', num_tiles =>50, table_name_out=>'ships_50t', partitioning_method => 'crange', tiling_type =>'temporal');
```