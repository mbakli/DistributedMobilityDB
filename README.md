# Distributed MobilityDB
Distributed MobilityDB is a PostgreSQL extension that extends the open source databases PostGIS and MobilityDB to distribute spatial and spatiotemporal data and queries.

# Key Features

* **Spatial and Spatiotemporal Data Partitioning**
  * Transform the input relation into a multirelation, preserving spatiotemporal data locality and load balancing.
  * Develop a two-level (global and local) distributed indexing scheme, effectively reducing the global transmission cost and local computation cost.
* **Spatial and Spatiotemporal Processing**
  * Provide an adaptive execution engine that transforms a SQL query into a distributed plan, which can then be executed on either a single machine or a cluster.
* **Declarative Query Language**
  * Offer declarative SQL functions for data partitioning, as well as map declarative SQL queries into distributed execution strategies.

🚧 **Please note that the extension is still under development, so stay tuned for more updates and features.** 🚧

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

The `create_spatiotemporal_distributed_table ()` function is utilized to define a distributed table that is partitioned using one of the Multidimensional Tiling methods. It splits the input table into several tiles stored in separate PostgreSQL tables.

func: create_spatiotemporal_distributed_table
#### Arguments:
- `table_name_in`: Name of the input table
- `num_tiles`: Number of generated tiles
- `table_name_out`: Name of the distributed table
- `tiling_method`: Name of the tiling method: <ins>crange</ins>, <ins>hierarchical</ins>
- `tiling_granularity` (Optional): The tiling granularity. The default value depends on the granularity selection process of the tiling method that chooses between shape- and point-based strategies to create load-balanced tiles. The user can set this parameter to customize the tiling granularity.
- `tiling_type` (Optional): The tiling type of the tiling method. It can be one of the following: temporal, spatial, or spatiotemporal. The default value depends on the given column type.
- `colocation_table` (Optional): This argument allows you to colocate the input table with another table.  For example, you can use this feature to create tiles based on given boundaries such as province borders. By specifying the colocation_table and colocation_column arguments, you can ensure that your data is organized and managed in a way that suits your specific needs.
- `colocation_column` (Optional): Specify the colocation column.
- `physical_partitioning` (Optional): Determine whether or not to physically partition data.
- `object_segmentation` (Optional): Determine whether or not to segment the input spatiotemporal column.

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
SELECT create_spatiotemporal_distributed_table(table_name_in => 'planet_osm_polygon', num_tiles =>50, 
  table_name_out=>'planet_osm_polygon_50t', tiling_method => 'crange');

-- Distribute the planet_osm_roads table into 30 tiles using the spatial column: geometry(polygon)
SELECT create_spatiotemporal_distributed_table(table_name_in => 'planet_osm_roads', num_tiles =>30, 
  table_name_out=>'planet_osm_roads_30t', tiling_method => 'crange');

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
SELECT create_spatiotemporal_distributed_table(table_name_in => 'ships', num_tiles =>50, 
  table_name_out=>'ships_50t', partitioning_method => 'crange', tiling_type =>'spatiotemporal');
-- Spatial tiling
SELECT create_spatiotemporal_distributed_table(table_name_in => 'ships', num_tiles =>50, 
  table_name_out=>'ships_50t', partitioning_method => 'crange', tiling_type =>'spatial');
-- Temporal tiling
SELECT create_spatiotemporal_distributed_table(table_name_in => 'ships', num_tiles =>50, 
  table_name_out=>'ships_50t', partitioning_method => 'crange', tiling_type =>'temporal');
```

# Contributing

We are most definitely open to contributions of any kind.  Bug Reports, Feature Requests, and Documentation.

If you'd like to contribute code via a Pull Request, please make it against our `develop` branch.

Wrapping Postgres' internals to create a distributed version of MobilityDB is a complex undertaking that requires a significant amount of time and effort. However, the distributed version of MobilityDB is now available for use, and it will continue to evolve as development progresses. We welcome your feedback on how you would like to use Distributed MobilityDB and what features you would like to see added to it.

# Contact Us
We hope you find our project helpful and easy to use! If you have any questions, comments, or concerns, please don't hesitate to reach out to us. 

You can contact us by sending an email to mohamed.bakli@ulb.be