# Distributed MobilityDB
Distributed MobilityDB is an open-source extension for Postgres tailored to handle spatial and spatiotemporal data at scale on a distributed database cluster. It provides data and query distribution to PostGIS and MobilityDB.

# Key Features

* **Spatiotemporal Data Partitioning**
  * Transform the input relation into a multirelation, preserving spatiotemporal data locality and load balancing.
  * Develop a two-level (global and local) distributed indexing scheme, effectively reducing the global transmission cost and local computation cost.
* **Spatiotemporal Processing**
  * Handle a wide array of MobilityDB types including tint, tfloat, and tgeompoint, alongside PostGIS types, such as point, linestring, and polygon.
  * Provide an adaptive execution engine that transforms a SQL query into a distributed query plan, which can then be executed on either a single machine or a cluster.
  * Support spatial-only, temporal-only, and spatiotemporal queries, where PostGIS and MobilityDB predicates can co-exist in a single query.
  * Facilitate multiple types of queries including range, kNN, intersection, and distance joins.
  * Offer an execution framework that readily enables distributed processing for both PostGIS and MobilityDB functionalities.

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

The `create_spatiotemporal_distributed_table ()` function is utilized to define a distributed table that is partitioned using a Multidimensional Tiling method. It splits the input table into several tiles stored in separate PostgreSQL tables.

func: create_spatiotemporal_distributed_table
#### Arguments:
- `table_name_in`: Name of the input table
- `num_tiles`: Number of generated tiles
- `table_name_out`: Name of the distributed table
- `tiling_method`: Name of the tiling method: <ins>crange</ins>, <ins>hierarchical</ins>, <ins>grid</ins>
- `tiling_granularity` (Optional): The tiling granularity. The default value depends on the granularity selection process of the tiling method that chooses between shape- and point-based strategies to create load-balanced tiles. The user can set this parameter to customize the tiling granularity.
- `tiling_type` (Optional): The tiling type of the tiling method. It can be one of the following: temporal, spatial, or spatiotemporal. The default value depends on the given column type.
- `colocation_table` (Optional): This argument allows you to colocate the input table with another table.  For example, you can use this feature to create tiles based on given boundaries such as province borders. By specifying the colocation_table and colocation_column arguments, you can ensure that your data is organized and managed in a way that suits your specific needs.
- `colocation_column` (Optional): Specify the colocation column.
- `physical_partitioning` (Optional): Determine whether or not to physically partition data.
- `object_segmentation` (Optional): Determine whether or not to segment the input spatiotemporal column.

By utilizing the create_spatiotemporal_distributed_table() function with these arguments, you can easily create a distributed table that suits your data management needs. 

-----------------------------------------------------------------------------------------------------------------------
# Use Cases
Below are examples of well-known datasets, where Distributed MobilityDB showcases its proficiency in managing large spatiotemporal data, offering users diverse query types suitable for a wide range of applications. 

Distributed MobilityDB seamlessly converts PostGIS and MobilityDB tables into distributed tables, allowing users to execute their PostGIS and MobilityDB SQL queries in a distributed manner without any need for modification.

### OpenStreatMap (OSM) Data
Description: OSM data refers to geographic data collected by the OpenStreetMap community. It includes information such as roads, buildings, parks, and other features. 

Download: https://download.geofabrik.de/ 
```sql
-- Input tables
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

-- Distance-Join Query: Find buildings that are built within 1km of the primary highways.
SELECT distinct t1.name
FROM planet_osm_polygon_50t t1, planet_osm_roads_30t t2
WHERE t1.building = 'yes'
  AND t2.highway = 'primary'
  AND ST_dwithin(t1.way,t2.way, 1000);

-- Intersection-Join Query: Find health centers POIs in Berlin.
SELECT t2.name
FROM planet_osm_polygon_30t t1, planet_osm_point_12t t2
WHERE t2.amenity IN ('hospital', 'clinic', 'doctors')
  AND t1.name = 'Berlin'
  AND st_intersects(t1.way, t2.way);
```
### Automatic Identification System (AIS) Data
Description: AIS is a tracking system used on ships and vessels to provide information about their identification, course, speed, and dynamic data such as longitude, latitude, and time..

Download: https://web.ais.dk/aisdata/

```sql
-- Input tables
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

-- Distance-Join Query: Find fishing ships that were within 1km of tanker ships.
SELECT t1.mmsi Ship1ID, t2.mmsi Ship2ID
FROM ships_tanker_30t t1, ships_fishing_15t t2
WHERE edwithin(t1.trip, t2.trip, 1000);
```
### Global Surface Summary of the Day (GSOD) Data
Description: GSOD data is a collection of daily weather observations from weather stations around the world. It includes information such as temperature, time, location, humidity, and atmospheric pressure. It provides valuable insights into weather patterns, trends, and extremes on a global scale.

Download: https://www.ncei.noaa.gov/

```sql
-- Input tables
CREATE TABLE gsod_temp (
  loc geometry,
  temperature_tfloat tfloat(sequence),
  ...
);

-- Distribute the GSOM table into 32 tiles using the temporal float column: tfloat(sequence)
-- Temporal tiling
SELECT create_spatiotemporal_distributed_table(table_name_in => 'gsod_temp', num_tiles =>32, 
  table_name_out=>'gsod_temp_32t', partitioning_method => 'crange', tiling_type =>'temporal');

-- Temporal Query: Identify the hottest areas observed within the past 24 hours
SELECT station, loc
FROM gsod_temp_32t
WHERE temperature_tfloat && tstzspan '[2024-01-01, 2024-01-01]' 
	AND temperature_tfloat ?> 95 -- Fahrenheit
```

# Contributing

We are most definitely open to contributions of any kind.  Bug Reports, Feature Requests, and Documentation.

If you'd like to contribute code via a Pull Request, please make it against our `develop` branch.

Wrapping Postgres' internals to create a distributed version of MobilityDB is a complex undertaking that requires a significant amount of time and effort. However, the distributed version of MobilityDB is now available for use, and it will continue to evolve as development progresses. We welcome your feedback on how you would like to use Distributed MobilityDB and what features you would like to see added to it.

# Contact Us
We hope you find our project helpful and easy to use! If you have any questions, comments, or concerns, please don't hesitate to reach out to us. 

1. [ ] You can contact us by sending an email to mohamed.bakli@ulb.be]()