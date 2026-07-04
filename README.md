# Distributed MobilityDB

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](License)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-%3E%3D13-336791?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Citus](https://img.shields.io/badge/Citus-%3E%3D10-blue)](https://www.citusdata.com/)
[![PostGIS](https://img.shields.io/badge/PostGIS-%3E%3D3-3f9c35)](https://postgis.net/)
[![MobilityDB](https://img.shields.io/badge/MobilityDB-%3E%3D1.1-orange)](https://mobilitydb.com/)
[![Status](https://img.shields.io/badge/status-under%20development-red)]()

Distributed MobilityDB is an open-source extension for PostgreSQL tailored to handle spatial and spatiotemporal data at scale on a distributed database cluster. It provides data and query distribution for PostGIS and MobilityDB.

## Table of Contents

- [Key Features](#key-features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Using Distributed MobilityDB](#using-distributed-mobilitydb)
  - [Creating Distributed Tables](#creating-distributed-tables)
- [Use Cases](#use-cases)
  - [OpenStreetMap (OSM) Data](#openstreetmap-osm-data)
  - [Automatic Identification System (AIS) Data](#automatic-identification-system-ais-data)
  - [Global Surface Summary of the Day (GSOD) Data](#global-surface-summary-of-the-day-gsod-data)
- [Contributing](#contributing)
- [Contact Us](#contact-us)

## Key Features

**Spatiotemporal Data Partitioning**
- Transform the input relation into a multirelation, preserving spatiotemporal data locality and load balancing.
- Develop a two-level (global and local) distributed indexing scheme, effectively reducing the global transmission cost and local computation cost.

**Spatiotemporal Processing**
- Handle a wide array of MobilityDB types including `tint`, `tfloat`, and `tgeompoint`, alongside PostGIS types, such as `point`, `linestring`, and `polygon`.
- Provide an adaptive execution engine that transforms a SQL query into a distributed query plan, which can then be executed on either a single machine or a cluster.
- Support spatial-only, temporal-only, and spatiotemporal queries, where PostGIS and MobilityDB predicates can co-exist in a single query.
- Facilitate multiple types of queries including range, kNN, intersection, and distance joins.
- Offer an execution framework that readily enables distributed processing for both PostGIS and MobilityDB functionalities.

> 🚧 **Please note that the extension is still under development, so stay tuned for more updates and features.** 🚧

## Prerequisites

| Dependency | Minimum Version |
|---|---|
| [PostgreSQL](https://www.postgresql.org/) | 13 |
| [Citus](https://www.citusdata.com/) | 10 |
| [PostGIS](https://postgis.net/) | 3 |
| [MobilityDB](https://mobilitydb.com/) | 1.1 |

## Installation

### 1. Build from source (Linux)

```bash
git clone https://github.com/mbakli/DistributedMobilityDB
cd DistributedMobilityDB
mkdir build && cd build
cmake ..
make
sudo make install
```

### 2. Create the extension in PostgreSQL

```sql
CREATE EXTENSION Distributed_MobilityDB CASCADE;
```

## Using Distributed MobilityDB

### Creating Distributed Tables

The `create_spatiotemporal_distributed_table()` function is utilized to define a distributed table that is partitioned using a Multidimensional Tiling method. It splits the input table into several tiles stored in separate PostgreSQL tables.

**Function:** `create_spatiotemporal_distributed_table`

| Argument | Required | Description |
|---|---|---|
| `table_name_in` | Yes | Name of the input table. |
| `num_tiles` | Yes | Number of generated tiles. |
| `table_name_out` | Yes | Name of the distributed table. |
| `tiling_method` | Yes | Name of the tiling method: <ins>crange</ins>, <ins>hierarchical</ins>, <ins>grid</ins>. |
| `tiling_granularity` | No | The tiling granularity. Defaults to the value chosen by the tiling method's granularity selection process, which picks between shape- and point-based strategies to create load-balanced tiles. Set this to customize the tiling granularity. |
| `tiling_type` | No | The tiling type of the tiling method: `temporal`, `spatial`, or `spatiotemporal`. Defaults based on the given column type. |
| `colocation_table` | No | Colocate the input table with another table, e.g. to create tiles based on given boundaries such as province borders. Used together with `colocation_column`. |
| `colocation_column` | No | The colocation column to use with `colocation_table`. |
| `physical_partitioning` | No | Whether or not to physically partition data. |
| `object_segmentation` | No | Whether or not to segment the input spatiotemporal column. |

By utilizing the `create_spatiotemporal_distributed_table()` function with these arguments, you can easily create a distributed table that suits your data management needs.

## Use Cases

Below are examples of well-known datasets, where Distributed MobilityDB showcases its proficiency in managing large spatiotemporal data, offering users diverse query types suitable for a wide range of applications.

Distributed MobilityDB seamlessly converts PostGIS and MobilityDB tables into distributed tables, allowing users to execute their PostGIS and MobilityDB SQL queries in a distributed manner without any need for modification.

### OpenStreetMap (OSM) Data

**Description:** OSM data refers to geographic data collected by the OpenStreetMap community. It includes information such as roads, buildings, parks, and other features.

**Download:** https://download.geofabrik.de/

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

CREATE TABLE planet_osm_point (
  osm_id bigint,
  way geometry(point),
  ...
);

-- Distribute the planet_osm_polygon table into 50 tiles using the spatial column: geometry(polygon)
SELECT create_spatiotemporal_distributed_table(table_name_in => 'planet_osm_polygon', num_tiles => 50,
  table_name_out => 'planet_osm_polygon_50t', tiling_method => 'crange');

-- Distribute the planet_osm_roads table into 30 tiles using the spatial column: geometry(linestring)
SELECT create_spatiotemporal_distributed_table(table_name_in => 'planet_osm_roads', num_tiles => 30,
  table_name_out => 'planet_osm_roads_30t', tiling_method => 'crange');

-- Distribute the planet_osm_point table into 12 tiles using the spatial column: geometry(point)
SELECT create_spatiotemporal_distributed_table(table_name_in => 'planet_osm_point', num_tiles => 12,
  table_name_out => 'planet_osm_point_12t', tiling_method => 'crange');

-- Distance-Join Query: Find buildings that are built within 1km of the primary highways.
SELECT DISTINCT t1.name
FROM planet_osm_polygon_50t t1, planet_osm_roads_30t t2
WHERE t1.building = 'yes'
  AND t2.highway = 'primary'
  AND ST_DWithin(t1.way, t2.way, 1000);

-- Intersection-Join Query: Find health centers POIs in Berlin.
SELECT t2.name
FROM planet_osm_polygon_50t t1, planet_osm_point_12t t2
WHERE t2.amenity IN ('hospital', 'clinic', 'doctors')
  AND t1.name = 'Berlin'
  AND ST_Intersects(t1.way, t2.way);
```

### Automatic Identification System (AIS) Data

**Description:** AIS is a tracking system used on ships and vessels to provide information about their identification, course, speed, and dynamic data such as longitude, latitude, and time.

**Download:** https://web.ais.dk/aisdata/

```sql
-- Input tables
CREATE TABLE ships_tanker (
  mmsi int,
  trip tgeompoint(sequence),
  ...
);

CREATE TABLE ships_fishing (
  mmsi int,
  trip tgeompoint(sequence),
  ...
);

-- Distribute the ships_tanker table into 50 tiles using the spatiotemporal column: tgeompoint(sequence)
SELECT create_spatiotemporal_distributed_table(table_name_in => 'ships_tanker', num_tiles => 50,
  table_name_out => 'ships_tanker_50t', partitioning_method => 'crange', tiling_type => 'spatiotemporal');

-- Distribute the ships_fishing table into 15 tiles using the spatiotemporal column: tgeompoint(sequence)
SELECT create_spatiotemporal_distributed_table(table_name_in => 'ships_fishing', num_tiles => 15,
  table_name_out => 'ships_fishing_15t', partitioning_method => 'crange', tiling_type => 'spatiotemporal');

-- Distance-Join Query: Find fishing ships that were within 1km of tanker ships.
SELECT t1.mmsi AS Ship1ID, t2.mmsi AS Ship2ID
FROM ships_tanker_50t t1, ships_fishing_15t t2
WHERE eDWithin(t1.trip, t2.trip, 1000);

-- Temporal Query: What is the total travelled distance of ships that spent more than 5 days to reach the port of Kalundborg in Sept 19?
SELECT mmsi AS ShipID, length(Trip) / 1000 AS travelledKms
FROM ships_tanker_50t
WHERE Destination = 'Kalundborg'
  AND Trip && Period('2019-09-01', '2019-09-30')
  AND timespan(Trip) > '5 days';
```

### Global Surface Summary of the Day (GSOD) Data

**Description:** GSOD data is a collection of daily weather observations from weather stations around the world. It includes information such as temperature, time, location, humidity, and atmospheric pressure.

**Download:** https://www.ncei.noaa.gov/

```sql
-- Input tables
CREATE TABLE gsod_temp (
  loc geometry,
  temperature_tfloat tfloat(sequence),
  ...
);

-- Distribute the GSOD table into 32 tiles using the temporal float column: tfloat(sequence)
SELECT create_spatiotemporal_distributed_table(table_name_in => 'gsod_temp', num_tiles => 32,
  table_name_out => 'gsod_temp_32t', partitioning_method => 'crange', tiling_type => 'temporal');

-- Temporal Query: Identify the hottest areas observed within the past 24 hours
SELECT station, loc
FROM gsod_temp_32t
WHERE temperature_tfloat && tstzspan '[2024-01-01, 2024-01-01]'
  AND temperature_tfloat ?> 95; -- Fahrenheit

-- Aggregate Query: Retrieve the maximum temperature for each location
SELECT loc, xmax(extent(temperature_tfloat))
FROM gsod_temp_32t
GROUP BY loc;
```

## Contributing

We are most definitely open to contributions of any kind: bug reports, feature requests, and documentation.

If you'd like to contribute code via a Pull Request, please make it against our `develop` branch.

Wrapping Postgres' internals to create a distributed version of MobilityDB is a complex undertaking that requires a significant amount of time and effort. However, the distributed version of MobilityDB is now available for use, and it will continue to evolve as development progresses. We welcome your feedback on how you would like to use Distributed MobilityDB and what features you would like to see added to it.

Please also see our [Code of Conduct](CODE_OF_CONDUCT.md).

## Contact Us

We hope you find our project helpful and easy to use! If you have any questions, comments, or concerns, please don't hesitate to reach out to us at [mohamed.bakli@ulb.be](mailto:mohamed.bakli@ulb.be).

## License

Distributed MobilityDB is released under the [MIT License](License).
