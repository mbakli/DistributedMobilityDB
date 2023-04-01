-- Q1) Find buildings that are built within 1km of the primary highways.
select distinct t1.name
from planet_osm_polygon_30t t1, planet_osm_roads_30t t2
where t1.building = 'yes'
  AND t2.highway = 'primary'
  AND ST_dwithin(t1.way,t2.way, 1000);
-------------------------------------------------------------------------------------------------
--Q2) Parks that have lakes in them.
SELECT distinct t1.name
FROM planet_osm_polygon_30t t1, planet_osm_polygon_30t t2
WHERE t1.leisure='park'
  AND t2.water ='lake'
  AND st_intersects(t1.way, t2.way)
-------------------------------------------------------------------------------------------------
--Q3) Find services that are within a given region
SELECT osm_id, name, way
FROM planet_osm_point_30t
WHERE st_intersects(way,
                    ST_SetSRID(ST_MakeBox2D(ST_Point(1456987.900523874, 6861517.042867468),
                                            ST_Point(1531885.4238990662, 6923199.922124346)),3857));
-------------------------------------------------------------------------------------------------
--Q4) Find services that are within 1km of the highways
select distinct t2.name
from planet_osm_roads_30t t1, planet_osm_point_30t t2
where t1.highway is not null
  AND t2.amenity is not null
  AND ST_dwithin(t1.way,t2.way, 1000);
-------------------------------------------------------------------------------------------------
--Q5): Restaurants within 50m to Schools
SELECT distinct t2.name
FROM planet_osm_point_30t t1, planet_osm_point_30t t2
WHERE t1.amenity = 'school'
  AND t2.amenity='restaurant'
  AND st_dwithin(t1.way, t2.way, 200);
------------------------------------------------------------------------------------------------------------------------
--Q6): Three nearest routes (tram, bus, subway) to a POI
SELECT osm_id
FROM planet_osm_line_30t
WHERE route IN ('tram', 'bus', 'subway')
ORDER BY ST_Distance(way, st_setsrid('POINT(483404.4546237764 6588847.794027591)'::geometry,3857))
LIMIT 3;
------------------------------------------------------------------------------------------------------------------------
--Q7): Health centers POIs in Berlin?
SELECT  t2.name
FROM planet_osm_polygon_30t t1, planet_osm_point_sample_12t t2
WHERE t2.amenity IN ('hospital', 'clinic', 'doctors')
  AND t1.name = 'Berlin'
  AND st_intersects(t1.way, t2.way);
------------------------------------------------------------------------------------------------------------------------
