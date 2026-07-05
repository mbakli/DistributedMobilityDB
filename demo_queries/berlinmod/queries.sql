-----------------------------------------------------------------------------------------------------------------------
-- The 17 standard BerlinMOD/R benchmark queries, adapted to run against the
-- tables distributed in partitioning.sql: the spatiotemporal-tiled
-- trips_16t, and the reference tables vehicles_ref, licences_ref,
-- points_ref, regions_ref, instants_ref, periods_ref. Original queries:
-- https://github.com/MobilityDB/MobilityDB-BerlinMOD/blob/master/BerlinMOD/berlinmod_r_queries.sql
-----------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------
-- Q1) What are the models of the vehicles with licence plate numbers from Licences?
-----------------------------------------------------------------------------------------------------------------------
SELECT DISTINCT l.Licence, v.Model AS Model
FROM vehicles_ref v, licences_ref l
WHERE v.Licence = l.Licence;

-----------------------------------------------------------------------------------------------------------------------
-- Q2) How many vehicles exist that are passenger cars?
-----------------------------------------------------------------------------------------------------------------------
SELECT COUNT(Licence)
FROM vehicles_ref v
WHERE VehicleType = 'passenger';

-----------------------------------------------------------------------------------------------------------------------
-- Q3) Where have the vehicles with licences from Licences1 been at each of the instants from Instants1?
-----------------------------------------------------------------------------------------------------------------------
SELECT DISTINCT l.Licence, i.InstantId, i.Instant AS Instant,
  valueAtTimestamp(t.Trip, i.Instant) AS Location
FROM trips_16t t, Licences1 l, Instants1 i
WHERE t.VehicleId = l.VehicleId AND t.Trip::tstzspan @> i.Instant
ORDER BY l.Licence, i.InstantId;

-----------------------------------------------------------------------------------------------------------------------
-- Q4) Which vehicles have passed the points from Points?
-----------------------------------------------------------------------------------------------------------------------
SELECT DISTINCT p.PointId, p.Geom, v.Licence
FROM trips_16t t, vehicles_ref v, points_ref p
WHERE t.VehicleId = v.VehicleId
  AND ST_Intersects(trajectory(t.Trip), p.Geom)
ORDER BY p.PointId, v.Licence;

-----------------------------------------------------------------------------------------------------------------------
-- Q5) What is the minimum distance between places, where a vehicle with a licence from
-- Licences1 and a vehicle with a licence from Licences2 have been?
-----------------------------------------------------------------------------------------------------------------------
SELECT l1.Licence AS Licence1, l2.Licence AS Licence2,
  MIN(nearestapproachdistance(t1.Trip, t2.Trip)) AS MinDist
FROM trips_16t t1, Licences1 l1, trips_16t t2, Licences2 l2
WHERE t1.VehicleId = l1.VehicleId AND t2.VehicleId = l2.VehicleId
GROUP BY l1.Licence, l2.Licence
ORDER BY l1.Licence, l2.Licence;

-----------------------------------------------------------------------------------------------------------------------
-- Q6) What are the pairs of licence plate numbers of "trucks" that have ever been as close
-- as 10m or less to each other?
-----------------------------------------------------------------------------------------------------------------------
WITH Temp(Licence, VehicleId, Trip) AS (
  SELECT v.Licence, t.VehicleId, t.Trip
  FROM trips_16t t, vehicles_ref v
  WHERE t.VehicleId = v.VehicleId AND v.VehicleType = 'truck'
)
SELECT t1.Licence, t2.Licence
FROM Temp t1, Temp t2
WHERE t1.VehicleId < t2.VehicleId
  AND t1.Trip && expandSpace(t2.Trip, 10)
  AND eDwithin(t1.Trip, t2.Trip, 10.0)
ORDER BY t1.Licence, t2.Licence;

-----------------------------------------------------------------------------------------------------------------------
-- Q7) What are the licence plate numbers of the passenger cars that have reached the points
-- from Points first of all passenger cars during the complete observation period?
-----------------------------------------------------------------------------------------------------------------------
WITH Temp AS (
  SELECT DISTINCT v.Licence, p.PointId, p.Geom,
    MIN(startTimestamp(atValues(t.Trip, p.Geom))) AS Instant
  FROM trips_16t t, vehicles_ref v, points_ref p
  WHERE t.VehicleId = v.VehicleId AND v.VehicleType = 'passenger'
    AND ST_Intersects(trajectory(t.Trip), p.Geom)
  GROUP BY v.Licence, p.PointId, p.Geom
)
SELECT t1.Licence, t1.PointId, t1.Geom, t1.Instant
FROM Temp t1
WHERE t1.Instant <= ALL (
  SELECT t2.Instant
  FROM Temp t2
  WHERE t1.PointId = t2.PointId
)
ORDER BY t1.PointId, t1.Licence;

-----------------------------------------------------------------------------------------------------------------------
-- Q8) What are the overall travelled distances of the vehicles with licence plate numbers
-- from Licences1 during the periods from Periods1?
-----------------------------------------------------------------------------------------------------------------------
SELECT l.Licence, p.PeriodId, p.Period,
  SUM(length(atTime(t.Trip, p.Period))) AS Dist
FROM trips_16t t, Licences1 l, Periods1 p
WHERE t.VehicleId = l.VehicleId AND t.Trip && p.Period
GROUP BY l.Licence, p.PeriodId, p.Period
ORDER BY l.Licence, p.PeriodId;

-----------------------------------------------------------------------------------------------------------------------
-- Q9) What is the longest distance that was travelled by a vehicle during each of the periods
-- from Periods?
-----------------------------------------------------------------------------------------------------------------------
WITH Distances AS (
  SELECT p.PeriodId, p.Period, t.VehicleId,
    SUM(length(atTime(t.Trip, p.Period))) AS Dist
  FROM trips_16t t, periods_ref p
  WHERE t.Trip && p.Period
  GROUP BY p.PeriodId, p.Period, t.VehicleId
)
SELECT PeriodId, Period, MAX(Dist) AS MaxDist
FROM Distances
GROUP BY PeriodId, Period
ORDER BY PeriodId;

-----------------------------------------------------------------------------------------------------------------------
-- Q10) When and where did the vehicles with licence plate numbers from Licences1 meet other
-- vehicles (distance < 3m) and what are the latter licences?
-----------------------------------------------------------------------------------------------------------------------
WITH Temp AS (
  SELECT l1.Licence AS Licence1, t2.VehicleId AS Car2Id,
    whenTrue(tDwithin(t1.Trip, t2.Trip, 3.0)) AS Periods
  FROM trips_16t t1, Licences1 l1, trips_16t t2, vehicles_ref v
  WHERE t1.VehicleId = l1.VehicleId AND t2.VehicleId = v.VehicleId
    AND t1.VehicleId <> t2.VehicleId AND t2.Trip && expandSpace(t1.Trip, 3)
)
SELECT Licence1, Car2Id, Periods
FROM Temp
WHERE Periods IS NOT NULL;

-----------------------------------------------------------------------------------------------------------------------
-- Q11) Which vehicles passed a point from Points1 at one of the instants from Instants1?
-----------------------------------------------------------------------------------------------------------------------
WITH Temp AS (
  SELECT p.PointId, p.Geom, i.InstantId, i.Instant, t.VehicleId
  FROM trips_16t t, Points1 p, Instants1 i
  WHERE t.Trip @> stbox(p.Geom, i.Instant)
    AND valueAtTimestamp(t.Trip, i.Instant) = p.Geom
)
SELECT t.PointId, t.Geom, t.InstantId, t.Instant, v.Licence
FROM Temp t JOIN vehicles_ref v ON t.VehicleId = v.VehicleId
ORDER BY t.PointId, t.InstantId, v.Licence;

-----------------------------------------------------------------------------------------------------------------------
-- Q12) Which vehicles met at a point from Points1 at an instant from Instants1?
-----------------------------------------------------------------------------------------------------------------------
WITH Temp AS (
  SELECT DISTINCT p.PointId, p.Geom, i.InstantId, i.Instant, t.VehicleId
  FROM trips_16t t, Points1 p, Instants1 i
  WHERE t.Trip @> stbox(p.Geom, i.Instant)
    AND valueAtTimestamp(t.Trip, i.Instant) = p.Geom
)
SELECT DISTINCT t1.PointId, t1.Geom, t1.InstantId, t1.Instant,
  v1.Licence AS Licence1, v2.Licence AS Licence2
FROM Temp t1 JOIN vehicles_ref v1 ON t1.VehicleId = v1.VehicleId JOIN
  Temp t2 ON t1.VehicleId < t2.VehicleId AND t1.PointID = t2.PointID AND
  t1.InstantId = t2.InstantId JOIN vehicles_ref v2 ON t2.VehicleId = v2.VehicleId
ORDER BY t1.PointId, t1.InstantId, v1.Licence, v2.Licence;

-----------------------------------------------------------------------------------------------------------------------
-- Q13) Which vehicles travelled within one of the regions from Regions1 during the periods
-- from Periods1?
-----------------------------------------------------------------------------------------------------------------------
WITH Temp AS (
  SELECT DISTINCT r.RegionId, p.PeriodId, p.Period, t.VehicleId
  FROM trips_16t t, Regions1 r, Periods1 p
  WHERE t.Trip && stbox(r.Geom, p.Period)
    AND ST_Intersects(trajectory(atTime(t.Trip, p.Period)), r.Geom)
)
SELECT DISTINCT t.RegionId, t.PeriodId, t.Period, v.Licence
FROM Temp t, vehicles_ref v
WHERE t.VehicleId = v.VehicleId
ORDER BY t.RegionId, t.PeriodId, v.Licence;

-----------------------------------------------------------------------------------------------------------------------
-- Q14) Which vehicles travelled within one of the regions from Regions1 at one of the
-- instants from Instants1?
-----------------------------------------------------------------------------------------------------------------------
WITH Temp AS (
  SELECT DISTINCT r.RegionId, i.InstantId, i.Instant, t.VehicleId
  FROM trips_16t t, Regions1 r, Instants1 i
  WHERE t.Trip && stbox(r.Geom, i.Instant)
    AND ST_Contains(r.Geom, valueAtTimestamp(t.Trip, i.Instant))
)
SELECT DISTINCT t.RegionId, t.InstantId, t.Instant, v.Licence
FROM Temp t JOIN vehicles_ref v ON t.VehicleId = v.VehicleId
ORDER BY t.RegionId, t.InstantId, v.Licence;

-----------------------------------------------------------------------------------------------------------------------
-- Q15) Which vehicles passed a point from Points1 during a period from Periods1?
-----------------------------------------------------------------------------------------------------------------------
WITH Temp AS (
  SELECT DISTINCT pt.PointId, pt.Geom, pr.PeriodId, pr.Period, t.VehicleId
  FROM trips_16t t, Points1 pt, Periods1 pr
  WHERE t.Trip && stbox(pt.Geom, pr.Period)
    AND ST_Intersects(trajectory(atTime(t.Trip, pr.Period)), pt.Geom)
)
SELECT DISTINCT t.PointId, t.Geom, t.PeriodId, t.Period, v.Licence
FROM Temp t, vehicles_ref v
WHERE t.VehicleId = v.VehicleId
ORDER BY t.PointId, t.PeriodId, v.Licence;

-----------------------------------------------------------------------------------------------------------------------
-- Q16) List the pairs of licences for vehicles, the first from Licences1, the second from
-- Licences2, where the corresponding vehicles are both present within a region from Regions1
-- during a period from Periods1, but do not meet each other there and then.
-- NOT CURRENTLY SUPPORTED: self-join on trips_16t with no distribution-column equi-join.
-----------------------------------------------------------------------------------------------------------------------
SELECT p.PeriodId, p.Period, r.RegionId,
  l1.Licence AS Licence1, l2.Licence AS Licence2
FROM trips_16t t1, Licences1 l1, trips_16t t2, Licences2 l2, Periods1 p, Regions1 r
WHERE t1.VehicleId = l1.VehicleId AND t2.VehicleId = l2.VehicleId
  AND l1.Licence < l2.Licence
  AND ST_Intersects(trajectory(atTime(t1.Trip, p.Period)), r.Geom)
  AND ST_Intersects(trajectory(atTime(t2.Trip, p.Period)), r.Geom)
  AND aDisjoint(atTime(t1.Trip, p.Period), atTime(t2.Trip, p.Period))
ORDER BY p.PeriodId, r.RegionId, l1.Licence, l2.Licence;

-----------------------------------------------------------------------------------------------------------------------
-- Q17) Which point(s) from Points have been visited by a maximum number of different vehicles?
-----------------------------------------------------------------------------------------------------------------------
WITH PointCount AS (
  SELECT p.PointId, COUNT(DISTINCT t.VehicleId) AS Hits
  FROM trips_16t t, points_ref p
  WHERE ST_Intersects(trajectory(t.Trip), p.Geom)
  GROUP BY p.PointId
)
SELECT PointId, Hits
FROM PointCount AS p
WHERE p.Hits = (SELECT MAX(Hits) FROM PointCount);
