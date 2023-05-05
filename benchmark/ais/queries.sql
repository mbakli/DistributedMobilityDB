-------------------------------------------------------------------------------------------------
--Q1) Find fishing ships that were within 1km of tanker ships.
SELECT t1.mmsi Ship1ID, t2.mmsi Ship2ID
FROM ships_tanker_30t t1, ships_fishing_15t t2
WHERE edwithin(t1.trip, t2.trip, 1000);

-- To add a specific period, the query will be as follows:
SELECT t1.mmsi Ship1ID, t2.mmsi Ship2ID
FROM ships_tanker_30t t1, ships_fishing_15t t2
WHERE t1.trip && period('2019-01-02 00:30', '2019-01-02 01:00')
  AND t2.trip && period('2019-01-02 00:30', '2019-01-02 01:00')
  AND edwithin(t1.trip, t2.trip, 1000);

-------------------------------------------------------------------------------------------------
--Q2) List the departure time of all ships in the port of Kalundborg between 2019-01-02 00:30 and 01:00 AM.
SELECT T.ship_id, T.Trip, startTimestamp(atgeometry(T.trip, P.port_geom)) As DepartTime
FROM ships_tanker_30t T, ports P
WHERE P.port_name='Kalundborg'
  AND T.trip && STBOX(P.port_geom, period('2019-01-02 00:30', '2019-01-02 01:00') )
	and intersects(T.Trip, P.port_geom);
-------------------------------------------------------------------------------
--Q3) What are the ships that have passed a specific area in the sea?
SELECT Ship_id
FROM ships_tanker_30t
WHERE intersects(Trip, ST_MakeEnvelope(640730, 6058230, 654100, 6042487, 25832));
-------------------------------------------------------------------------------
--Q4): How many one-way trips that ships did on September 2, 2019 between the ports of Rødby and Puttgarden?
WITH TEMP
         AS
         (SELECT
              (SELECT Port_geom AS Port_geom_Rodby from Ports where port_name='Rodby'),
              (SELECT Port_geom AS Port_geom_Puttgarden from Ports where port_name='Puttgarden')
         )
SELECT Ship_id, (numSequences(atGeometry(S.Trip, P.Port_geom_Rodby)) +numSequences(atGeometry(S.Trip, P.Port_geom_Puttgarden)))/2.0 AS NumTrips
FROM ships_tanker_30t S, TEMP P
WHERE intersects(S.Trip, P.port_geom_Rodby)
  AND intersects(S.Trip, P.port_geom_Puttgarden);
-------------------------------------------------------------------------------
--Q5): What is the trajectory and speed of all ships that spent more than 5 days to reach to the port of Kalundborg in Sept 19?
SELECT Ship_id, trajectory(Trip) AS Traj, speed(Trip) AS TripSpeed
FROM ships_tanker_30t
WHERE Destination='Kalundborg'
  AND Trip && Period('2019-09-01', '2019-09-30')
	AND timespan(Trip) > '5 days';
-------------------------------------------------------------------------------
--Q6): List the minimum distance ever between each ship and a specific point
SELECT Ship_id, MIN(trajectory(Trip) <-> ST_transform(ST_setsrid(ST_MakePoint(10.945873, 56.447255), 4326), 25832)) AS MinDistance
FROM ships_tanker_30t
GROUP BY mmsi
ORDER BY mmsi;

-------------------------------------------------------------------------------
--Q7): What is the total travelled distance of all trips between the ports of Rødby and Puttgarden in Sept 2, 19?
SELECT Ship_id, length(Trip)
FROM ships_tanker_30t
WHERE Destination='Puttgarden'
  AND Trip && Period('2019-09-02 00:00:00', '2019-09-02 23:59:59');
------------------------------------------------------------------------------------------------------------------------
