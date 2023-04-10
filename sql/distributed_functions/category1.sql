-- Sample of the functions that fall in this category
INSERT INTO pg_dist_spatiotemporal_dist_functions (worker, combiner, final, sExec_id)
SELECT 'intersects', NULL, 'bool_or', 2
UNION ALL
SELECT 'speed', NULL, 'merge', 2
UNION ALL
SELECT 'extent', NULL, 'merge', 2
UNION ALL
SELECT 'numinstants', NULL, 'sum', 2
UNION ALL
SELECT 'starttimestamp', NULL, 'min', 2
UNION ALL
SELECT 'endtimestamp', NULL, 'max', 2
UNION ALL
SELECT 'length', NULL, 'sum', 2
UNION ALL
SELECT 'count', NULL, 'sum', 2
UNION ALL
SELECT 'minvalue', NULL, 'min', 2;

