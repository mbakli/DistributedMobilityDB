-----------------------------------------------------------------------------------------------------------------------
-- Points
-- OSM table: planet_osm_point
-----------------------------------------------------------------------------------------------------------------------
select create_spatiotemporal_distributed_table(table_name_in => 'planet_osm_point', num_tiles =>50, table_name_out=>'planet_osm_point_50t', tiling_method => 'crange');
-----------------------------------------------------------------------------------------------------------------------
-- lines
-- OSM table: planet_osm_line
-----------------------------------------------------------------------------------------------------------------------
select create_spatiotemporal_distributed_table(table_name_in => 'planet_osm_line', num_tiles =>50, table_name_out=>'planet_osm_line_50t', tiling_method => 'crange');
-----------------------------------------------------------------------------------------------------------------------
-- roads
-- OSM table: planet_osm_roads
-----------------------------------------------------------------------------------------------------------------------
select create_spatiotemporal_distributed_table(table_name_in => 'planet_osm_roads', num_tiles =>50, table_name_out=>'planet_osm_roads_50t', tiling_method => 'crange');
-----------------------------------------------------------------------------------------------------------------------
-- polygons
-- OSM table: planet_osm_polygon
-----------------------------------------------------------------------------------------------------------------------
select create_spatiotemporal_distributed_table(table_name_in => 'planet_osm_polygon', num_tiles =>50, table_name_out=>'planet_osm_polygon_50t', tiling_method => 'crange');
