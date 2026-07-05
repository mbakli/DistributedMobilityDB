--------------------------------------------------------------------------------------------------------------------------------------------------------
-- Catalog Cleanup
--------------------------------------------------------------------------------------------------------------------------------------------------------
-- Dropping a distributed spatiotemporal table (e.g. DROP TABLE ... CASCADE)
-- only removes the table itself and its Citus shards; Postgres has no
-- dependency link between it and the rows this extension keeps in
-- pg_dist_spatiotemporal_tables/pg_dist_spatiotemporal_tiles, so those rows
-- are silently orphaned otherwise. This event trigger removes them
-- automatically whenever a registered table is dropped.
CREATE OR REPLACE FUNCTION distributed_mobilitydb_drop_cleanup()
    RETURNS event_trigger AS $$
DECLARE
    obj record;
    dropped_table_id integer;
BEGIN
    IF to_regclass('pg_dist_spatiotemporal_tables') IS NULL THEN
        -- The extension itself (and its catalog tables) is being dropped;
        -- nothing left to clean up.
        RETURN;
    END IF;
    FOR obj IN SELECT * FROM pg_event_trigger_dropped_objects()
               WHERE object_type = 'table'
    LOOP
        SELECT id INTO dropped_table_id
        FROM pg_dist_spatiotemporal_tables
        WHERE tableName = obj.object_name;

        IF dropped_table_id IS NOT NULL THEN
            DELETE FROM pg_dist_spatiotemporal_tiles WHERE table_id = dropped_table_id;
            DELETE FROM pg_dist_spatiotemporal_tables WHERE id = dropped_table_id;
        END IF;
    END LOOP;
END;
$$ LANGUAGE 'plpgsql';

DROP EVENT TRIGGER IF EXISTS distributed_mobilitydb_drop_cleanup_trigger;
CREATE EVENT TRIGGER distributed_mobilitydb_drop_cleanup_trigger
    ON sql_drop
    EXECUTE FUNCTION distributed_mobilitydb_drop_cleanup();
