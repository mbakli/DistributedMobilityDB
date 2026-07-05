----------------------------------------------------------------------------------------------------------------------
-- create_range_shard changes min and max of one shard
----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION create_range_shard(table_name regclass, start_value text, end_value text)
    RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
    new_shard_id bigint := master_create_empty_shard(table_name::text);
BEGIN
    UPDATE pg_dist_shard
    SET shardminvalue = start_value::text, shardmaxvalue = end_value::text
    WHERE shardid = new_shard_id;
    RETURN new_shard_id;
END;
$$;

----------------------------------------------------------------------------------------------------------------------
-- create_range_shards changes min and max of each shard
----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION create_range_shards(shards integer, tablename text)
    RETURNS boolean
LANGUAGE plpgsql
AS $$
DECLARE
    i integer;
BEGIN
    FOR i in 1..shards
    LOOP
        EXECUTE format('%s',concat('SELECT create_range_shard(''',tablename,''', ''',i,''', ''',i,''')'));
    END LOOP;
    RETURN TRUE;
END;
$$;

----------------------------------------------------------------------------------------------------------------------
-- colocate_shards physically moves table2's shards so each one lands on the
-- same node as table1's shard sharing the same tile_key range (shardminvalue).
-- Citus' own colocate_with option doesn't support range-distributed tables,
-- so this does the move explicitly with citus_move_shard_placement() instead
-- of relying on Citus' colocation groups.
----------------------------------------------------------------------------------------------------------------------
DROP FUNCTION IF EXISTS colocate_multirelation;
CREATE OR REPLACE FUNCTION colocate_shards(table1 text, table2 text)
    RETURNS boolean
LANGUAGE plpgsql
AS $$
DECLARE
    tile_pair record;
BEGIN
    FOR tile_pair IN
        SELECT s2.shardid AS moving_shard,
               n1.nodename AS target_node, n1.nodeport AS target_port,
               n2.nodename AS source_node, n2.nodeport AS source_port
        FROM pg_dist_shard s1
        JOIN pg_dist_placement p1 ON p1.shardid = s1.shardid
        JOIN pg_dist_node n1 ON n1.groupid = p1.groupid AND n1.noderole = 'primary'
        JOIN pg_dist_shard s2 ON s2.shardminvalue = s1.shardminvalue
        JOIN pg_dist_placement p2 ON p2.shardid = s2.shardid
        JOIN pg_dist_node n2 ON n2.groupid = p2.groupid AND n2.noderole = 'primary'
        WHERE s1.logicalrelid = table1::regclass
          AND s2.logicalrelid = table2::regclass
          AND (n1.nodename, n1.nodeport) IS DISTINCT FROM (n2.nodename, n2.nodeport)
    LOOP
        PERFORM citus_move_shard_placement(tile_pair.moving_shard,
                                           tile_pair.source_node, tile_pair.source_port,
                                           tile_pair.target_node, tile_pair.target_port,
                                           'block_writes');
    END LOOP;
    RETURN TRUE;
END;
$$;

----------------------------------------------------------------------------------------------------------------------
-- create_reshuffled_multirelation creates a multirelation using the same meta data of the given multirelation
----------------------------------------------------------------------------------------------------------------------
DROP FUNCTION IF EXISTS create_reshuffled_multirelation;
CREATE OR REPLACE FUNCTION create_reshuffled_multirelation(tableName text, shards integer, reshuffled_table text)
    RETURNS boolean
LANGUAGE plpgsql
AS $$
DECLARE
i integer;
    node_info record;
    j integer;
    repartitioning_numrows integer;
    selfjoin_numrows integer;
    startTime_joinPairs timestamptz;
    endTime_joinPairs timestamptz;
    startTime_selfJoin timestamptz;
    endTime_selfJoin timestamptz;
    results json;
BEGIN
    PERFORM create_range_shards(shards, reshuffled_table);
    /* create_range_shards() above already assigns reshuffled_table's shards
     * the correct tile_key-aligned ranges (1..shards), matching tableName's
     * own tile numbering by convention -- both tables tile the same way.
     * Citus' shard placement for the newly-created shards is otherwise
     * independent of tableName's placement, so without this, a tile-key
     * join between the two tables is a cross-node repartition even though
     * both sides cover the same tile_key range. colocate_shards() moves
     * each of reshuffled_table's shards onto tableName's matching-tile node. */
    PERFORM colocate_shards(tableName, reshuffled_table);
RETURN true;
END;
$$;
