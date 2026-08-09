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
-- register_colocation makes Citus' own query planner treat table1 and table2
-- as colocated at the metadata level (pg_dist_partition.colocationid), not
-- just physically (colocate_shards() above only moves shards onto matching
-- nodes -- Citus' push-down planner doesn't trust that alone, it separately
-- checks colocationid before allowing a join to push down).
--
-- Neither create_distributed_table(..., colocate_with => ...) nor the
-- standalone update_distributed_table_colocation() can be used for this:
-- both explicitly reject range-distributed tables ("relation ... should be
-- a hash or single shard distributed table"), which is exactly what every
-- table this whole tiling architecture creates. Writing pg_dist_colocation/
-- pg_dist_partition directly is this project's established workaround for
-- that same Citus limitation (create_range_shard above already does the
-- equivalent for shard ranges via a raw pg_dist_shard UPDATE).
--
-- Without this, every join against a freshly reshuffled table fails with
-- "cannot push down this subquery ... not colocated" even though the
-- shards are genuinely co-resident -- and since that error aborts the
-- whole top-level statement, it silently rolls back the reshuffled data
-- ReshuffleData() just inserted moments earlier in the very same
-- transaction (confirmed directly: SPI_processed reported the real insert
-- count, but the table read back empty once the same statement's later
-- push-down error unwound everything).
----------------------------------------------------------------------------------------------------------------------
DROP FUNCTION IF EXISTS register_colocation;
CREATE OR REPLACE FUNCTION register_colocation(table1 text, table2 text)
    RETURNS boolean
LANGUAGE plpgsql
AS $$
DECLARE
    group_id integer;
    tbl_shardcount integer;
    tbl_disttype oid;
    tbl_distcollation oid;
BEGIN
    SELECT colocationid INTO group_id FROM pg_dist_partition WHERE logicalrelid = table1::regclass;

    IF group_id IS NULL OR group_id = 0 THEN
        SELECT count(*) INTO tbl_shardcount FROM pg_dist_shard WHERE logicalrelid = table1::regclass;
        SELECT a.atttypid, a.attcollation INTO tbl_disttype, tbl_distcollation
        FROM pg_dist_partition p
        JOIN pg_attribute a ON a.attrelid = p.logicalrelid
        WHERE p.logicalrelid = table1::regclass
          AND a.attname = column_to_column_name(p.logicalrelid, p.partkey);

        SELECT colocationid INTO group_id FROM pg_dist_colocation
        WHERE shardcount = tbl_shardcount AND replicationfactor = 1
          AND distributioncolumntype = tbl_disttype AND distributioncolumncollation = tbl_distcollation
        LIMIT 1;

        IF group_id IS NULL THEN
            INSERT INTO pg_dist_colocation (colocationid, shardcount, replicationfactor,
                                            distributioncolumntype, distributioncolumncollation)
            VALUES (nextval('pg_dist_colocationid_seq'), tbl_shardcount, 1, tbl_disttype, tbl_distcollation)
            RETURNING colocationid INTO group_id;
        END IF;

        UPDATE pg_dist_partition SET colocationid = group_id WHERE logicalrelid = table1::regclass;
    END IF;

    UPDATE pg_dist_partition SET colocationid = group_id WHERE logicalrelid = table2::regclass;
    RETURN true;
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
    /* colocate_shards() only achieves physical colocation -- see
     * register_colocation's own comment above for why Citus' planner still
     * needs the metadata updated separately before it will push down a
     * join against reshuffled_table. */
    PERFORM register_colocation(tableName, reshuffled_table);
RETURN true;
END;
$$;
