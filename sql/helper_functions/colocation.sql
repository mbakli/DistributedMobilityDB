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
-- colocate_multirelation colocates one multirelation with another
----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION colocate_multirelation(table1 text, table2 text)
    RETURNS boolean
LANGUAGE plpgsql
AS $$
DECLARE
i integer;
    shard_info record;
    node_info record;
    node text;
    shardid_test bigint;
BEGIN
    --Move one of them to a new place because one node contains 10 and the other contains 11
    --It is an enterpise feature
    --SELECT master_move_shard_placement(102660,'pgxl4', 5432,'pgxl2', 5432);
    --For every node, update shards information
    FOR node_info in SELECT * FROM master_get_active_worker_nodes()
    LOOP
        i = 0;
        --Get the shards of every table in every node
        FOR shard_info in SELECT shard.shardid, shard.shardminvalue, shard.shardmaxvalue
                          FROM pg_dist_placement AS placement, pg_dist_node AS node, pg_dist_shard As shard
                          WHERE placement.groupid = node.groupid
                            AND shard.logicalrelid = table1::regclass
                            AND placement.shardid = shard.shardid
                            AND node.noderole = 'primary'
                            AND nodename=node_info.node_name
        LOOP
                SELECT shard.shardid
                FROM pg_dist_placement AS placement, pg_dist_node AS node, pg_dist_shard As shard
                WHERE placement.groupid = node.groupid
                    AND shard.logicalrelid = table2::regclass
                    AND placement.shardid = shard.shardid
                    AND node.noderole = 'primary'
                    AND nodename=node_info.node_name offset i limit 1 INTO shardid_test;
                --RAISE NOTICE 'Update:%',shardid_test;
                UPDATE pg_dist_shard SET shardminvalue = shard_info.shardminvalue, shardmaxvalue=shard_info.shardmaxvalue
                WHERE shardid = shardid_test;
                i := i + 1;
        END LOOP;
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
    --Modify the ranges
    UPDATE pg_catalog.pg_dist_partition
    SET colocationid=2
    WHERE logicalrelid=reshuffled_table::regclass;
    PERFORM colocate_multirelation(tableName, reshuffled_table);
RETURN true;
END;
$$;
