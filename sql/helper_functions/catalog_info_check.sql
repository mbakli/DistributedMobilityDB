----------------------------------------------------------------------------------------------------------------------
-- IsMobilityDBType returns true of the input column is of type MobilityDB and returns false if it is of type PostGIS
----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION IsMobilityDBType(table_name_in varchar(250), tiling tiling)
    RETURNS boolean AS $$
DECLARE
    column_type varchar(30);
BEGIN
    SELECT getDistColType(table_name_in, tiling.distCol)
    INTO column_type;
    tiling.distColType := column_type;
    IF column_type = 'geometry' THEN
        RETURN false;
    ELSE
        RETURN true;
    END IF;
END;
$$ LANGUAGE 'plpgsql' STRICT;

CREATE OR REPLACE FUNCTION getDistColType(table_name_in varchar(250), column_name text)
    RETURNS varchar(30) AS $$
DECLARE
    column_type varchar(30);
BEGIN
    -- Resolved via pg_attribute/regclass, not information_schema.columns
    -- string-matched by table_name: table_name_in can be schema-qualified
    -- (e.g. dist_mobilitydb.<x>_hash, a hash-staged copy -- see
    -- stage_hash_distributed_source), and information_schema.columns.
    -- table_name only ever holds the *bare* name -- a schema-qualified
    -- string (with the dot) never matches it at all, however the
    -- table_schema filter is handled, silently misreporting the column as
    -- untyped/unidentified instead of resolving it correctly.
    -- table_name_in::regclass resolves either form correctly on its own.
    SELECT t.typname INTO column_type
    FROM pg_attribute a
    JOIN pg_type t ON t.oid = a.atttypid
    WHERE a.attrelid = table_name_in::regclass
      AND a.attname = column_name
      AND a.attnum > 0 AND NOT a.attisdropped;
    return column_type;
END;
$$ LANGUAGE 'plpgsql' STRICT;

----------------------------------------------------------------------------------------------------------------------
-- partitioning_column_type returns the type of the multirelation column
----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION getDistributedColInternalType(table_name_in varchar(250), spatiotemporal_col_name varchar(30))
    RETURNS varchar(10) AS $$
DECLARE
    column_type text;
    column_sub_type varchar(10);
BEGIN
    -- MobilityDB or PostGIs
    -- Resolved via pg_attribute/regclass -- see the identical comment in
    -- getDistColType above; table_name_in can be schema-qualified here too
    -- (this is called with a hash-staged dist_mobilitydb.* copy from
    -- create_spatiotemporal_distributed_table).
    SELECT t.typname INTO column_type
    FROM pg_attribute a
    JOIN pg_type t ON t.oid = a.atttypid
    WHERE a.attrelid = table_name_in::regclass
      AND a.attname = spatiotemporal_col_name
      AND a.attnum > 0 AND NOT a.attisdropped;
    IF column_type = 'tgeompoint' THEN
        EXECUTE format('%s', concat('SELECT tempSubType(',spatiotemporal_col_name,') FROM ',table_name_in,' LIMIT 1;'))
        INTO column_sub_type;
    ELSIF column_type = 'geometry' THEN
        EXECUTE format('%s', concat('SELECT GeometryType(',spatiotemporal_col_name,') FROM ',table_name_in,' LIMIT 1;'))
        INTO column_sub_type;
    ELSE
        RAISE EXCEPTION 'The spatiotemporal type is not identified!';
    END IF;

    RETURN lower(column_sub_type);
END;
$$ LANGUAGE 'plpgsql' STRICT;

----------------------------------------------------------------------------------------------------------------------
-- getGroupCol returns the primary key of the given table
----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION getGroupCol(table_name_in varchar(250))
    RETURNS varchar(50) AS $$
DECLARE
    pk_column_name varchar(50);
    pk_column_type varchar(50);
BEGIN
    EXECUTE format('%s', concat('
                SELECT c.column_name, c.data_type
                FROM information_schema.table_constraints tc
                JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name)
                JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema
                  AND tc.table_name = c.table_name AND ccu.column_name = c.column_name
                WHERE constraint_type = ''PRIMARY KEY'' and tc.table_name = ''',table_name_in,''';'))
    INTO pk_column_name, pk_column_type;
    IF pk_column_type in ('integer', 'bigint') THEN
        RETURN pk_column_name;
    ELSE
        RAISE Exception 'The input table does not have a primary key! Please add it and try again!';
    END IF;
END;
$$ LANGUAGE 'plpgsql' STRICT;

----------------------------------------------------------------------------------------------------------------------
-- getGroupColWithFallback picks a hash-distribution key for a table that
-- otherwise has no usable primary key, unlike getGroupCol (above) which
-- raises rather than guess. Only used by stage_hash_distributed_source
-- (data_allocation.sql) to distribute a throwaway staging copy of a
-- single-node source table -- it never affects tiling.groupCol itself,
-- which keeps going through the strict getGroupCol and keeps requiring a
-- real integer/bigint primary key exactly as before.
--
-- Falls through three tiers: the primary key first (same query as
-- getGroupCol); then any UNIQUE constraint on an integer/bigint column
-- (still one row per value, just not the PK); then, as a last resort, the
-- first integer/bigint column in the table at all -- Citus' hash
-- distribution doesn't require the distribution column to be unique
-- (rows with equal values simply land on the same shard), so this still
-- produces a valid distributed table, just without a "one row per value"
-- guarantee. Only raises if the table has no integer/bigint column
-- whatsoever, since there's then nothing left to hash on.
----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION getGroupColWithFallback(table_name_in varchar(250))
    RETURNS varchar(50) AS $$
DECLARE
    pk_column_name varchar(50);
    pk_column_type varchar(50);
    unique_column_name varchar(50);
    unique_column_type varchar(50);
    fallback_column_name varchar(50);
    fallback_column_type varchar(50);
BEGIN
    EXECUTE format('%s', concat('
                SELECT c.column_name, c.data_type
                FROM information_schema.table_constraints tc
                JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name)
                JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema
                  AND tc.table_name = c.table_name AND ccu.column_name = c.column_name
                WHERE constraint_type = ''PRIMARY KEY'' and tc.table_name = ''',table_name_in,''';'))
    INTO pk_column_name, pk_column_type;
    IF pk_column_type in ('integer', 'bigint') THEN
        RETURN pk_column_name;
    END IF;

    EXECUTE format('%s', concat('
                SELECT c.column_name, c.data_type
                FROM information_schema.table_constraints tc
                JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name)
                JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema
                  AND tc.table_name = c.table_name AND ccu.column_name = c.column_name
                WHERE constraint_type = ''UNIQUE'' and tc.table_name = ''',table_name_in,'''
                ORDER BY c.ordinal_position LIMIT 1;'))
    INTO unique_column_name, unique_column_type;
    IF unique_column_type in ('integer', 'bigint') THEN
        RAISE INFO 'Table % has no primary key -- using UNIQUE column % to hash-distribute its staging copy', table_name_in, unique_column_name;
        RETURN unique_column_name;
    END IF;

    -- No table_schema filter, matching the PK/UNIQUE tiers above (they
    -- resolve schema implicitly via constraint_schema, not a hardcoded
    -- 'public') -- staging can be asked about a table that already lives in
    -- a non-public schema (e.g. dist_mobilitydb itself, if a caller stages
    -- something that's already been through a schema-qualified stage once),
    -- and this used to only ever find columns for a same-named table in
    -- public specifically, silently reporting "no integer/bigint column at
    -- all" for one that actually has plenty just in a different schema.
    EXECUTE format('%s', concat('
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = ''',table_name_in,'''
                  AND data_type IN (''integer'', ''bigint'')
                ORDER BY ordinal_position LIMIT 1;'))
    INTO fallback_column_name, fallback_column_type;
    IF fallback_column_name IS NOT NULL THEN
        RAISE WARNING 'Table % has no primary key or unique integer/bigint column -- hash-distributing its staging copy on % instead, which does not guarantee one row per value', table_name_in, fallback_column_name;
        RETURN fallback_column_name;
    END IF;

    RAISE EXCEPTION 'Table % has no integer/bigint column at all -- cannot hash-distribute a staging copy of it', table_name_in;
END;
$$ LANGUAGE 'plpgsql' STRICT;

----------------------------------------------------------------------------------------------------------------------
-- partitioning_column_name returns the multirelation column of the given table
----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION getDistributedCol(table_name_in varchar(250))
    RETURNS varchar(30) AS $$
DECLARE
column_name varchar(30);
BEGIN
-- No table_schema filter -- see the identical comment in getDistColType
-- above.
EXECUTE format('%s', concat('
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = ''',table_name_in,''' and udt_name in (''tgeompoint'', ''geometry'') ;'))
    INTO column_name;
return column_name;
END;
$$ LANGUAGE 'plpgsql' STRICT;