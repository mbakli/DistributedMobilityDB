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
    EXECUTE format('%s', concat('
                SELECT udt_name
                FROM information_schema.columns
                WHERE table_schema = ''public''
                    AND table_name   = ''',table_name_in,''' and column_name=''',column_name,''' ;'))
    INTO column_type;
    return column_type;
END;
$$ LANGUAGE 'plpgsql' STRICT;

----------------------------------------------------------------------------------------------------------------------
-- partitioning_column_type returns the type of the partitioning column
----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION getDistributedColInternalType(table_name_in varchar(250), spatiotemporal_col_name varchar(30))
    RETURNS varchar(10) AS $$
DECLARE
    column_type text;
    column_sub_type varchar(10);
BEGIN
    -- MobilityDB or PostGIs
EXECUTE format('%s', concat('
            SELECT udt_name
            FROM information_schema.columns
            WHERE table_schema = ''public''
                AND table_name   = ''',table_name_in,''' and column_name=''',spatiotemporal_col_name,''' ;'))
    INTO column_type;
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
-- partitioning_column_name returns the partitioning column of the given table
----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION getDistributedCol(table_name_in varchar(250))
    RETURNS varchar(30) AS $$
DECLARE
column_name varchar(30);
BEGIN
EXECUTE format('%s', concat('
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = ''public''
                AND table_name = ''',table_name_in,''' and udt_name in (''tgeompoint'', ''geometry'') ;'))
    INTO column_name;
return column_name;
END;
$$ LANGUAGE 'plpgsql' STRICT;