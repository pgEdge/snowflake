/* snowflake/snowflake--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION snowflake" to load this file. \quit

GRANT USAGE ON SCHEMA snowflake TO public;

CREATE SEQUENCE snowflake.id_seq;
GRANT USAGE ON SEQUENCE snowflake.id_seq TO public;

CREATE FUNCTION snowflake.nextval(regclass = 'snowflake.id_seq'::regclass)
RETURNS pg_catalog.int8
AS 'MODULE_PATHNAME', 'snowflake_nextval'
LANGUAGE C;
GRANT EXECUTE ON FUNCTION snowflake.nextval(regclass) TO public;

CREATE FUNCTION snowflake.currval(regclass = 'snowflake.id_seq'::regclass)
RETURNS pg_catalog.int8
AS 'MODULE_PATHNAME', 'snowflake_currval'
LANGUAGE C;
GRANT EXECUTE ON FUNCTION snowflake.currval(regclass) TO public;

CREATE FUNCTION snowflake.get_epoch(pg_catalog.int8)
RETURNS pg_catalog.numeric
AS 'MODULE_PATHNAME', 'snowflake_get_epoch'
LANGUAGE C;
GRANT EXECUTE ON FUNCTION snowflake.get_epoch(pg_catalog.int8) TO public;

CREATE FUNCTION snowflake.get_count(pg_catalog.int8)
RETURNS pg_catalog.int4
AS 'MODULE_PATHNAME', 'snowflake_get_count'
LANGUAGE C;
GRANT EXECUTE ON FUNCTION snowflake.get_count(pg_catalog.int8) TO public;

CREATE FUNCTION snowflake.get_node(pg_catalog.int8)
RETURNS pg_catalog.int4
AS 'MODULE_PATHNAME', 'snowflake_get_node'
LANGUAGE C;
GRANT EXECUTE ON FUNCTION snowflake.get_node(pg_catalog.int8) TO public;

CREATE FUNCTION snowflake.format(pg_catalog.int8)
RETURNS jsonb
AS $$
	SELECT ('{"ts": "' || to_timestamp(snowflake.get_epoch($1))::text ||
		     '", "count": ' || snowflake.get_count($1)::text ||
			 ', "id": ' || snowflake.get_node($1)::text ||
			'}')::jsonb;
$$ language sql;
GRANT EXECUTE ON FUNCTION snowflake.format(pg_catalog.int8) TO public;
