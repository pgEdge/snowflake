
/* snowflake/snowflake--2.2.sql */

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

-- ----------------------------------------------------------------------
-- convert_column_to_int8()
--
--	Change the data type of a column to int8 and recursively alter
--	all columns that reference this one through foreign key constraints.
-- ----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION snowflake.convert_column_to_int8(p_rel regclass, p_attnum smallint)
RETURNS integer
SET search_path = pg_catalog
AS $$
DECLARE
	v_attr			record;
	v_fk			record;
	v_attidx		integer;
	v_cmd			text;
	v_num_altered	integer := 0;
BEGIN
	-- ----
	-- Get the attribute definition
	-- ----
	SELECT * INTO v_attr
	FROM pg_namespace N
	JOIN pg_class C
		ON N.oid = C.relnamespace
	JOIN pg_attribute A
		ON C.oid = A.attrelid
	WHERE A.attrelid = p_rel
		AND A.attnum = p_attnum;

	IF NOT FOUND THEN
		RAISE EXCEPTION 'Attribute % of reation % not found', p_attnum, p_rel;
	END IF;

	-- ----
	-- If the attribute type is not bigint, we change it
	-- ----
	IF v_attr.atttypid <> 'int8'::regtype THEN
		v_cmd = 'ALTER TABLE ' ||
			quote_ident(v_attr.nspname) || '.' ||
			quote_ident(v_attr.relname) ||
			' ALTER COLUMN ' ||
			quote_ident(v_attr.attname) ||
			' SET DATA TYPE int8';
		RAISE NOTICE 'EXECUTE %', v_cmd;
		EXECUTE v_cmd;

		v_num_altered = v_num_altered + 1;
	END IF;

	-- ----
	-- Convert foreign keys referencing this column as well
	-- ----
	FOR v_fk IN
		SELECT * FROM pg_constraint F
			JOIN pg_class C
				ON C.oid = F.conrelid
			JOIN pg_namespace N
				ON N.oid = C.relnamespace
			WHERE F.contype = 'f'
			AND F.confrelid = v_attr.attrelid
	LOOP
		-- ----
		-- Lookup the attribute index in the possibly compount FK
		-- ----
		v_attidx = array_position(v_fk.confkey, v_attr.attnum);
		IF v_attidx IS NULL THEN
			CONTINUE;
		END IF;

		-- ----
		-- Recurse for the referencing column
		-- ----
		v_num_altered = v_num_altered +
			snowflake.convert_column_to_int8(v_fk.conrelid,
										 v_fk.conkey[v_attidx]);
	END LOOP;
	RETURN v_num_altered;
END;
$$ LANGUAGE plpgsql;

-- ----------------------------------------------------------------------
-- convert_sequence_to_snowflake()
--
--	Convert the DEFAULT expression for a sequence to snowflake's nextval()
--	function. Eventually change the data type of columns using it
--	to bigint.
-- ----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION snowflake.convert_sequence_to_snowflake(p_seqid regclass)
RETURNS integer
SET search_path = pg_catalog
AS $$
DECLARE
	objdesc			record; -- contains target (reloid,attnum) value
	v_attrdef		record;
	v_attr			record;
	v_seq			record;
	v_cmd			text;
	v_num_altered	integer := 0;
	v_last_value	bigint;

	v_seqname1		text;
	extseqname		text;
	is_identity		boolean;
BEGIN
	-- Identify the (relation,attnum) that uses this sequence as a source
	-- for values. Follow the logic of the getOwnedSequences_internal.
	--
	-- Complain, if such data wasn't found - incoming object may be not
	-- a sequence, or sequence which is used for different purposes.
	-- objdesc's fields:
	-- heapreloid - Oid of the target relation.
	-- nspname - namespace of the sequence
	-- seqname - sequence name
	-- attnum - number of relation's attribute that employs this sequence
	SELECT INTO objdesc
		refobjid AS heapreloid,
		c.relnamespace::regnamespace::text AS nspname,
		c.relname AS seqname,
		refobjsubid AS attnum
	FROM pg_depend AS d JOIN pg_class AS c ON (d.objid = c.oid)
	WHERE
		classid = 'pg_class'::regclass AND
		(deptype = 'i' OR deptype = 'a') AND
		c.oid = p_seqid AND relkind = 'S';

	IF (objdesc IS NULL) THEN
		raise EXCEPTION 'Input value "%" is not used by any relation as a DEFAULT value or an IDENTITY', p_seqid;
		RETURN false;
	END IF;

	SELECT INTO is_identity
		(attidentity = 'a' OR attidentity = 'd')
	FROM pg_attribute a JOIN pg_class c ON (c.oid = a.attrelid)
	WHERE a.attrelid = objdesc.heapreloid AND a.attnum = objdesc.attnum;

	IF (is_identity) THEN
		raise EXCEPTION 'transformation of an IDENTITY column to snowflake is not supported';
		RETURN false;
	END IF;

	-- ----
	-- We are looking for column defaults that use the requested
	-- sequence and the function nextval(). Because pg_get_expr()
	-- omits the schemaname of the sequence if it is "public" we
	-- need to be prepared for a schema qualified and unqualified
	-- name here.
	-- ----
	SELECT INTO v_seqname1
		quote_ident(objdesc.seqname);
	SELECT INTO extseqname
		quote_ident(objdesc.nspname) || '.' || quote_ident(objdesc.seqname);

	FOR v_attrdef IN
		WITH AD AS (
			SELECT AD.*,
				   pg_get_expr(AD.adbin, AD.adrelid, true) adstr
			FROM pg_attrdef AD
		)
		SELECT * FROM AD
		WHERE AD.adstr = 'nextval(' || quote_literal(v_seqname1) || '::regclass)'
		   OR AD.adstr = 'nextval(' || quote_literal(extseqname) || '::regclass)'
	LOOP
		-- ----
		-- Get the attribute definition
		-- ----
		SELECT * INTO v_attr
		FROM pg_namespace N
		JOIN pg_class C
			ON N.oid = C.relnamespace
		JOIN pg_attribute A
			ON C.oid = A.attrelid
		WHERE A.attrelid = v_attrdef.adrelid
			AND A.attnum = v_attrdef.adnum;

		IF NOT FOUND THEN
			RAISE EXCEPTION 'Attribute for % not found', v_attrdef.adstr;
		END IF;

		-- ----
		-- Get the sequence definition
		-- ----
		SELECT * INTO v_seq
		FROM pg_namespace N
		JOIN pg_class C
			ON N.oid = C.relnamespace
		WHERE C.oid = p_seqid;

		IF NOT FOUND THEN
			RAISE EXCEPTION 'Sequence with Oid % not found', p_seqid;
		END IF;

		-- ----
		-- If the attribute type is not bigint, we change it
		-- ----
		v_num_altered = v_num_altered +
			snowflake.convert_column_to_int8(v_attr.attrelid, v_attr.attnum);

		-- ----
		-- Now we can change the default to snowflake.nextval()
		-- ----
		v_cmd = 'ALTER TABLE ' ||
			quote_ident(v_attr.nspname) || '.' ||
			quote_ident(v_attr.relname) ||
			' ALTER COLUMN ' ||
			quote_ident(v_attr.attname) ||
			' SET DEFAULT snowflake.nextval(' ||
			quote_literal(
				quote_ident(v_seq.nspname) || '.' ||
				quote_ident(v_seq.relname)
			) ||
			'::regclass)';
		RAISE NOTICE 'EXECUTE %', v_cmd;
		EXECUTE v_cmd;

		v_num_altered = v_num_altered + 1;
	END LOOP;

	-- ----
	-- Finally we need to change the sequence itself to settings that
	-- prevent pg_catalog.nextval() from working. We do this by setting
	-- the sequence's MAXVAL to its current last_value + 1, then invoke
	-- our own nextval() function to bump it.
	-- ----
	v_cmd = 'SELECT last_value FROM ' ||
		pg_catalog.quote_ident(N.nspname) || '.' ||
		pg_catalog.quote_ident(C.relname)
		FROM pg_catalog.pg_class C
		JOIN pg_catalog.pg_namespace N ON N.oid = C.relnamespace
		WHERE C.oid = p_seqid;
	EXECUTE v_cmd INTO v_last_value;

	v_cmd = 'ALTER SEQUENCE ' ||
		pg_catalog.quote_ident(N.nspname) || '.' ||
		pg_catalog.quote_ident(C.relname) ||
		' NO CYCLE MAXVALUE ' ||
		v_last_value + 1
		FROM pg_catalog.pg_class C
		JOIN pg_catalog.pg_namespace N ON N.oid = C.relnamespace
		WHERE C.oid = p_seqid;
	RAISE NOTICE '%', v_cmd;
	EXECUTE v_cmd;

	PERFORM snowflake.nextval(p_seqid);
	v_num_altered = v_num_altered + 1;

	RETURN v_num_altered;
END;
$$ LANGUAGE plpgsql;
