
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
-- If input sequence is used for Serial or Bigserial DEFAULT values generation,
-- or participate in an IDENTITY constraint, alter this column definition to
-- use snowflake's nextval() as a DEFAULT value.
--
-- NOTES:
-- 1. Eventually change the data type of the column to bigint.
-- 2. IDENTITY ALWAYS restriction will be eased to DEFAULT.
-- ----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION snowflake.convert_sequence_to_snowflake(p_seqid regclass)
RETURNS integer
SET search_path = pg_catalog
AS $$
DECLARE
	objdesc			record; -- contains target (reloid,attnum) value
	identdesc		record; -- sequence-related flags (attisidentity, atthasdef)
	v_attrdef		record;
	v_cmd			text;
	v_num_altered	integer;
	v_last_value	bigint;

	v_seqname1		text;
	extseqname		text;
	is_serial_def	boolean;
	textstr			text;
--	msg1 record;
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

	SELECT INTO identdesc
		(attidentity = 'a' OR attidentity = 'd') AS is_identity,
		atthasdef,
		c.relnamespace::regnamespace::text AS nspname,
		c.relname AS relname,
		a.attname AS attname
	FROM pg_attribute a JOIN pg_class c ON (c.oid = a.attrelid)
	WHERE a.attrelid = objdesc.heapreloid AND a.attnum = objdesc.attnum;

	IF (identdesc.is_identity) THEN
		UPDATE pg_attribute SET attidentity = ''
		WHERE attrelid = objdesc.heapreloid AND attnum = objdesc.attnum;
		RAISE NOTICE
			'Update pg_attribute: reset attidentity value for table %, column %',
			objdesc.heapreloid::regclass, identdesc.attname;
	ELSE
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

		-- Extract DEFAULT definition for the column and conver it into
		-- a readable string representation.
		SELECT INTO textstr
			pg_get_expr(ad.adbin, ad.adrelid, true)
		FROM pg_attrdef ad
		WHERE adrelid = objdesc.heapreloid AND adnum = objdesc.attnum;

		-- Check that the DEFAULT expression contains input sequence in a form
		-- as related to the serial and bigserial type.
		SELECT INTO is_serial_def
		CASE WHEN
				textstr = 'nextval(' || quote_literal(v_seqname1) || '::regclass)' OR
				textstr = 'nextval(' || quote_literal(extseqname) || '::regclass)'
		THEN true ELSE false END;

		-- If there are another DEFAULT expression already set for this column, we
		-- are not so bold to remove it, just complain,
		IF (NOT is_serial_def) THEN
			raise EXCEPTION
				'definition of DEFAULT value for column "%" of relation "%" does not correspond serial or bigserial type: "%"',
				identdesc.attname, identdesc.relname, textstr;
		END IF;
	END IF;

	-- ----
	-- If the attribute type is not bigint, we change it
	-- ----
	v_num_altered =
		snowflake.convert_column_to_int8(objdesc.heapreloid::regclass,
										 objdesc.attnum::smallint);

	-- ----
	-- Now we can change the default to snowflake.nextval()
	-- ----
	v_cmd = 'ALTER TABLE ' ||
		quote_ident(identdesc.nspname) || '.' ||
		quote_ident(identdesc.relname) ||
		' ALTER COLUMN ' || quote_ident(identdesc.attname) ||
		' SET DEFAULT snowflake.nextval(' ||
		quote_literal(
			quote_ident(objdesc.nspname) || '.' ||
			quote_ident(objdesc.seqname)
		) || '::regclass)';
	RAISE NOTICE 'EXECUTE %', v_cmd;
	EXECUTE v_cmd;

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
	RETURN v_num_altered;
END;
$$ LANGUAGE plpgsql;
