/* snowflake--2.2--2.3.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION snowflake UPDATE TO '2.3'" to load this file. \quit

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
