/* snowflake--2.0--2.2.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION snowflake UPDATE TO '2.2'" to load this file. \quit

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
	v_attrdef		record;
	v_attr			record;
	v_seq			record;
	v_cmd			text;
	v_num_altered	integer := 0;
	v_last_value	bigint;

	v_seqname1		text;
	v_seqname2		text;
BEGIN
	-- ----
	-- We are looking for column defaults that use the requested
	-- sequence and the function nextval(). Because pg_get_expr()
	-- omits the schemaname of the sequence if it is "public" we
	-- need to be prepared for a schema qualified and unqualified
	-- name here.
	-- ----
	SELECT INTO v_seqname1
		quote_ident(C.relname)
	FROM pg_class C
	WHERE C.oid = p_seqid;

	SELECT INTO v_seqname2
		quote_ident(N.nspname) || '.' || quote_ident(C.relname)
	FROM pg_class C
	JOIN pg_namespace N ON N.oid = C.relnamespace
	WHERE C.oid = p_seqid;

	FOR v_attrdef IN
		WITH AD AS (
			SELECT AD.*,
				   pg_get_expr(AD.adbin, AD.adrelid, true) adstr
			FROM pg_attrdef AD
		)
		SELECT * FROM AD
		WHERE AD.adstr = 'nextval(' || quote_literal(v_seqname1) || '::regclass)'
		   OR AD.adstr = 'nextval(' || quote_literal(v_seqname2) || '::regclass)'
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
