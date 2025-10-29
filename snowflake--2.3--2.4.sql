/* snowflake--2.3--2.4.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION snowflake UPDATE TO '2.4'" to load this file. \quit

-- ----------------------------------------------------------------------
-- convert_sequence_to_snowflake()
--
-- Convert a sequence to use snowflake's nextval() for the following cases:
--  - Used in an IDENTITY constraint
--  - Used as a generated sequence for Serial or Bigserial
--  - Used with nextval() as a default value
--  - A plain sequence
--
-- Returns the number of changes done, including column default changes,
-- and data type changes, such as changing to int8. The number does not
-- include changing the sequence itself.
--
-- NOTES:
-- 1. Changes the data type of affected columns to bigint.
-- 2. IDENTITY ALWAYS restriction will be eased to DEFAULT.
-- ----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION snowflake.convert_sequence_to_snowflake(p_seqid regclass)
RETURNS integer
SET search_path = pg_catalog
AS $$
DECLARE
	v_objdesc			record; -- contains target (reloid,attnum) value
	v_identdesc		record; -- sequence-related flags (attisidentity)
	v_attrdef		record;
	v_attr			record;
	v_seq			record;
	v_cmd			text;
	v_num_altered	integer := 0;
	v_last_value	bigint;
	v_seqname1		text;
	v_extseqname		text;
	v_is_serial_def	boolean;
	v_textstr			text;
BEGIN
	-- Identify the (relation,attnum) that uses this sequence as a source
	-- for values. Follow the logic of the getOwnedSequences_internal.
	--
	-- Complain, if such data wasn't found - incoming object may be not
	-- a sequence, or sequence which is used for different purposes.
	-- v_objdesc's fields:
	-- heapreloid - Oid of the target relation.
	-- nspname - namespace of the sequence
	-- seqname - sequence name
	-- attnum - number of relation's attribute that employs this sequence
	SELECT INTO v_objdesc
		refobjid AS heapreloid,
		c.relnamespace::regnamespace::text AS nspname,
		c.relname AS seqname,
		refobjsubid AS attnum,
		deptype
	FROM pg_depend AS d JOIN pg_class AS c ON (d.objid = c.oid)
	WHERE
		classid = 'pg_class'::regclass AND
		deptype IN ('a','i','n') AND
		c.oid = p_seqid AND relkind = 'S'
	ORDER BY deptype; -- prioritize (a)uto and (i)nternal before (n)ormal

	IF (v_objdesc IS NULL) THEN
		raise EXCEPTION 'Input value "%" is not a valid convertable sequence', p_seqid;
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
		quote_ident(v_objdesc.seqname);
	SELECT INTO v_extseqname
		quote_ident(v_objdesc.nspname) || '.' || quote_ident(v_objdesc.seqname);

	IF v_objdesc.deptype IN ('a','i') THEN
		-- Handle IDENTITY case and SERIAL/BIGSERIAL case

		SELECT INTO v_identdesc
			(attidentity = 'a' OR attidentity = 'd') AS is_identity,
			c.relnamespace::regnamespace::text AS nspname,
			c.relname AS relname,
			a.attname AS attname
		FROM pg_attribute a JOIN pg_class c ON (c.oid = a.attrelid)
		WHERE a.attrelid = v_objdesc.heapreloid AND a.attnum = v_objdesc.attnum;

		IF (v_identdesc.is_identity) THEN
			UPDATE pg_attribute SET attidentity = ''
			WHERE attrelid = v_objdesc.heapreloid AND attnum = v_objdesc.attnum;
			RAISE NOTICE
				'Update pg_attribute: reset attidentity value for table %, column %',
				v_objdesc.heapreloid::regclass, v_identdesc.attname;
		ELSE

			-- Extract DEFAULT definition for the column and conver it into
			-- a readable string representation.
			SELECT INTO v_textstr
				pg_get_expr(ad.adbin, ad.adrelid, true)
			FROM pg_attrdef ad
			WHERE adrelid = v_objdesc.heapreloid AND adnum = v_objdesc.attnum;

			-- Check that the DEFAULT expression contains input sequence in a form
			-- as related to the serial and bigserial type.
			SELECT INTO v_is_serial_def
			CASE WHEN
					v_textstr = 'nextval(' || quote_literal(v_seqname1) || '::regclass)' OR
					v_textstr = 'nextval(' || quote_literal(v_extseqname) || '::regclass)'
			THEN true ELSE false END;

			-- If there is another DEFAULT expression already set for this column, we
			-- are not so bold to remove it, just complain,
			IF (NOT v_is_serial_def) THEN
				raise EXCEPTION
					'definition of DEFAULT value for column "%" of relation "%" does not correspond serial or bigserial type: "%"',
					v_identdesc.attname, v_identdesc.relname, v_textstr;
			END IF;
		END IF;

		-- ----
		-- If the attribute type is not bigint, we change it
		-- ----
		v_num_altered =
			snowflake.convert_column_to_int8(v_objdesc.heapreloid::regclass,
										 v_objdesc.attnum::smallint);

		-- ----
		-- Now we can change the default to snowflake.nextval()
		-- ----
		v_cmd = 'ALTER TABLE ' ||
			quote_ident(v_identdesc.nspname) || '.' ||
			quote_ident(v_identdesc.relname) ||
			' ALTER COLUMN ' || quote_ident(v_identdesc.attname) ||
			' SET DEFAULT snowflake.nextval(' ||
			quote_literal(
				quote_ident(v_objdesc.nspname) || '.' ||
				quote_ident(v_objdesc.seqname)
			) || '::regclass)';
		RAISE NOTICE 'EXECUTE %', v_cmd;
		EXECUTE v_cmd;
	ELSE
		-- Look for cases where the sequence is used as an explicit default value

		FOR v_attrdef IN
			WITH AD AS (
				SELECT AD.*,
					   pg_get_expr(AD.adbin, AD.adrelid, true) adstr
				FROM pg_attrdef AD
			)
			SELECT * FROM AD
			WHERE AD.adstr = 'nextval(' || quote_literal(v_seqname1) || '::regclass)'
			   OR AD.adstr = 'nextval(' || quote_literal(v_extseqname) || '::regclass)'
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
	END IF;

	-- Note: for plain sequences not associated with columns, we still
	-- fall through to here so snowflake can be used, to support
	-- sequences used explicitly by applications

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
