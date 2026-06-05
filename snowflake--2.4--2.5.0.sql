/* snowflake--2.4--2.5.0.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION snowflake UPDATE TO '2.5.0'" to load this file. \quit

-- ----------------------------------------------------------------------
-- 2.4 -> 2.5 - fix MAXVALUE on snowflake-converted sequences.
--
-- The pre-2.5 convert_sequence_to_snowflake() set MAXVALUE to
-- (old last_value + 1) when converting a sequence to snowflake IDs.
-- The intent was to "lock out" pg_catalog.nextval(), but
-- snowflake.nextval() bypasses MAXVALUE so the sequence happily
-- generated huge snowflake IDs while the catalog still reported a
-- tiny ceiling.  pg_dump captures both ("MAXVALUE 4" plus
-- "SELECT pg_catalog.setval(..., 4446196691613229056, true)") and
-- the resulting dump cannot be restored:
--
--   ERROR: setval: value 4446196691613229056 is out of bounds for
--          sequence "orders_id_seq" (1..4)
--
-- 2.5 changes the sequence's data type to bigint and sets MAXVALUE to
-- the full bigint range (2^63-1) so any snowflake ID and any
-- pg_dump-emitted setval stays in bounds.  This script:
--   1. Re-installs convert_sequence_to_snowflake() with the fix so
--      future conversions store the correct MAXVALUE.
--   2. Walks pg_sequences and raises MAXVALUE to 2^63-1 (and the
--      sequence data type to bigint) on any sequence whose column
--      default already uses snowflake.nextval() and whose max_value is
--      below bigint MAX.
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
	-- Finally bump the sequence past its current last_value by
	-- invoking snowflake.nextval().  The MAXVALUE is left at the
	-- full bigint range (2^63-1) so subsequent snowflake IDs - and
	-- the pg_catalog.setval() calls that pg_dump emits during a
	-- restore - all stay within bounds.
	-- ----
	v_cmd = 'ALTER SEQUENCE ' ||
		pg_catalog.quote_ident(N.nspname) || '.' ||
		pg_catalog.quote_ident(C.relname) ||
		' AS bigint NO CYCLE MAXVALUE 9223372036854775807'
		FROM pg_catalog.pg_class C
		JOIN pg_catalog.pg_namespace N ON N.oid = C.relnamespace
		WHERE C.oid = p_seqid;
	RAISE NOTICE '%', v_cmd;
	EXECUTE v_cmd;

	PERFORM snowflake.nextval(p_seqid);
	RETURN v_num_altered;
END;
$$ LANGUAGE plpgsql;

-- ----------------------------------------------------------------------
-- One-shot repair: raise MAXVALUE to 2^63-1 on every sequence that was
-- already converted to snowflake but still has the old, low ceiling.
--
-- convert_sequence_to_snowflake() supports three flavors of sequence
-- and the repair has to cover all three:
--   1. SERIAL / BIGSERIAL / IDENTITY  (pg_depend.deptype = 'a' or 'i')
--   2. Plain sequence used via an explicit nextval() column DEFAULT
--      (pg_depend.deptype = 'n')
--   3. Standalone sequence with no associated column, used directly
--      via snowflake.nextval('seq_name').
--
-- Flavors (1) and (2) are detected by the column DEFAULT being rewritten
-- to snowflake.nextval(...).  Flavor (3) leaves no such marker on a
-- column, so we detect it by the inconsistent on-disk state the bug
-- produces: last_value > max_value, which cannot occur in normal
-- PostgreSQL operation and is the unique signature of a pre-2.5
-- conversion of a standalone sequence.
-- ----------------------------------------------------------------------
DO $repair$
DECLARE
	r record;
	v_cmd text;
BEGIN
	FOR r IN
		-- Flavors 1 + 2: owned or explicit-default sequences whose
		-- column DEFAULT now references snowflake.nextval(...)
		SELECT n.nspname AS seq_nspname,
		       c.relname AS seq_relname,
		       s.max_value
		FROM pg_class c
		JOIN pg_namespace n   ON n.oid = c.relnamespace
		JOIN pg_sequences s   ON s.schemaname = n.nspname
		                     AND s.sequencename = c.relname
		JOIN pg_depend d      ON d.objid = c.oid
		                     AND d.classid = 'pg_class'::regclass
		                     AND d.deptype IN ('a','i','n')
		JOIN pg_attrdef ad    ON ad.adrelid = d.refobjid
		                     AND ad.adnum   = d.refobjsubid
		WHERE c.relkind = 'S'
		  AND s.max_value < 9223372036854775807
		  AND pg_get_expr(ad.adbin, ad.adrelid, true) LIKE '%snowflake.nextval(%'
		UNION
		-- Flavor 3: standalone sequence with no rewritten column DEFAULT.
		-- last_value > max_value is the unique signature of a pre-2.5
		-- conversion (snowflake.nextval bypasses MAXVALUE, so the value
		-- stored is well beyond the recorded ceiling).
		SELECT n.nspname AS seq_nspname,
		       c.relname AS seq_relname,
		       s.max_value
		FROM pg_class c
		JOIN pg_namespace n  ON n.oid = c.relnamespace
		JOIN pg_sequences s  ON s.schemaname = n.nspname
		                    AND s.sequencename = c.relname
		WHERE c.relkind = 'S'
		  AND s.max_value < 9223372036854775807
		  AND s.last_value IS NOT NULL
		  AND s.last_value > s.max_value
	LOOP
		v_cmd := format('ALTER SEQUENCE %I.%I AS bigint NO CYCLE MAXVALUE 9223372036854775807',
		                r.seq_nspname, r.seq_relname);
		RAISE NOTICE 'snowflake 2.4->2.5 repair: % (was max_value=%)', v_cmd, r.max_value;
		EXECUTE v_cmd;
	END LOOP;
END
$repair$;
