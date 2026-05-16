/*
 * maxvalue.sql - regression for the SUP-140 dump/restore failure.
 *
 * Before the fix, convert_sequence_to_snowflake() set
 *   ALTER SEQUENCE ... MAXVALUE (last_value + 1)
 * which is fine for the snowflake.nextval() path (it bypasses MAXVALUE)
 * but fatal for pg_dump/restore: the dump captures the resulting
 * snowflake-sized last_value via pg_catalog.setval(...) and the
 * restore is rejected with
 *   ERROR: setval: value <big_snowflake_id> is out of bounds for
 *          sequence "<seq>" (1..<small>)
 *
 * This test asserts that after conversion the sequence's max_value is
 * the full bigint range (2^63-1).
 */

\set VERBOSITY terse

SET snowflake.node = 1;

CREATE EXTENSION snowflake;

-- ----------------------------------------------------------------------
-- 1. Customer reproducer: bigserial table, INSERT, convert.
-- ----------------------------------------------------------------------
CREATE TABLE orders (
    id    bigserial PRIMARY KEY,
    customer text   NOT NULL,
    amount  numeric  NOT NULL
);
INSERT INTO orders (customer, amount) VALUES
   ('Alice', 100.00),
   ('Bob',   250.00),
   ('Carol',  75.50);

SELECT snowflake.convert_sequence_to_snowflake('orders_id_seq'::regclass);

-- After conversion max_value MUST be the bigint ceiling, not (last_value+1).
SELECT max_value = 9223372036854775807 AS max_is_bigint_max
FROM pg_sequences WHERE sequencename = 'orders_id_seq';

-- ----------------------------------------------------------------------
-- 2. Generate a real snowflake-sized id; setval to that value must succeed
--    (this is what pg_dump emits in the restore script).
-- ----------------------------------------------------------------------
INSERT INTO orders (customer, amount) VALUES ('Dave', 999) RETURNING id > 9999;

-- A pg_catalog.setval() with a snowflake-sized value must NOT raise
-- "out of bounds for sequence" any more.
SELECT pg_catalog.setval('orders_id_seq', 4446196691613229056, true);

SELECT last_value FROM orders_id_seq;

-- ----------------------------------------------------------------------
-- 3. Plain (un-owned) sequence path.
-- ----------------------------------------------------------------------
CREATE SEQUENCE standalone_seq START 100;
SELECT snowflake.convert_sequence_to_snowflake('standalone_seq'::regclass);
SELECT max_value = 9223372036854775807 AS max_is_bigint_max
FROM pg_sequences WHERE sequencename = 'standalone_seq';
SELECT pg_catalog.setval('standalone_seq', 4446196691613229056, true);

-- ----------------------------------------------------------------------
-- Cleanup
-- ----------------------------------------------------------------------
DROP TABLE orders CASCADE;
DROP SEQUENCE standalone_seq;
DROP EXTENSION snowflake;
