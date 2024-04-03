# Snowflake Sequences for PostgreSQL

* [Implementation](#implementation)
* [Installation](#installation)
  * [Installation from binaries](#installation-from-pgedge-binaries)
  * [Installation from source code](#installation-from-source-code)
* [Configuration](#configuration)
* [API](#api)
* [Examples](#examples)
  * [New table using a snowflake as a PK](#new-table-using-a-snowflake-as-pk)
  * [Converting an existing bigserial column into a snowflake](#converting-an-existing-bigserial-column-into-a-snowflake)

## Implementation

**Snowflake** is a PostgreSQL extension providing an `int8`
and `sequence` based unique ID solution to optionally replace 
the PostgreSQL built-in `bigserial` data type.

Internally **Snowflakes** are 64 bit integers represented externally as `bigint` values. The 64 bits are divided into bit fields

```
bit  63    - unused (sign of int8)
bits 22-62 - timestamp with millisecond precision
bits 10-21 - counter within one millisecond
bits 0-9   - unique PostgreSQL node number set in postgresql.conf
```

* The timestamp is a 41-bit unsigned value representing millisecond
  precision and an epoch of 2023-01-01.

* The counter is a 12-bit unsigned value that increments per ID allocation.
  This provides for 4096 unique IDs per millisecond, or 4 million IDs per
  second. Even the most aggressive allocation of sequences cannot reach
  one million per second and a modified pgbench with a **Snowflake** based
  primary key on the `history` table can only generate IDs in the double
  digit per millisecond range.

  Should it be possible in the future to generate more than 4096
  **snowflakes** per millisecond the algorithm is going to bump the
  timestamp one millisecond into the future to keep **snowflakes**
  unique.

* The `node number` is a 10-bit unique identifier of the PostgreSQL
  instance inside a global cluster. This value must be set with the
  GUC `snowflake.node` in the `postgresql.conf` file.

With this design a **Snowflake** ID is unique within one `sequence`
across multiple PostgreSQL instances in a distributed cluster.

## Installation

### Installation from pgEdge binaries

Go to [pgeEdge Github](https://github.com/pgEdge/pgedge) and install the pgEdge CLI.

./pgedge install pg16 --start : install snowflake

### Installation from source code

For installation from source code it is assumed that the user is
familiar with how to build standard PostgreSQL extensions from a
source.

```
cd contrib
git clone https://github.com/pgEdge/snowflake-sequences.git
cd snowflake-sequences
USE_PGXS=1 make
USE_PGXS=1 make install
```

## Configuration

The **Snowflake** extension uses a custom GUC `snowflake.node`.
This configuration variable determines the `node` part of every
**snowflake**, generated by this PostgreSQL instance. The
permitted values are 1 thru 1023.

This configuration option has an invalid default value (on purpose).
If not set, the **Snowflake** extension will throw an exception on
a call to `snowflake.nextval()`.  This (hopefully) prevents you from
accidentally missing this GUC in your postgresql.conf file.

If you ever intend to use **Snowflakes** in a multi-node, distributed
or replicated setup, it is important to set the GUC to a unique value
for each PostgreSQL instance. There is nothing
in place to prevent you from shooting yourself in the foot by
assigning multiple PostgreSQL instances in a multi-master cluster
the same node number.

## API

After creating the `extension` via
```
CREATE EXTENSION snowflake;
```
the following functions become available:

* `snowflake.nextval([sequence regclass])`  
  Generates the next **snowflake** for the given sequence. If no
  sequence is specified the internal, database-wide sequence
  `snowflake.id_seq` will be used.

  **NOTE:** **snowflakes** are only unique per database, per sequence.
  If you need **snowflakes** to be unique across all **snowflake**
  columns within a database, you need to use the built-in sequence
  `snowflake.id_seq`. However, that may interfere with the usage
  of `currval()` of your application logic.

* `snowflake.currval([sequence regclass])`  
  Returns the current value of the given sequence (or the default, internal
  sequence). Like for PostgreSQL sequences this value is undefined until
  the function `snowflake.nextval()` has been called for the sequence in
  the current session.

* `snowflake.get_epoch(snowflake int8)`  
   Returns the timestamp part of the given **snowflake** as EPOCH
   (seconds since 2023-01-01) as a NUMERIC value with precision of
   three digits. One can use `to_timestamp(snowflake.get_epoch(<value>))`
   to convert this into an actual timestamp.

* `snowflake.get_count(snowflake int8)`  
  Returns the count part of the given **snowflake** as int4 value.
  This is a unique value within the milliseconds of the **snowflake**'s
  timestamp.

* `snowflake.get_node(snowflake int8)`  
  Returns the setting of GUC `snowflake.node` in postgresql.conf at
  the time, this **snowflake** was allocated.

* `snowflake.format(snowflake int8)`  
  Returns a `jsonb` object of the given **snowflake** like:  
  `{"node": 1, "ts": "2023-10-16 17:57:26.361+00", "count": 0}`

* `snowflake.convert_sequence_to_snowflake(p_relid regclass)`  
  Converts an existing sequence into a snowflake. This is done by
  changing any column that uses the given sequence with a
  `DEFAULT pg_catalog.nextval(relid)` expression. The column is
  forced to be type `int8` and the `DEFAULT` expression is
  altered to use the `snowflake.nextval()` function. Then all
  columns that eventually reference such column in a foreign key
  are forced to `int8` as well. Finally the sequence's MAXVALUE
  is adjusted to the current `last_value + 1`, which prevents
  the accidental use of `pg_catalog.nextval()` from user code.


## Examples

### New table using a **snowflake** as PK
This SQL code shows how to use **snowflake** in a newly created table.
```
-- Assuming extension snowflake has been installed

CREATE TABLE table1 (
    id          bigint PRIMARY KEY DEFAULT snowflake.nextval(),
    some_data   text
);

INSERT INTO table1 (some_data) VALUES ('first row');
INSERT INTO table1 (some_data) VALUES ('second row');

SELECT id, snowflake.format(id), some_data FROM table1;
```
Result:
```
        id         |                          format                           | some_data
-------------------+-----------------------------------------------------------+------------
 18014518154714241 | {"node": 1, "ts": "2023-10-16 18:47:12.257+00", "count": 0} | first row
 18014518154714242 | {"node": 1, "ts": "2023-10-16 18:47:12.258+00", "count": 0} | second row
(2 rows)
```

### Converting an existing bigserial column into a snowflake
This SQL code demonstrates how to convert an existing `bigserial` or
`serial8` column into a **snowflake**.
```
CREATE TABLE table2 (
    id          bigserial PRIMARY KEY,
    some_data   text
);

INSERT INTO table2 (some_data) VALUES ('first row');
INSERT INTO table2 (some_data) VALUES ('second row');

ALTER TABLE table2 ALTER COLUMN id SET DEFAULT snowflake.nextval();

-- Alternatively, if you need to retain the ability to retrieve
-- the snowflake.currval('table2_id_seq') of the sequence individually, use
-- ALTER TABLE table2 ALTER COLUMN id SET DEFAULT snowflake.nextval('table2_id_seq');

INSERT INTO table2 (some_data) VALUES ('third row');
INSERT INTO table2 (some_data) VALUES ('fourth row');

SELECT id, snowflake.format(id), some_data FROM table2;
```
Result:
```
        id         |                          format                           | some_data  
-------------------+-----------------------------------------------------------+------------
                 1 | {"node": 0, "ts": "2023-01-01 00:00:00.001+00", "count": 0} | first row
                 2 | {"node": 0, "ts": "2023-01-01 00:00:00.002+00", "count": 0} | second row
 18014518155600128 | {"node": 1, "ts": "2023-10-16 19:01:58.144+00", "count": 0} | third row
 18014518155600129 | {"node": 1, "ts": "2023-10-16 19:01:58.145+00", "count": 0} | fourth row
(4 rows)
```
**NOTE:** The `int8` value of ID remains unique, although it does
jump ahead quite a bit (which is in compliance with standard PostgreSQL
sequences that may have gaps). Because the **snowflake** EPOCH is
2023-01-01; your existing database would have had to use over 18 **trillion**
sequence numbers before this conversion to cause any trouble with
possible duplicate key values. 

