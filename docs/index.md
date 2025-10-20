# Snowflake Sequences

Using a [`Snowflake` sequence] (https://github.com/pgEdge/snowflake?tab=readme-ov-file#snowflake-sequences-for-postgresql) allows you to take full advantage of the benefits offered by a distributed, multi-master replication solution. Snowflake is a Postgres extension that provides an int8 and sequence based unique ID solution to optionally replace the Postgres built-in bigserial data type. 

Internally, snowflakes are 64 bit integers represented externally as bigint values. The 64 bits are divided into bit fields:

* bit  63    - unused (sign of int8)
* bits 22-62 - timestamp with millisecond precision
* bits 10-21 - counter within one millisecond
* bits 0-9   - unique pgEdge Distributed Postgres node number
* The timestamp is a 41-bit unsigned value representing millisecond precision and an epoch of 2023-01-01.
* The counter is a 12-bit unsigned value that increments per ID allocation. This provides for 4096 unique IDs per millisecond, or 4 million IDs per second.
* The node number is a 10-bit unique identifier for the Postgres instance inside a global cluster. This value is set with the `snowflake.node` GUC in the `postgresql.conf` file.

Snowflake sequences let you:

* add or modify data in different regions while ensuring a unique transaction sequence.
* preserve unique transaction identifiers without manual/administrative management of a numbering scheme.
* accurately identify the order in which globally distributed transactions are performed.

Snowflake sequences also alleviate concerns that network lag could disrupt sequences in distributed transactions.

A Snowflake sequence is made up of a timestamp, a counter, and a unique node identifier; these components are sized to ensure over 18 trillion unique sequence numbers. The extension supports versatile commands and [functions](./snowflake_functions.md) that simplify [creating a new Snowflake sequence](./creating.md) or [converting an existing sequence](./converting.md).
