# Snowflake Sequences for PostgreSQL

For detailed information about using Snowflake sequences and examples, see the pgEdge [documentation](https://docs.pgedge.com/platform/advanced/snowflake#example-converting-an-existing-sequence). 

## Snowflake Sequences - Overview

Snowflake is a PostgreSQL extension that provides an `int8` and `sequence` based unique ID solution to optionally replace the PostgreSQL built-in `bigserial` data type.

Internally snowflake are 64 bit integers represented externally as `bigint` values. The 64 bits are divided into bit fields:

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
  second.

* The `node number` is a 10-bit unique identifier of the PostgreSQL
  instance inside a global cluster. This value is set with the
  GUC `snowflake.node` in the `postgresql.conf` file.

With this design each snowflake ID is unique within one `sequence`
across multiple PostgreSQL instances in a distributed cluster.

## Installation

**Installing Snowflake with pgEdge binaries**

To use pgEdge binaries to install Snowflake, go to [pgeEdge Github](https://github.com/pgEdge/pgedge) and install the pgEdge CLI:

`./pgedge install pg16 --start : install snowflake`

**Installing Snowflake from source code**

If you're installing from source code, we assume that you're familiar with how to build standard PostgreSQL extensions from source:

```
cd contrib
git clone https://github.com/pgEdge/snowflake-sequences.git
cd snowflake-sequences
USE_PGXS=1 make
USE_PGXS=1 make install
```

After installing the Snowflake extension with the pgEdge binary or from source code, connect to your Postgres database and create the extension with the command:

```
CREATE EXTENSION snowflake;
```

## Configuring Snowflake

The Snowflake extension uses a custom GUC named `snowflake.node` that determines the `node` part of each snowflake generated by the PostgreSQL instance. The permitted values are 1 thru 1023.

This GUC has an invalid default value (on purpose); if not set, the Snowflake extension will throw an exception on a call to `snowflake.nextval()`.  This is intended to prevent you from
accidentally missing this GUC in your `postgresql.conf` file.

If you ever intend to use snowflake in a multi-node, distributed
or replicated setup, it is important to set the GUC to a unique value
for each PostgreSQL instance. There is no protection in place to prevent 
assigning multiple PostgreSQL instances in a multi-master cluster
the same node number.

## Snowflake Functions

After you install and create the Snowflake extension, you can use Snowflake functions to implement and manage Snowflake sequences.  To review a complete list of Snowflake functions, see the [pgEdge online documentation](https://docs.pgedge.com/platform/advanced/snowflake#snowflake-functions).
