# Creating a Snowflake Sequence in a New Cluster

Before you use Snowflake sequences, each node in your cluster must have a unique `snowflake.node` identifier. The value ranges from 1 to 1023, and it must be distinct on every node in the cluster.

## Setting snowflake.node in a managed deployment

A pgEdge deployment assigns `snowflake.node` for you, deriving the value from each node's place in the cluster:

* the Control Plane sets `snowflake.node` from the node ordinal.
* pgEdge Helm charts derive `snowflake.node` from each node name.
* pgEdge Ansible playbooks set `snowflake.node` from the node zone.

When you deploy with one of these methods, no further action is needed to configure the node identifier.

## Setting snowflake.node manually

For a self-managed or from-source installation, set the value yourself. Use `ALTER SYSTEM` to assign a unique node number, then reload the server so the change takes effect. The following example sets the node identifier to `10`:

```sql
ALTER SYSTEM SET snowflake.node = 10;
SELECT pg_reload_conf();
```

You can also add the parameter to the `postgresql.conf` file directly. The following line identifies the host as node `11`:

```ini
snowflake.node = 11
```

After you edit `postgresql.conf`, reload the server for the change to take effect. You can reload from psql:

```sql
SELECT pg_reload_conf();
```

You can also reload the server with `pg_ctl`:

```sh
pg_ctl reload -D "/path/to/your/data/directory"
```

Once Snowflake is configured, you can begin to use Snowflake sequences by first [converting](converting.md) them from Postgres sequences.
