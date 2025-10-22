# Creating a Snowflake Sequence in a New Cluster

Before using Snowflake sequences, each node in your cluster must be assigned a unique `snowflake.node` identity in the `postgresql.conf` file. There are three ways to set the value of `snowflake.node`. You can:

* use the `pgedge spock node-create` command.
* use the `pgedge db guc-set snowflake.node` command.
* manually modify the `postgresql.conf` file.

If your cluster has 9 nodes (or fewer), and you use the node naming convention `N1`, `N2`, `N3` for your nodes, the `pgedge spock node-create` command will set the `snowflake.node` value for you. When you invoke the `pgedge spock node-create` command to define a new node, the value of `snowflake.node` is displayed after the command confirmation:

```sh
[pgedge]$ ./pgedge spock node-create n2 'host=10.2.1.2 user=pgedge port=5432 dbname=acctg' acctg

[
  {
	"node_create": 560818415
  }
]
  new: snowflake.node = 2
```

If you have a cluster with more than 9 nodes or a different node naming convention (other than N1, N2, etc.), you can use the `pgedge db guc-set` command to manually set `snowflake.node` to a numeric value that is unique within your cluster. The syntax is:

`[pgedge]$ ./pgedge database_name guc-set snowflake.node node_number`

**Where**

`node_number` is a unique number associated with the node. For example, the following command sets the `snowflake.node` value to `10`:

```sh
[pgedge]$ ./pgedge acctg guc-set snowflake.node 10
```
You also have the option to manually edit the `postgresql.conf` file, adding the value of `snowflake.node`. For example, you can add the following statement at the end of the file to identify the host as node `11`:

```sh
snowflake.node = 11
```

After setting the node identifier, you need to reload the server before using a Snowflake sequence.  You can use the `pgedge reload` command:

```sh
[pgedge]$ ./pgedge reload pg16
pg16 reloading
```

or use the `pg_ctl reload` command to reload the server:

```sh
[pgedge]$ /home/pgedge/pg16/bin/pg_ctl reload -D "/home/pgedge/data/pg16"
server signaled
```
