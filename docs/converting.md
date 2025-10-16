# Converting a PostgreSQL Sequence to a Snowflake Sequence with the Spock Extension

If you are using the spock extension, you can use the `pgedge spock sequence-convert` command at the psql command line to convert your existing sequence definitions into Snowflake sequences. Note that this converts the sequence definition; existing values in a sequence column will not change. The command syntax is:

`pgedge spock.sequence_convert sequence_name`

**Where**

`sequence_name` is the name of a sequence; you can use wildcards when specifying the sequence name to convert sequences in more than one table. 

For example, the following command:

```sh
[pgedge]$ ./pgedge spock.sequence_convert my_sequence
```

Converts the sequences in tables that reside in the `public` schema of the `acctg` database to use Snowflake sequences. If you invoke the command on one node of a replicating cluster, the table definition updates are propagated to the other nodes in the cluster.  


## Example: Converting an Existing Sequence

The example that follows starts at the psql command line; in this example, we are using a table named `orders` that has three columns; the last column is a sequence named `id`:

```sh
acctg=# CREATE TABLE orders (customer VARCHAR, invoice VARCHAR, id bigserial PRIMARY KEY);
CREATE TABLE
```

After creating the table, we insert data into the `orders` table. We only need to provide content for the first two columns, since the sequence definition keeps track of the value of the third column and adds it as needed:

```sh
acctg=# INSERT INTO orders VALUES ('Chesterfield Schools', 'art_9338');
INSERT 0 1
acctg=# INSERT INTO orders VALUES ('Chesterfield Schools', 'math_9663');
INSERT 0 1
acctg=# INSERT INTO orders VALUES ('Albemarle Schools', 'sci_2009');
INSERT 0 1
acctg=# INSERT INTO orders VALUES ('King William Schools', 'sci_7399');
INSERT 0 1
acctg=# INSERT INTO orders VALUES ('King William Schools', 'art_9484');
INSERT 0 1
acctg=# INSERT INTO orders VALUES ('Hanover Schools', 'music_1849');
INSERT 0 1
acctg=# INSERT INTO orders VALUES ('Washington Schools', 'hist_2983');
INSERT 0 1
```
When we select the rows from our table, we can see the sequence numbers in the `id` column:

```sh
acctg=# SELECT * FROM orders;
       customer       |  invoice   | id 
----------------------+------------+----
 Chesterfield Schools | art_9338   |  1
 Chesterfield Schools | math_9663  |  2
 Albemarle Schools    | sci_2009   |  3
 King William Schools | sci_7399   |  4
 King William Schools | art_9484   |  5
 Hanover Schools      | music_1849 |  6
 Washington Schools   | hist_2983  |  7
(7 rows)
```
To convert the sequence definition for the `orders` table to use Snowflake sequences, we exit psql on `n1`, and invoke the command:

```sh
[pgedge]$ ./pgedge spock sequence-convert public.orders_id_seq acctg
Converting sequence public.orders_id_seq to snowflake sequence.
```

The conversion process modifies the sequence definition to use Snowflake sequences, but does not update existing rows. If we reconnect with psql and add new rows to the table, the new row's `id` will be a Snowflake sequence:

```sh
acctg=# INSERT INTO orders VALUES ('Prince William Schools', 'math_8330');
INSERT 0 1
acctg=# INSERT INTO orders VALUES ('Fluvanna Schools', 'art_9447');
INSERT 0 1
```

In the query results that follows, you can see the unformatted sequence value in the `id` column, and the same information in the `format` column, formatted with the `snowflake.format(id)` function. The rows added before the conversion to Snowflake sequences show a fixed timestamp of `2022-12-31 19:00:00-05`, while the Snowflake sequences have a unique `id` and timestamp.

Original entries in the table display a Postgres sequence, while entries made after the conversion display Snowflake sequences:

```sh
acctg=# SELECT id, snowflake.format(id), customer, invoice FROM orders;
         id         |                          format                           |        customer        |  invoice   
--------------------+-----------------------------------------------------------+------------------------+------------
                  1 | {"id": 1, "ts": "2022-12-31 19:00:00-05", "count": 0}     | Chesterfield Schools   | art_9338
                  2 | {"id": 2, "ts": "2022-12-31 19:00:00-05", "count": 0}     | Chesterfield Schools   | math_9663
                  3 | {"id": 3, "ts": "2022-12-31 19:00:00-05", "count": 0}     | Albemarle Schools      | sci_2009
                  4 | {"id": 4, "ts": "2022-12-31 19:00:00-05", "count": 0}     | King William Schools   | sci_7399
                  5 | {"id": 5, "ts": "2022-12-31 19:00:00-05", "count": 0}     | King William Schools   | art_9484
                  6 | {"id": 6, "ts": "2022-12-31 19:00:00-05", "count": 0}     | Hanover Schools        | music_1849
                  7 | {"id": 7, "ts": "2022-12-31 19:00:00-05", "count": 0}     | Washington Schools     | hist_2983
 135824181823537153 | {"id": 1, "ts": "2024-01-10 14:16:48.438-05", "count": 0} | Prince William Schools | math_8330
 135824609030176769 | {"id": 1, "ts": "2024-01-10 14:18:30.292-05", "count": 0} | Fluvanna Schools       | art_9447
(9 rows)
```
