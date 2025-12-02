# Converting a PostgreSQL Sequence to a Snowflake Sequence

You cannot directly create a snowflake sequence; you must first create a sequence in Postgres and then convert it. Sequences are created explicitly with the `CREATE SEQUENCE` command but are also implicitly created by `GENERATED { ALWAYS | BY DEFAULT } AS IDENTITY` table columns or `SERIAL` and `BIGSERIAL` columns. `GENERATED AS IDENTITY` is the recommended ANSI SQL method.

You can use the `snowflake.convert_sequence_to_snowflake()` function to convert individual sequences to snowflake sequences. Note that this converts the sequence definition; existing values in a sequence column will not change. The command syntax in SQL is:

    `SELECT snowflake.convert_sequence_to_snowflake('sequence_name');`

**Where**

`sequence_name` is the name of a sequence.

There are a number of ways to return a list of sequences. One way is to use the Postgres [`psql client`](https://www.postgresql.org/docs/current/app-psql.html) to execute the following command:

```
    \ds
```

Another way is to query metadata catalog information directly in `psql` or another SQL editor.

```
    SELECT * FROM information_schema.sequences;
```

The second command will include data type information, which is useful for learning which sequences will be converted to the BIGINT data type (64 bits).


## Example: Converting an Existing Sequence

The example that follows demonstrates using the psql command line to convert a sequence; in this example, we are using a table named `orders` that has three columns; the last column is a sequence named `id`:

```
acctg=# CREATE TABLE orders (customer VARCHAR, invoice VARCHAR, id bigserial PRIMARY KEY);
CREATE TABLE
```

After creating the table, we insert data into the `orders` table. We only need to provide content for the first two columns, since the sequence definition keeps track of the value of the third column and adds it as needed:

```
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

```
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

To convert the sequence definition for the `orders` table to use Snowflake sequences, invoke the following command in `psql` or another SQL editor:

```
SELECT snowflake.convert_sequence_to_snowflake('orders_id_seq'::regclass);
```

The conversion process modifies the sequence definition to use Snowflake sequences, but does not update existing rows. If we reconnect with psql and add new rows to the table, the new row's `id` will be a Snowflake sequence:

```
acctg=# INSERT INTO orders VALUES ('Prince William Schools', 'math_8330');
INSERT 0 1
acctg=# INSERT INTO orders VALUES ('Fluvanna Schools', 'art_9447');
INSERT 0 1
```

In the query results that follows, you can see the unformatted sequence values in the `id` column, and the same information in the `format` column, formatted with the `snowflake.format(id)` function. The rows added before the conversion to Snowflake sequences show a fixed timestamp of `2022-12-31 19:00:00-05`, while the Snowflake sequences have a unique `id` and timestamp.

Original entries in the table display a Postgres sequence, while entries made after the conversion display Snowflake sequences:

```
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

**INCREMENT and Caching**

Within the same millisecond, snowflake will allow a maximum of 4096 values. Some frameworks like the ORM framework Hibernate may be configured to try to fetch a sequence value, cache and assign a range by using an `INCREMENT` value for the sequence. Snowflake sequences honor the increment value, but you should not use a value greater than 4096. If you would like to change the increment value, use `ALTER SEQUENCE <seq_name> INCREMENT <increment_value> NO MAXVALUE`.
