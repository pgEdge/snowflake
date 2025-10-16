# Snowflake Functions

You can query the following Snowflake functions at the psql command line to return useful information about tables that use Snowflake sequences.

**`snowflake.nextval([sequence regclass])`** 
Generates the next Snowflake in the specified sequence. If no sequence is specified, the internal, database-wide sequence `snowflake.id_seq` is used. For example, the following query returns the next number in the `orders_id_seq` sequence:

```sql
acctg=# SELECT * FROM snowflake.nextval('orders_id_seq'::regclass);
      nextval       
--------------------
 136169504773242881
(1 row)
```

**`snowflake.currval([sequence regclass])`** 
Returns the current value of the specified sequence. This value is undefined until the function `snowflake.nextval()` has been called for the sequence in the current session. For example, the following query returns the current value of the `orders_id_seq` sequence:

```sql
acctg=# SELECT * FROM snowflake.currval('orders_id_seq'::regclass);
      currval       
--------------------
 136169504773242881
(1 row)
```

**`snowflake.get_epoch(snowflake int8)`** Returns the timestamp part of a Snowflake as `EPOCH` (seconds since 2023-01-01) as a `NUMERIC` value with precision of three digits. For example, the timestamp of the following sequence value is `1704996539.845` seconds past Jan. 1, 2023:

```sql
acctg=# SELECT * FROM snowflake.get_epoch(136169504773242881);
   get_epoch    
----------------
 1704996539.845
(1 row)
```

You can use the `to_timestamp(snowflake.get_epoch(<value>))` function to convert the epoch into a timestamp.  For example:

```sql
acctg=# SELECT to_timestamp(snowflake.get_epoch(136169504773242881));
        to_timestamp        
----------------------------
 2024-01-11 13:08:59.845-05
(1 row)
```

**`snowflake.get_count(snowflake int8)`** Returns the count part of the given Snowflake as `int4` value; count resets to `0` for each new millisecond. For example:

```sql
acctg=# SELECT snowflake.get_count(136169504773242881);
 get_count 
-----------
         3
(1 row)
```

**`snowflake.get_node(snowflake int8)`** Returns the value of `snowflake.node` in `postgresql.conf` for the host of the specified Snowflake sequence. For example, the Snowflake sequence number `136169504773242881` resides on Node `1`:

```sql
acctg=# SELECT * FROM snowflake.get_node(136169504773242881);
 get_node 
----------
        1
(1 row)
```

**`snowflake.format(snowflake int8)`** Returns a `jsonb` object of the given Snowflake in the form: `{"node": 1, "ts": "2023-10-16 17:57:26.361+00", "count": 0}`.  For example:

```sql
acctg=# SELECT * FROM snowflake.format(136169504773242881);
                          format                           
-----------------------------------------------------------
 {"id": 1, "ts": "2024-01-11 13:08:59.845-05", "count": 0}
(1 row)
```


