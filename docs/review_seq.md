# Reviewing Sequence Definitions in your Table Meta-data

If you query the table meta-data before performing a sequence conversion with the `\d+ orders` command, the PostgreSQL sequence is defined in the `Default` column:

```sh
acctg=# \d+ orders;
                                                             Table "public.orders"
  Column  |       Type        | Collation | Nullable |              Default               | Storage  | Compression | Stats target | Description 
----------+-------------------+-----------+----------+------------------------------------+----------+-------------+--------------+-------------
 customer | character varying |           |          |                                    | extended |             |              | 
 invoice  | character varying |           |          |                                    | extended |             |              | 
 id       | bigint            |           | not null | nextval('orders_id_seq'::regclass) | plain    |             |              | 
Indexes:
    "orders_pkey" PRIMARY KEY, btree (id)
Access method: heap
```

After performing a conversion, the `\d+ table_name` command shows a `Default` value of `snowflake.nextval('orders_id_seq'::regclass)`:

``` sh
acctg=# \d+ orders
                                                                  Table "public.orders"
  Column  |       Type        | Collation | Nullable |                   Default                    | Storage  | Compression | Stats target | Descripti
on 
----------+-------------------+-----------+----------+----------------------------------------------+----------+-------------+--------------+----------
---
 customer | character varying |           |          |                                              | extended |             |              | 
 invoice  | character varying |           |          |                                              | extended |             |              | 
 id       | bigint            |           | not null | snowflake.nextval('orders_id_seq'::regclass) | plain    |             |              | 
Indexes:
    "orders_pkey" PRIMARY KEY, btree (id)
Access method: heap
```


