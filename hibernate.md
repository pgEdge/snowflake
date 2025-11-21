Hibernate uses identifiers as the primary key of an entity. There are ways to
define identifiers in Hibernate, which are unique keys for an entity, ensuring
they are not null.

pgEdge Snowflake Sequences for PostgreSQL can be used with Hibernate, in this
document we will share guidelines.

The simplest way to define an identifier is to use the `@Id` annotation. Let's
create related table in the database i.e.

```
CREATE TABLE CITY (
ID BIGINT NOT NULL,
NAME VARCHAR(20) DEFAULT NULL,
PRIMARY KEY (ID)
);
```

Here we have created `id` column as `bigint` type as Snow flake uses 64bit integers
that can be represented externally as bigint values.

Now create a simple object with the @Entity annotation represent database table i.e.

```
@Entity
@Table(name= "City")
public class City {
@Id
@GenericGenerator(name="seq_id", strategy="com.pgedge.snowflake.SnowFlakeSeqGenerator")
@GeneratedValue(generator="seq_id")
@Column(name = "id")
private long id;
@Column(name = "name")
private String name;

public long getId() {
    return id;
}

public void setId(long id) {
    this.id = id;
}

public String getName() {
    return name;
}

public void setName(String name) {
    this.name = name;
}
}
```

Here we are using `@GenericGenerator` to allows the use of custom identifier generation strategies i.e.

```
public class SnowFlakeSeqGenerator implements IdentifierGenerator {
@Override
public Object generate(SharedSessionContractImplementor sharedSessionContractImplementor, Object o) {
Query query = sharedSessionContractImplementor.createQuery( "select snowflake.nextval()" );
return (Long)query.getSingleResult();
}
}
```

It uses `snowflake.nextval()` function to generates the next snowflake for the sequence.

Now when you create City object and commit the transcation e.g.

```
City city=new City();
city.setName("London");
```

It will populate database table `city` with a new record with sequence number and city name i.e.

```
SELECT
	*
FROM
	CITY;

"id"	"name"
170505378935607296	"London"
```

It is a basic example to demostrate use of pgEdge Snowflake sequence with the
Java Hibernate code.


**INCREMENT and Caching**

Within the same millisecond, snowflake will allow a maximum of 4096 values. Hibernate may be configured to try to fetch a sequence value, cache and assign a range by using an `INCREMENT` value for the sequence. Snowflake sequences honor the increment value, but you should not use a value greater than 4096. If you would like to change the increment value, use `ALTER SEQUENCE <seq_name> INCREMENT <increment_value> NO MAXVALUE`.
