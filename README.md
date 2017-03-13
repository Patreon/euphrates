# Euphrates

Periodic table copier from mysql to redshift.

## Hows it work?

1. Take a list of tables
2. Use `mysqldump --xml` to turn each table into xml.
3. Transform the structure into a table named "_$name_new" where $name is the target table.
4. Transform the table data into JSON and store in s3 as segments.
5. Load segments into Redshift using COPY command.
6. Once all tables are loaded, perform swap of tables.

## Fragile by design

If any error is encountered, it immediately quits! If a record is unable to be copied, it immediately quits! Either your tables copied 100%, or it didn't happen.

## Running

mvn something -c config.json

## Config

```json
{
  "mysql": {
    "user": "root",
    "password": "xxx",
    "host": "some.rds.host",
    "port": 3306,
    "database": "db_name",
    "maxConnections": 6
  },
  "redshift": {
    "user": "root",
    "password": "xxx",
    "host": "something.us-east-1.redshift.amazonaws.com",
    "port": 5439,
    "database": "hotcar",
    "maxConnections": 4,
    "schema": "public"
  },
  "tables": [
    {
      "name": "users",
      "extra": "DISTKEY (user_id) INTERLEAVED SORTKEY (created_at, updated_at)",
      "columns": {
        "user_id": "integer not null ENCODE DELTA",
        "user_id": "integer not null ENCODE DELTA"
      }
    }
  ],
  "s3": {
    "bucket": "some-transfer-bucket",
    "region": "us-east-1",
    "accessKey": "xxx",
    "secretKey": "xxx",
    "minimumSegmentSize": 20000000
  }
}
```

## Known Issues

### Inconsistent snapshotting

Because each table is snapshotted independently, the start time for each table copy is slightly different. Can be fixed by acquiring current binlog coordinates first, and using that as the basis for each dump.

### Not using binlog

Hard to do bulk updates/inserts/deletes reliably and in a timely manner into redshift. Performance concerns.
(If you know how tell me!)

### Operations is tough!

I agree! Make a web interface, do all the scheduling in one continous process, allow for manual snapshots.
