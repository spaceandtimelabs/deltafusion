# deltafusion

A repo which demonstrates the datafusion CLI querying deltalake tables.

## Usage

```shell
cargo run
```

```sql
select * from demo;
```

```text
+---------------+----------------+
| accountNumber | accountBalance |
+---------------+----------------+
| 1             | 100            |
| 2             | 600            |
| 3             | 390            |
+---------------+----------------+
3 rows in set. Query took 0.009 seconds.
```