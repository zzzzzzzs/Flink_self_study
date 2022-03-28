CREATE TABLE aaa
(
    id   INT,
    name STRING,
    primary key (`id`) NOT ENFORCED
)
    WITH ( 'connector' = 'mysql-cdc',
        'hostname' = 'localhost',
        'port' = '3306',
        'username' = 'root',
        'password' = '111',
        'database-name' = 'mydb',
        'table-name' = 'mydb.aaa',
        'scan.incremental.snapshot.enabled' = 'false'
        )