1.安装ClickHouse
```bash
brew install clickhouse
```

2.本地运行
```bash
clickhouse local
```

3.导出数据成 parquet 格式
```bash
select * from postgresql('localhost:5432', 'stats', 'user_stats', 'postgres', 'postgres') into outfile 'user_stats.parquet'
```

4.clickhouse如何在 parquet 文件上查询, 注意要转换时间到对应的时区，否则默认使用UTC时区会导致某些误差
```bash
select count(*) from file('user_stats.parquet', Parquet) where last_visited_at >= CAST('2024-05-01', 'DateTime(\'Asia/Shanghai\')');
```

5.将 parquet 转 ndjson 格式
```bash
select * from file('user_stats.parquet', Parquet) LIMIT 100 INTO OUTFILE 'users.ndjson';
```

6.读取文件的Schema
```bash
describe file('user_stats.parquet', Parquet);
```

7.在ClickHouse中创建表

```bash
CREATE TABLE user_stats (
    `email` String,
    `name` String,
    `gender` String,
    `created_at` DateTime('UTC'),
    `last_visited_at` DateTime('UTC'),
    `last_watched_at` DateTime('UTC'),
    `recent_watched` Array(Int32),
    `viewed_but_not_started` Array(Int32),
    `started_but_not_finished` Array(Int32),
    `finished` Array(Int32),
    `last_email_notification` DateTime('UTC'),
    `last_in_app_notification` DateTime('UTC'),
    `last_sms_notification` DateTime('UTC'),
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(created_at)
PRIMARY KEY email
```

8.把Postgres里的数据写入的ClickHouse里面
```bash
insert into user_stats select * from posrgresql('localhost:5432', 'stats', 'user_stats', 'postgres', 'postgres')
```

9.也可以从ClickHouse中导出数据到parquet
```bash
select * from user_stats limit 1000 into outfile 'sample.parquet'
```
