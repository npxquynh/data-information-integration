CREATE EXTERNAL TABLE  github_events_hive (rowkey STRING,actor_login STRING,actor_name STRING,repository_id STRING,repository_name STRING,repository_url STRING,repository_watchers INT,repository_stargazers INT,repository_forks INT,repository_language STRING,created_at_year INT,created_at_month INT,created_at_day INT,created_at_weekday INT,created_at_hour INT,created_at_timestamp TIMESTAMP,event_type STRING)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,actor:login,actor:name,repository:id,repository:name,repository:url,repository:watchers,repository:stargazers,repository:forks,repository:language,created_at:year,created_at:month,created_at:day,created_at:weekday,created_at:hour,created_at:timestamp,event:type')
TBLPROPERTIES ('hbase.table.name' = 'github_events');
