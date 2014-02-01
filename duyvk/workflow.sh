// creat hbase table
create 'github_events', 'actor', 'repository', 'created_at', 'event'

// import to hbase

python importFromJsonFile.py /home/kidio/assignments/DII/data-information-integration/duyvk/importFromJsonFile/data   // folder contains *.json
or
zcat data_gzip/*.gz | python importFromGzipFile.py             // so much faster than gzip open


// hive command
CREATE EXTERNAL TABLE  github_events_hive (rowkey STRING,actor_login STRING,actor_name STRING,repository_id STRING,repository_name STRING,repository_url STRING,repository_watchers INT,repository_stargazers INT,repository_forks INT,repository_language STRING,created_at_year INT,created_at_month INT,created_at_day INT,created_at_weekday INT,created_at_hour INT,created_at_timestamp TIMESTAMP,event_type STRING, event_message STRING)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,actor:login,actor:name,repository:id,repository:name,repository:url,repository:watchers,repository:stargazers,repository:forks,repository:language,created_at:year,created_at:month,created_at:day,created_at:weekday,created_at:hour,created_at:timestamp,event:type,event:message') 
TBLPROPERTIES ('hbase.table.name' = 'github_events'); 


// query

//----------- group by created_at_hour-------------------
select actor_login , created_at_hour , count(*) from github_events_hive group by actor_login, created_at_hour;

//----------- group by created_at_weekday-------------------
select actor_login , created_at_weekday , count(*) from github_events_hive group by actor_login, created_at_weekday;

//----------- group by repository_language-------------------
select actor_login , repository_name , count(*) from github_events_hive group by actor_login, repository_name;

//----------- group by repository_language------------------
select actor_login , repository_language , count(*) from github_events_hive group by actor_login, repository_language;
