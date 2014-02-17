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

CREATE EXTERNAL TABLE lang_emotion_hive (rowkey STRING,anger INT,joy INT, amusement INT, surprise INT, swear INT, issue INT)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key#b,emotion:anger#b,emotion:joy#b,emotion:amusement#b,emotion:surprise#b,emotion:swear#b,emotion:issue#b')
TBLPROPERTIES ('hbase.table.name' = 'lang_emotion'); 





// query

//----------- group by created_at_hour-------------------
select actor_login , created_at_hour , count(*) from github_events_hive group by actor_login, created_at_hour;

//----------- group by created_at_weekday-------------------
select actor_login , created_at_weekday , count(*) from github_events_hive group by actor_login, created_at_weekday;

//----------- group by repository_language-------------------
select actor_login , repository_name , count(*) from github_events_hive group by actor_login, repository_name;

//----------- group by repository_language------------------
select actor_login , repository_language , count(*) from github_events_hive group by actor_login, repository_language;









------------------------DAILY-----------------------




// create a daily table in hive
CREATE  TABLE  github_events_hive_daily (rowkey STRING,actor_login STRING,actor_name STRING,repository_id STRING,repository_name STRING,repository_url STRING,repository_watchers INT,repository_stargazers INT,repository_forks INT,repository_language STRING,created_at_weekday INT,created_at_hour INT,created_at_timestamp TIMESTAMP,event_type STRING, event_message STRING)
PARTITIONED by (created_at_year INT,created_at_month INT,created_at_day INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';


// insert data from "github_events_hive" to "github_events_hive_daily"
INSERT OVERWRITE TABLE github_events_hive_daily PARTITION (created_at_year=2014,created_at_month=2,created_at_day=1)
SELECT rowkey ,actor_login ,actor_name ,repository_id ,repository_name ,repository_url ,repository_watchers ,repository_stargazers ,repository_forks ,repository_language ,created_at_weekday ,created_at_hour ,created_at_timestamp ,event_type , event_message  FROM github_events_hive WHERE created_at_year=2014 and created_at_month=2 and created_at_day=1;

//query
select actor_login , created_at_hour , count(*) from github_events_hive_daily group by actor_login, created_at_hour;
select actor_login , created_at_weekday , count(*) from github_events_hive_daily group by actor_login, created_at_weekday;
select actor_login , event_type , count(*) from github_events_hive_daily group by actor_login, event_type;
select actor_login , repository_language , count(*) from github_events_hive_daily group by actor_login, repository_language;
select actor_login , repository_id , count(*) from github_events_hive_daily group by actor_login, repository_id;


// top 20 most active user
select actor_login , count(*) as events from github_events_hive_daily group by actor_login sort by events desc limit 20;

































----------------------ignore this ----------------------------------

CREATE EXTERNAL TABLE  test_hive (rowkey STRING,actor_login STRING,actor_name STRING,repository_id STRING,repository_name STRING,repository_url STRING,repository_watchers INT,repository_stargazers INT,repository_forks INT,repository_language STRING,created_at_year INT,created_at_month INT,created_at_day INT,created_at_weekday INT,created_at_hour INT,created_at_timestamp TIMESTAMP,event_type STRING, event_message STRING)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,actor:login,actor:name,repository:id,repository:name,repository:url,repository:watchers,repository:stargazers,repository:forks,repository:language,created_at:year,created_at:month,created_at:day,created_at:weekday,created_at:hour,created_at:timestamp,event:type,event:message') 
TBLPROPERTIES ('hbase.table.name' = 'test'); 


CREATE  TABLE  test_hive_daily (rowkey STRING,actor_login STRING,actor_name STRING,repository_id STRING,repository_name STRING,repository_url STRING,repository_watchers INT,repository_stargazers INT,repository_forks INT,repository_language STRING,created_at_weekday INT,created_at_hour INT,created_at_timestamp TIMESTAMP,event_type STRING, event_message STRING)
PARTITIONED by (created_at_year INT,created_at_month INT,created_at_day INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';


INSERT OVERWRITE TABLE test_hive_daily PARTITION (created_at_year=2014,created_at_month=2,created_at_day=1)
SELECT rowkey ,actor_login ,actor_name ,repository_id ,repository_name ,repository_url ,repository_watchers ,repository_stargazers ,repository_forks ,repository_language ,created_at_weekday ,created_at_hour ,created_at_timestamp ,event_type , event_message  FROM test_hive WHERE created_at_year=2014 and created_at_month=2 and created_at_day=1;

select actor_login , created_at_hour , count(*) from test_hive_daily group by actor_login, created_at_hour;
select actor_login , created_at_weekday , count(*) from test_hive_daily group by actor_login, created_at_weekday;
select actor_login , event_type , count(*) from test_hive_daily group by actor_login, event_type;
select actor_login , repository_language , count(*) from test_hive_daily group by actor_login, repository_language;
select actor_login , repository_id , count(*) from test_hive_daily group by actor_login, repository_id;



select actor_login , count(*) as events from test_hive_daily group by actor_login sort by events desc limit 20;