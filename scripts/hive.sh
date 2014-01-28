CREATE EXTERNAL TABLE foo(k STRING, ab1 INT, ab2 INT)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,f1:c1,f1:c3")
TBLPROPERTIES ('hbase.table.name' = 't1');

SELECT * FROM foo WHERE ab2 = '3';
