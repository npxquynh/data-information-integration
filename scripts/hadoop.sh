su - hduser

# Note: remember to change the ip in '/etc/hosts' for 'hdnode01'
# Start the HDFS
$HADOOP_HOME/bin/start-all.sh

# Check if Hadoop is correct started

# Start HBase
$HBASE_HOME/bin/start-hbase.sh


