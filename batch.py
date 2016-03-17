from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

conf = SparkConf().setAppName("Stand Alone Python Script")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

rawmetrics = sqlContext.read.format("org.apache.spark.sql.cassandra").options(table="raw_metrics", keyspace="metrics").load().show()



#table1.write.format("org.apache.spark.sql.cassandra").options(table="alt_raw_metrics", keyspace = "metrics").save(mode ="append")
