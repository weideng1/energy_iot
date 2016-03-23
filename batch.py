from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import datetime
from pyspark.sql import functions as func
from pyspark.sql.functions import col
from pyspark.sql.functions import lit

#prepare the spark context
conf = SparkConf().setAppName("Stand Alone Python Script")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

#set time variables for date filtering
time = datetime.datetime.now()
epochtime = int(time.strftime("%s"))
start_time = epochtime - 86400
compare_time = datetime.datetime.fromtimestamp(start_time)

#create a dataframe from the raw metrics
rawmetrics = sqlContext.read.format("org.apache.spark.sql.cassandra").options(table="raw_metrics", keyspace="metrics").load()

#filter metrics to those in last 24 hours
last_day = rawmetrics.where(rawmetrics.metric_time > compare_time)

#aggregates
averages = last_day.groupby('device_id').agg(func.avg('metric_value').alias('metric_avg'))
maximums = last_day.groupby('device_id').agg(func.max('metric_value').alias('metric_max'))
minimums = last_day.groupby('device_id').agg(func.min('metric_value').alias('metric_min'))

#rename id columns for uniqueness
averages_a = averages.withColumnRenamed("device_id", "id")
maximums_a = maximums.withColumnRenamed("device_id", "maxid")
minimums_a = minimums.withColumnRenamed("device_id", "minid")

#join the tables above
temp = averages_a.join(maximums_a, averages_a.id == maximums_a.maxid)
aggs = temp.join(minimums, temp.id == minimums.device_id).select('id','metric_min','metric_max','metric_avg')

#add columns to format for cassandra
addday = aggs.withColumn("metric_day", lit(time))
addname = addday.withColumn("metric_name",lit("KWH"))
inserts = addname.withColumnRenamed("id","device_id")

#write to cassandra
inserts.write.format("org.apache.spark.sql.cassandra").options(table="daily_rollups", keyspace = "metrics").save(mode ="append")
