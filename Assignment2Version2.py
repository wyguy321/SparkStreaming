#    Spark
from pyspark import SparkContext
from pyspark.sql import SparkSession
#    Spark Streaming
from pyspark.streaming import StreamingContext
#    Kafka
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType,FloatType
#    json parsing
import json
import os
import random
import uuid
#import org.apache.spark.sql.SaveMode

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0,com.datastax.spark:spark-cassandra-connector_2.11:2.3.0 --conf spark.cassandra.connection.host=172.31.2.176 pyspark-shell'

def load_and_get_table_df(keys_space_name, table_name):
	table_df = sqlContext.read\
		.format("org.apache.spark.sql.cassandra")\
		.options(table=table_name, keyspace=keys_space_name)\
		.load()
	return table_df


def handler(message):
	records = message.collect()
	for record in records:
####QUESTIONS 2.1,2.2,2.3
	# 	print(record[1])
	# 	data=record[1]
	# 	dataRecords = data.split(",")
	# 	print("RECORD 1")
	# 	print(dataRecords[0])
	# 	print("RECORD 2")
	# 	print(dataRecords[1])
	# 	print("RECORD 3")
	# 	print(dataRecords[2])
	# 	r1 = random.randint(1, 100)
	# 	schema = StructType([ \
	# StructField("flightid",IntegerType(),True), \
	# StructField("airline",StringType(),True), \
	# StructField("origin",StringType(),True),StructField("averagedelay",FloatType(),True)])

	# 	#df = spark.createDataFrame(data=data2,schema=schema)
	# 	data2 = [(r1,str(dataRecords[0]),str(dataRecords[2]),float(dataRecords[1]))]
	# 	df = spark.createDataFrame(data=data2,schema=schema)
	# 	df.show(truncate=False)
	# 	df.write.format("org.apache.spark.sql.cassandra").mode('append').options(table='questiontwoone', keyspace='question2').save()
	####QUESTIONS 3.2
	# 	print(record[1])
	# 	data=record[1]
	# 	dataRecords = data.split(",")
	# 	print("RECORD 1")
	# 	print(dataRecords[0])
	# 	print("RECORD 2")
	# 	print(dataRecords[1])
	# 	print("RECORD 3")
	# 	print(dataRecords[2])
	# 	print("RECORD 4")
	# 	print(dataRecords[3])
	# 	uuidVar = uuid.uuid4()
	# 	schema = StructType([ \
	# StructField("uid",StringType(),True), \
	# StructField("arrivaldelay",IntegerType(),True), \
	# StructField("dest",StringType(),True),StructField("origin",StringType(),True),StructField("scheduledceparture",StringType(),True)])

	# 	#df = spark.createDataFrame(data=data2,schema=schema)
	# 	data2 = [(uuidVar,str(dataRecords[0]),str(dataRecords[1]),str(dataRecords[2],str(dataRecords[3]))]
	# 	df = spark.createDataFrame(data=data2,schema=schema)
	# 	df.show(truncate=False)
	# 	df.write.format("org.apache.spark.sql.cassandra").mode('append').options(table='questionthreetwo', keyspace='question3').save()





	#	print(type(record[1]))

sc = SparkContext(appName="PythonSparkStreamingKafka_RM_01")
sc.setLogLevel("WARN")

ssc = StreamingContext(sc, 1)

spark = SparkSession.builder.appName('HW2Streaming').getOrCreate()


sqlContext = SQLContext(sc)


kafka_servers = "XXXXX.c2.kafka.us-east-1.amazonaws.com:9092,XXXXX.c2.kafka.us-east-1.amazonaws.com:9092"





stream = KafkaUtils.createDirectStream(ssc, topicsList, {
						'bootstrap.servers':kafka_servers, 
						'group.id':'test', 
						'fetch.message.max.bytes':'15728640',
						'auto.offset.reset':'largest'})
parsed = stream.map(lambda v: v[1])

stream.foreachRDD(handler)


ssc.start()
ssc.awaitTermination()

