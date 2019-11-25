from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row, SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, expr, when
from datetime import datetime
from pyspark.sql.functions import from_unixtime, unix_timestamp, to_date, date_trunc
from pyspark.sql.types import DateType

def saveOnCassandra(rdd):
    if not rdd.isEmpty():

        df = spark.read.json(rdd)

        if 'conn' in df.columns:

            #--------- CREATE PROTO DF ---------
            try:
                df = df.na.drop(subset=["conn.proto"])
                dfProto = df.select(["conn.proto"])
                dfProtoAllFinal = dfProto.groupBy(["proto"]).count()
                dfPrTs = df.select(["conn.ts","conn.proto"])
                dfProtoTs = dfPrTs.withColumn("date", from_unixtime(dfPrTs['ts']))
                dfProtoHour = dfProtoTs.select('proto', date_trunc("Hour", "date").alias("hour"))
                dfProtoHourFinal = dfProtoHour.groupBy(["proto","hour"]).count()
                dfProtoDay = dfProtoTs.select('proto', date_trunc("day", "date").alias("day"))
                dfProtoDayFinal = dfProtoDay.groupBy(["proto","day"]).count()
                #dfProtoAllFinal.show()
                #dfProtoHourFinal.show()
                #dfProtoDayFinal.show()
            except Exception as e: 
                print(e)
   
            #-------------- SAVE PROTO --------------
            try:
                dfProtoAllFinal.write\
                    .format("org.apache.spark.sql.cassandra")\
                    .mode('append')\
                    .options(table="proto_all", keyspace="testing")\
                    .save()
            except Exception as e: 
                print(e)

            try:
                dfProtoHourFinal.write\
                    .format("org.apache.spark.sql.cassandra")\
                    .mode('append')\
                    .options(table="proto_hour", keyspace="testing")\
                    .save()
            except Exception as e: 
                print(e)

            try:
                dfProtoDayFinal.write\
                    .format("org.apache.spark.sql.cassandra")\
                    .mode('append')\
                    .options(table="proto_day", keyspace="testing")\
                    .save()
            except Exception as e: 
                print(e)


            #--------- CREATE SERVICE DF ---------
            try:
                df = df.na.drop(subset=["conn.service"])
                dfServiceTs = df.select(["conn.service", "conn.ts"])
                dfServiceDate = dfServiceTs.withColumn('date', from_unixtime(dfServiceTs['ts']))
                dfServiceHour = dfServiceDate.select("service", date_trunc("Hour", "date").alias("hour"))
                dfServiceHourFinal = dfServiceHour.groupBy(["service","hour"]).count()
                dfServiceDay = dfServiceDate.select('service', date_trunc("day", "date").alias("day"))
                dfServiceDayFinal = dfServiceDay.groupBy(["service","day"]).count()
                #dfServiceHourFinal.show()
                #dfServiceDayFinal.show()
            except Exception as e: 
                print(e)

            #-------------- SAVE SERVICE --------------
            try:
                dfServiceHourFinal.write\
                    .format("org.apache.spark.sql.cassandra")\
                    .mode('append')\
                    .options(table="service_hour", keyspace="testing")\
                    .save()
            except Exception as e: 
                print(e)

            try:
                dfServiceDayFinal.write\
                    .format("org.apache.spark.sql.cassandra")\
                    .mode('append')\
                    .options(table="service_day", keyspace="testing")\
                    .save()
            except Exception as e: 
                print(e)

            #--------- CREATE ORIG_H DF ---------
            try:
                df = df.na.drop(subset=["conn.`id.orig_h`"])
                dfIpOrigTs = df.select(["conn.ts","conn.`id.orig_h`"])
                dfIpOrigTsRenamed = dfIpOrigTs.withColumnRenamed("id.orig_h", "orig_h")
                dfIpOrigDate = dfIpOrigTsRenamed.withColumn("date", from_unixtime(dfIpOrigTsRenamed['ts']))
                dfIpOrigHour = dfIpOrigDate.select('orig_h', date_trunc("Hour", "date").alias("hour"))
                dfIpOrigHourFinal = dfIpOrigHour.groupBy(["orig_h","hour"]).count()
                #dfIpOrigHourFinal.show()
                dfIpOrigDay = dfIpOrigDate.select('orig_h', date_trunc("day", "date").alias("day"))
                dfIpOrigDayFinal = dfIpOrigDay.groupBy(["orig_h","day"]).count()
                #dfIpOrigDayFinal.show()
            except Exception as e: 
                print(e)

            #-------------- SAVE ORIG_H --------------
            try:
                dfIpOrigHourFinal.write\
                    .format("org.apache.spark.sql.cassandra")\
                    .mode('append')\
                    .options(table="orig_h_hour", keyspace="testing")\
                    .save()
            except Exception as e: 
                print(e)

            try:
                dfIpOrigDayFinal.write\
                    .format("org.apache.spark.sql.cassandra")\
                    .mode('append')\
                    .options(table="orig_h_day", keyspace="testing")\
                    .save()
            except Exception as e: 
                print(e)

            #--------- CREATE RESP_H DF ---------
            try:
                df = df.na.drop(subset=["conn.`id.resp_h`"])
                dfIpRespTs = df.select(["conn.ts","conn.`id.resp_h`"])
                dfIpRespTsRenamed = dfIpRespTs.withColumnRenamed("id.resp_h", "resp_h")
                dfIpRespDate = dfIpRespTsRenamed.withColumn("date", from_unixtime(dfIpRespTsRenamed['ts']))
                dfIpRespHour = dfIpRespDate.select('resp_h', date_trunc("Hour", "date").alias("hour"))
                dfIpRespHourFinal = dfIpRespHour.groupBy(["resp_h","hour"]).count()
                #dfIpRespHourFinal.show()
                dfIpRespDay = dfIpRespDate.select('resp_h', date_trunc("day", "date").alias("day"))
                dfIpRespDayFinal = dfIpRespDay.groupBy(["resp_h","day"]).count()
                #dfIpRespDayFinal.show()
            except Exception as e: 
                print(e)

            #-------------- SAVE RESP_H --------------
            try:
                dfIpRespHourFinal.write\
                    .format("org.apache.spark.sql.cassandra")\
                    .mode('append')\
                    .options(table="resp_h_hour", keyspace="testing")\
                    .save()
            except Exception as e: 
                print(e)

            try:
                dfIpRespDayFinal.write\
                    .format("org.apache.spark.sql.cassandra")\
                    .mode('append')\
                    .options(table="resp_h_day", keyspace="testing")\
                    .save()
            except Exception as e: 
                print(e)

            #--------- CREATE RESP_P DF ---------
            try:
                df = df.na.drop(subset=["conn.`id.resp_p`"])
                dfPortRespTs = df.select(["conn.ts","conn.`id.resp_p`"])
                dfPortRespTsRenamed = dfPortRespTs.withColumnRenamed("id.resp_p", "resp_p")
                dfPortRespDate = dfPortRespTsRenamed.withColumn("date", from_unixtime(dfPortRespTsRenamed['ts']))
                dfPortRespHour = dfPortRespDate.select('resp_p', date_trunc("Hour", "date").alias("hour"))
                dfPortRespHourFinal = dfPortRespHour.groupBy(["resp_p","hour"]).count()
                #dfPortRespHourFinal.show()
                dfPortRespDay = dfPortRespDate.select('resp_p', date_trunc("day", "date").alias("day"))
                dfPortRespDayFinal = dfPortRespDay.groupBy(["resp_p","day"]).count()
                #dfPortRespDayFinal.show()
            except Exception as e: 
                print(e)

            #-------------- SAVE RESP_P --------------
            try:
                dfPortRespHourFinal.write\
                    .format("org.apache.spark.sql.cassandra")\
                    .mode('append')\
                    .options(table="resp_p_hour", keyspace="testing")\
                    .save()
            except Exception as e: 
                print(e)

            try:
                dfPortRespDayFinal.write\
                    .format("org.apache.spark.sql.cassandra")\
                    .mode('append')\
                    .options(table="resp_p_day", keyspace="testing")\
                    .save()
            except Exception as e: 
                print(e)

            #--------- CREATE ORIG_P DF ---------
            try:
                df = df.na.drop(subset=["conn.`id.orig_p`"])
                dfPortOrigTs = df.select(["conn.ts","conn.`id.orig_p`"])
                dfPortOrigTsRenamed = dfPortOrigTs.withColumnRenamed("id.orig_p", "orig_p")
                dfPortOrigDate = dfPortOrigTsRenamed.withColumn("date", from_unixtime(dfPortOrigTsRenamed['ts']))
                dfPortOrigHour = dfPortOrigDate.select('orig_p', date_trunc("Hour", "date").alias("hour"))
                dfPortOrigHourFinal = dfPortOrigHour.groupBy(["orig_p","hour"]).count()
                #dfPortOrigHourFinal.show()
                dfPortOrigDay = dfPortOrigDate.select('orig_p', date_trunc("day", "date").alias("day"))
                dfPortOrigDayFinal = dfPortOrigDay.groupBy(["orig_p","day"]).count()
                #dfPortOrigDayFinal.show()
            except Exception as e: 
                print(e)

            #-------------- SAVE ORIG_P --------------
            try:
                dfPortOrigHourFinal.write\
                    .format("org.apache.spark.sql.cassandra")\
                    .mode('append')\
                    .options(table="orig_p_hour", keyspace="testing")\
                    .save()
            except Exception as e: 
                print(e)

            try:
                dfPortOrigDayFinal.write\
                    .format("org.apache.spark.sql.cassandra")\
                    .mode('append')\
                    .options(table="orig_p_day", keyspace="testing")\
                    .save()
            except Exception as e: 
                print(e)


            #--------- CREATE FLOW DF ---------
            try:
                dfFlowWrongNames = df.select(["conn.ts","conn.`id.orig_h`","conn.`id.orig_p`","conn.`id.resp_h`", "conn.`id.resp_p`", "conn.proto"])
                dfFlowTs = dfFlowWrongNames.select(col("ts"), col("`id.orig_h`").alias("orig_h"), col("`id.orig_p`").alias("orig_p"), col("`id.resp_h`").alias("resp_h"), col("`id.resp_p`").alias("resp_p"),col("proto"))
                dfFlowDate = dfFlowTs.withColumn("date", from_unixtime(dfFlowTs['ts']))
                dfFlowHourWithTsDate = dfFlowDate.withColumn("hour", date_trunc("Hour", "date"))
                dfFlowHour = dfFlowHourWithTsDate.drop("ts","date")
                dfFlowHourFinal = dfFlowHour.groupBy(["hour","orig_h","orig_p","resp_h", "resp_p", "proto"]).count()
                #dfFlowHourFinal.show()
                dfFlowDayWithTsDate = dfFlowDate.withColumn("day", date_trunc("day", "date"))
                dfFlowDay = dfFlowDayWithTsDate.drop("ts","date")
                dfFlowDayFinal = dfFlowDay.groupBy(["day","orig_h","orig_p","resp_h", "resp_p", "proto"]).count()
                #dfFlowDayFinal.show()
            except Exception as e: 
                print(e)

            #-------------- SAVE FLOW --------------
            try:
                dfFlowHourFinal.write\
                    .format("org.apache.spark.sql.cassandra")\
                    .mode('append')\
                    .options(table="flow_hour", keyspace="testing")\
                    .save()
            except Exception as e: 
                print(e)

            try:
                dfFlowDayFinal.write\
                    .format("org.apache.spark.sql.cassandra")\
                    .mode('append')\
                    .options(table="flow_day", keyspace="testing")\
                    .save()
            except Exception as e: 
                print(e)


try:
    sc = SparkContext(appName="PythonSparkStreamingKafka")
    #sc.setLogLevel("OFF")
    sc.setLogLevel("WARN")
    spark = SparkSession(sc)
    ssc = StreamingContext(sc,5)
    kafkaStream = KafkaUtils.createStream(ssc, 'zookeeper:2181', 'my-group', {'zeek':1})
    lines = kafkaStream.map(lambda x: x[1])
    lines.foreachRDD(saveOnCassandra)
    #lines.pprint()
    ssc.start()
    ssc.awaitTermination()
except Exception as e: 
    print(e)