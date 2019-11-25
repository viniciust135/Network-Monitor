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

            #Proto
            try:
                df = df.na.drop(subset=["conn.proto"])
                dfProto = df.select(["conn.proto"])
                dfProtoAllFinal = dfProto.groupBy(["proto"]).count()
                #Proto count with timestamp
                dfPrTs = df.select(["conn.ts","conn.proto"])
                dfProtoTs = dfPrTs.withColumn("date", from_unixtime(dfPrTs['ts']))
                dfProtoHour = dfProtoTs.select('proto', date_trunc("Hour", "date").alias("hour"))
                dfProtoHourFinal = dfProtoHour.groupBy(["proto","hour"]).count()
                dfProtoDay = dfProtoTs.select('proto', date_trunc("day", "date").alias("day"))
                dfProtoDayFinal = dfProtoDay.groupBy(["proto","day"]).count()
                dfProtoAllFinal.show()
                dfProtoHourFinal.show()
                dfProtoDayFinal.show()
            except:
                print("Erro no df de proto")
                #pass
            
            #Service
            try:
                df = df.na.drop(subset=["conn.service"])
                dfService = df.select(["conn.service"])
                dfServiceTs = df.select(["conn.service", "conn.ts"])
                dfServiceDate = dfServiceTs.withColumn('date', from_unixtime(dfServiceTs['ts']))
                dfServiceHour = dfServiceDate.select("service", date_trunc("Hour", "date").alias("hour"))
                dfServiceHourFinal = dfServiceHour.groupBy(["service","hour"]).count()
                dfServiceDay = dfServiceDate.select('service', date_trunc("day", "date").alias("day"))
                dfServiceDayFinal = dfServiceDay.groupBy(["service","day"]).count()
                dfServiceHourFinal.show()
                dfServiceDayFinal.show()
            except:
                print("Erro no df de service")
                #pass

            #IP orig
            try:
                df = df.na.drop(subset=["conn.`id.orig_h`"])
                dfIpOrigTs = df.select(["conn.ts","conn.`id.orig_h`"])
                dfIpOrigTsRenamed = dfIpOrigTs.withColumnRenamed("id.orig_h", "orig_h")
                dfIpOrigDate = dfIpOrigTsRenamed.withColumn("date", from_unixtime(dfIpOrigTsRenamed['ts']))

                dfIpOrigHour = dfIpOrigDate.select('orig_h', date_trunc("Hour", "date").alias("hour"))
                dfIpOrigHourFinal = dfIpOrigHour.groupBy(["orig_h","hour"]).count()
                dfIpOrigHourFinal.show()

                dfIpOrigDay = dfIpOrigDate.select('orig_h', date_trunc("day", "date").alias("day"))
                dfIpOrigDayFinal = dfIpOrigDay.groupBy(["orig_h","day"]).count()
                dfIpOrigDayFinal.show()
            except:
                print("Erro no df de Orig_h")
                #pass
            
            #IP resp
            try:
                df = df.na.drop(subset=["conn.`id.resp_h`"])
                dfIpRespTs = df.select(["conn.ts","conn.`id.resp_h`"])
                dfIpRespTsRenamed = dfIpRespTs.withColumnRenamed("id.resp_h", "resp_h")
                dfIpRespDate = dfIpRespTsRenamed.withColumn("date", from_unixtime(dfIpRespTsRenamed['ts']))

                dfIpRespHour = dfIpRespDate.select('resp_h', date_trunc("Hour", "date").alias("hour"))
                dfIpRespHourFinal = dfIpRespHour.groupBy(["resp_h","hour"]).count()
                dfIpRespHourFinal.show()

                dfIpRespDay = dfIpRespDate.select('resp_h', date_trunc("day", "date").alias("day"))
                dfIpRespDayFinal = dfIpRespDay.groupBy(["resp_h","day"]).count()
                dfIpRespDayFinal.show()
            except:
                print("Erro no df de resp_h")
                #pass

            #port_resp

            try:
                df = df.na.drop(subset=["conn.`id.resp_p`"])
                dfPortRespTs = df.select(["conn.ts","conn.`id.resp_p`"])
                dfPortRespTsRenamed = dfPortRespTs.withColumnRenamed("id.resp_p", "resp_p")
                dfPortRespDate = dfPortRespTsRenamed.withColumn("date", from_unixtime(dfPortRespTsRenamed['ts']))

                dfPortRespHour = dfPortRespDate.select('resp_p', date_trunc("Hour", "date").alias("hour"))
                dfPortRespHourFinal = dfPortRespHour.groupBy(["resp_p","hour"]).count()
                dfPortRespHourFinal.show()

                dfPortRespDay = dfPortRespDate.select('resp_p', date_trunc("day", "date").alias("day"))
                dfPortRespDayFinal = dfPortRespDay.groupBy(["resp_p","day"]).count()
                dfPortRespDayFinal.show()
            except:
                print("Erro no df de resp_p")
                #pass

            #port_orig
            try:
                df = df.na.drop(subset=["conn.`id.orig_p`"])
                dfPortOrigTs = df.select(["conn.ts","conn.`id.orig_p`"])
                dfPortOrigTsRenamed = dfPortOrigTs.withColumnRenamed("id.orig_p", "orig_p")
                dfPortOrigDate = dfPortOrigTsRenamed.withColumn("date", from_unixtime(dfPortOrigTsRenamed['ts']))

                dfPortOrigHour = dfPortOrigDate.select('orig_p', date_trunc("Hour", "date").alias("hour"))
                dfPortOrigHourFinal = dfPortOrigHour.groupBy(["orig_p","hour"]).count()
                dfPortOrigHourFinal.show()

                dfPortOrigDay = dfPortOrigDate.select('orig_p', date_trunc("day", "date").alias("day"))
                dfPortOrigDayFinal = dfPortOrigDay.groupBy(["orig_p","day"]).count()
                dfPortOrigDayFinal.show()
            except:
                print("Erro no df de orig_p")
                #pass

            #Flow
            try:
                dfFlow = df.groupBy(["conn.`id.orig_h`","conn.`id.orig_p`","conn.`id.resp_h`", "conn.`id.resp_p`", "conn.proto"]).count()
                #Flow with timestamp
                dfFlowTs = df.select(["conn.ts","conn.`id.orig_h`","conn.`id.orig_p`","conn.`id.resp_h`", "conn.`id.resp_p`", "conn.proto"])
                dfFlowDate = dfFlowTs.withColumn("date", from_unixtime(dfFlowTs['ts']))
                #Flow per hour
                dfFlowHourWithTsDate = dfFlowDate.withColumn("hour", date_trunc("Hour", "date"))
                dfFlowHour = dfFlowHourWithTsDate.drop("ts","date")
                dfFlowHourFinal = dfFlowHour.groupBy(["hour","`id.orig_h`","`id.orig_p`","`id.resp_h`", "`id.resp_p`", "proto"]).count()
                dfFlowHourFinal.show()
                #Flow per day
                dfFlowDayWithTsDate = dfFlowDate.withColumn("day", date_trunc("day", "date"))
                dfFlowDay = dfFlowDayWithTsDate.drop("ts","date")
                dfFlowDayFinal = dfFlowDay.groupBy(["day","`id.orig_h`","`id.orig_p`","`id.resp_h`", "`id.resp_p`", "proto"]).count()
                dfFlowDayFinal.show()
            except:
                print("Erro no df de Flow")
                #pass            

            #dfProtoDayFinal.write\
            #    .format("org.apache.spark.sql.cassandra")\
            #    .mode('append')\
            #    .options(table="proto3_count_day", keyspace="network")\
            #    .save()  

            #dfProtoHourFinal.write\
            #    .format("org.apache.spark.sql.cassandra")\
            #    .mode('append')\
            #    .options(table="proto3_count_hour", keyspace="network")\
            #    .save() 
            
            
        
            #df3.write\
            #    .format("org.apache.spark.sql.cassandra")\
            #    .mode('append')\
            #    .options(table="conn_proto_count2", keyspace="network")\
            #    .save()           
            #df2.write\
            #    .format("org.apache.spark.sql.cassandra")\
            #    .mode('append')\
            #    .options(table="conn_service_proto_count", keyspace="network")\
            #    .save()
try:
    sc = SparkContext(appName="PythonSparkStreamingKafka")
    #sc.setLogLevel("OFF")
    sc.setLogLevel("WARN")
    spark = SparkSession(sc)
    ssc = StreamingContext(sc,5)
except:
    print("deu ruim 1")

try:
    kafkaStream = KafkaUtils.createStream(ssc, 'zookeeper:2181', 'my-group', {'zeek':1})
    lines = kafkaStream.map(lambda x: x[1])
except:
    print("deu ruim 2")

lines.foreachRDD(saveOnCassandra)
#lines.pprint()

try:
    ssc.start()
    ssc.awaitTermination()
except:
    print("deu ruim 3")