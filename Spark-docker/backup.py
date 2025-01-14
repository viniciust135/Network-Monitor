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
        #js = rdd.map(lambda x: json.loads(x))
        #a=rdd.collect()
        #print(a)
        #dict = rdd.collectAsMap()
        #rddString = str(rdd.collect())
        #print(rddString)
        #dict = loads(rddString)
        #print(dict)
        #rdd = rddR.map(.value.toString)

        df = spark.read.json(rdd)
        #df = 
        #df = df
        #print(df.select("conn.ts").collect())
        #df.select(to_date(col('conn.ts')).alias('ts').cast("date")).show(10,False)
        #df = df.withColumn('conn.date', df['conn.ts'].cast('date'))

        #df1 = df.columns = ["conn"]
        #df.printSchema()
        #df2 = df.select()
        #print(df.select("conn.id.orig_h").alias("orig_h").collect())
        #df3 = df.groupBy(["conn.proto"]).count().orderBy('count', ascending=False).cache()
        #df.printSchema()
        #df.show()
        #df.printSchema()
        #df = loads(df).decode('utf-8')
        #df['conn'].show()
        #row = Row("conn")
        #df = rdd.map(row).toDF()
        #print(dict)
        #print(df.columns['conn'])

        if 'conn' in df.columns:
            '''
            #Proto
            try:
                df = df.na.drop(subset=["conn.proto"])
                dfProto = df.select(["conn.proto"])
                dfProtoAllFinal = dfProto.groupBy(["proto"]).count().show()
                #Proto count with timestamp
                dfPrTs = df.select(["conn.ts","conn.proto"])
                dfProtoTs = dfPrTs.withColumn("date", from_unixtime(dfPrTs['ts']))
                dfProtoHour = dfProtoTs.select('proto', date_trunc("Hour", "date").alias("hour"))
                dfProtoHourFinal = dfProtoHour.groupBy(["proto","hour"]).count()
                dfProtoDay = dfProtoTs.select('proto', date_trunc("day", "date").alias("day"))
                dfProtoDayFinal = dfProtoDay.groupBy(["proto","day"]).count()
            except:
                print("Erro no df de proto")
                pass
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
                pass

            try:
                #Flow
                dfFlow = df.groupBy(["conn.`id.orig_h`","conn.`id.orig_p`","conn.`id.resp_h`", "conn.`id.resp_p`", "conn.proto"]).count()
                #Flow with timestamp
                dfFlowTs = df.select(["conn.ts","conn.`id.orig_h`","conn.`id.orig_p`","conn.`id.resp_h`", "conn.`id.resp_p`", "conn.proto"])
                dfFlowDate = dfFlowTs.withColumn("date", from_Orig_h`id.orig_h`","`id.orig_p`","`id.resp_h`", "`id.resp_p`", "proto"]).count()
                dfFlowHourFinal.show()
                #Flow per day
                dfFlowDayWithTsDate = dfFlowDate.withColumn("day", date_trunc("day", "date"))
                dfFlowDay = dfFlowDayWithTsDate.drop("ts","date")
                dfFlowDayFinal = dfFlowDay.groupBy(["day","`id.orig_h`","`id.orig_p`","`id.resp_h`", "`id.resp_p`", "proto"]).count()
                dfFlowDayFinal.show()
            except:
                print("Erro no df de Flow")
                pass
            '''

            
            #IP orig
            try:
                dfIpOrigTs = df.select(["conn.ts","conn.`id.orig_h`"])
                dfIpOrigTsRenamed = dfIpOrigTs.withColumnRenamed("id.orig_h", "orig_h")
                dfIpOrigDate = dfIpOrigTsRenamed.withColumn("date", from_unixtime(dfIpOrigTsRenamed['ts']))
                dfIpOrigHour = dfIpOrigDate.select('orig_h', date_trunc("Hour", "date").alias("hour"))
                dfIpOrigHourFinal = dfIpOrigHour.groupBy(["orig_h","hour"]).count()
                #dfIpOrigDay = dfIpOrigDate.select('orig_h', date_trunc("day", "date").alias("day"))
                #dfIpOrigDayFinal = dfIpOrigDay.groupBy(["orig_h","day"]).count()
                dfIpOrigHourFinal.show()
                #dfIpOrigDayFinal.show()
            except:
                print("Erro no df de Orig_h")
                pass

            
            #IP resp
            try:
                dfIpRespTs = df.select(["conn.ts","conn.`id.resp_h`"])
                dfIpRespTsRenamed = dfIpRespTs.withColumnRenamed("id.resp_h", "resp_h")
                dfIpRespDate = dfIpRespTsRenamed.withColumn("date", from_unixtime(dfIpRespTsRenamed['ts']))
                dfIpRespHour = dfIpRespDate.select('resp_h', date_trunc("Hour", "date").alias("hour"))
                dfIpRespHourFinal = dfIpRespHour.groupBy(["resp_h","hour"]).count()
                #dfIpRespDay = dfIpRespDate.select('orig_h', date_trunc("day", "date").alias("day"))
                #dfIpRespDayFinal = dfIpRespDay.groupBy(["orig_h","day"]).count()
                dfIpRespHourFinal.show()
                #dfIpRespDayFinal.show()
            except:
                print("Erro no df de resp_h")
                pass

            #df3 = df.select(["conn.ts","conn.`id.orig_p`"])
            #df33 = df3.withColumnRenamed("id.orig_p", "orig_p")
            #df1 = df.groupBy(["conn.ts","conn.proto"]).count().orderBy('count', ascending=False).cache()
            #df33.show()
            #Port orig
            #dfDatePort = df33.withColumn("date", from_unixtime(df2['ts']))
            #dfPortHour = dfDatePort.select('orig_p', date_trunc("Hour", "date").alias("hour"))
            #dfPortHourFinal = dfPortHour.groupBy(["orig_p","hour"]).count().orderBy('count', ascending=False).cache()
            #dfPortHourFinal.show()
            #dfOrigDay = dfDateOrig.select('orig_p', date_trunc("day", "date").alias("day"))
            #dfOrigDayFinal = dfOrigDay.groupBy(["orig_p","day"]).count().orderBy('count', ascending=False).cache()
            #dfOrigDayFinal.show()
            

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
            
            
            
            #df = df.withColumn('conn.date', df['conn.ts'].cast(DateType()))
            #df3 = df.groupBy(["conn.ts","conn.proto"]).count().orderBy('count', ascending=False).cache()
            #df3.show()
            #df = df['conn']
            #.withColumnRenamed("conn.conn_state", "conn.alala")
            #df = df.toDF
            #df.printSchema()
            #df.printSchema()
            
            #df2 = df.groupBy(["conn.service","conn.proto"]).count().orderBy('count', ascending=False).cache()
            #df2.show()
            #print(df.select("conn.proto").collect())
            #print(df.select("conn.proto").rdd.map(lambda r: r(0)).collect())
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

#clusterIPs = ['172.27.0.2']
#ConnectDB( clusterIPs )
#print("Starting connection to db")
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