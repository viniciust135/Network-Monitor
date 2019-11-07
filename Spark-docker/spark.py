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
            df = df.na.drop(subset=["conn.proto"])
            df1 = df.select(["conn.ts","conn.proto"])
            #df1 = df.groupBy(["conn.ts","conn.proto"]).count().orderBy('count', ascending=False).cache()
            dfPrTs = df1.withColumn("date", from_unixtime(df1['ts']))
            dfProtoHour = dfPrTs.select('proto', date_trunc("Hour", "date").alias("hour"))
            dfProtoDay = dfPrTs.select('proto', date_trunc("day", "date").alias("day"))
            dfProtoDayFinal = dfProtoDay.groupBy(["proto","day"]).count().orderBy('count', ascending=False).cache().show()
            dfProtoHoraFinal = dfProtoHour.groupBy(["proto","hour"]).count().orderBy('count', ascending=False).cache().show()
            
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

sc = SparkContext(appName="PythonSparkStreamingKafka")
#sc.setLogLevel("OFF")
sc.setLogLevel("WARN")
spark = SparkSession(sc)
ssc = StreamingContext(sc,5)

#clusterIPs = ['172.27.0.2']
#ConnectDB( clusterIPs )
#print("Starting connection to db")

kafkaStream = KafkaUtils.createStream(ssc, 'zookeeper:2181', 'my-group', {'zeek':1})
lines = kafkaStream.map(lambda x: x[1])
lines.foreachRDD(saveOnCassandra)

#js = lines.map(lambda x: loads(x.decode('utf-8')))
#js.pprint()

#lines.pprint()

ssc.start()
ssc.awaitTermination()