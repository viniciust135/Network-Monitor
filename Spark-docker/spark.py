from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row, SparkSession
from pyspark.sql import SQLContext
import time

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
        df = df.na.drop(subset=["conn.proto"])
        #df3 = df.groupBy(["conn.proto"]).count().orderBy('count', ascending=False).cache()
        #df.printSchema()
        #df.show()
        #df.printSchema()
        #df = loads(df).decode('utf-8')
        #df['conn'].show()
        #row = Row("conn")
        #df = rdd.map(row).toDF()
        #print(dict)

        if 'conn' in df.columns:
            df3 = df.groupBy(["conn.proto"]).count().orderBy('count', ascending=False).cache()
            df3.show()
            #df2 = df.groupBy(["conn.service","conn.proto"]).count().orderBy('count', ascending=False).cache()
            #df2.show()
            #print(df.select("conn.proto").collect())
            #print(df.select("conn.proto").rdd.map(lambda r: r(0)).collect())
            df3.write\
                .format("org.apache.spark.sql.cassandra")\
                .mode('append')\
                .options(table="conn_proto_count2", keyspace="network")\
                .save()           
            #df2.write\
            #    .format("org.apache.spark.sql.cassandra")\
            #    .mode('append')\
            #    .options(table="conn_service_proto_count", keyspace="network")\
            #    .save()

sc = SparkContext(appName="PythonSparkStreamingKafka")
#sc.setLogLevel("OFF")
sc.setLogLevel("WARN")
spark = SparkSession(sc)
ssc = StreamingContext(sc,10)

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

