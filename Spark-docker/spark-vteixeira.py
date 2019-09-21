from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row, SparkSession
#from cassandra_helper import *
import time
#from json import loads

def savetheresult(rdd):
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
        df.show()
        df2 = df.groupBy(["orig_h", "resp_h", "resp_p", "proto"]).count().orderBy('count', ascending=False).cache()
        df2.show()
        #df.show()
        #df.printSchema()

        #df = loads(df).decode('utf-8')
        #df['conn'].show()
        #row = Row("conn")
        #df = rdd.map(row).toDF()
        #print(dict)

        #if 'conn' in df.columns:
            #print(df.select("conn.proto").collect())
            #print(df.select("conn.proto").rdd.map(lambda r: r(0)).collect())
            #df.schema()
            #df['conn'].update(identification = time.time())
            #insert_connection(df['conn'])
            #print("inseriu no banco")
        df2.write\
            .format("org.apache.spark.sql.cassandra")\
            .options(table="conn_by_ip_total", keyspace="network")\
            .mode("append")\
            .save()
            #df.write\
            #    .format("org.apache.spark.sql.cassandra")\
            #    .mode('append')\
            #    .options(table="Connection", keyspace="network")\
            #    .save()

sc = SparkContext(appName="PythonSparkStreamingKafka")
#sc.setLogLevel("OFF")
sc.setLogLevel("WARN")
spark = SparkSession(sc)
ssc = StreamingContext(sc,4)

clusterIPs = ['172.18.0.2']
ConnectDB( clusterIPs )
print("Starting connection to db")

kafkaStream = KafkaUtils.createStream(ssc, 'zookeeper:2181', 'my-group', {'zeek':1})
lines = kafkaStream.map(lambda x: x[1])

#js = lines.map(lambda x: loads(x.decode('utf-8')))
#js.pprint()

lines.foreachRDD(savetheresult)
#lines.pprint()

ssc.start()
ssc.awaitTermination()

