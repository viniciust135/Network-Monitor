# Network Monitor

This resposity describes Docker images for network moniting and storage on Apache Cassandra.

## Running all containers

The containers are not able to monitor automatically. First, create them and follow the steps below.
```
docker-compose up -d
```

## Running Zeek monitor 

First option is to monitor a physical network interface running:
```
docker exec -ti zeek zeekctl deploy
```

If you need to load a PCAP file into Zeek:
```
docker exec -ti zeek zeek -r maccdc2012_00000.pcap local.zeek
```
Assuming that the PCAP file is inside the container. If not, you need to map the file by Docker volumes.

If Zeek container gives an error, verify if the network interface is the same that is configured in ```node.cfg```, else change the interface in this file.


## Read logs into Cassandra

Zeek produces network logs into Kafka. The script ```consumer.py``` is able to consume Kafka and insert data into Cassandra database. The requirements of this script are:
- kafka-python 1.4.6
- cassandra-driver 3.19.0
You can create a Python virtualenv:
```
virtualenv -p python3 .
source bin/activate
pip install -r requirements.txt
```

First, you need to discover Cassandra IP:
```
docker exec -ti cassandra-n01 ip a
```
Modify the script with this IP and run. You should see all messages on the terminal.


## Images 
Images used in project:

- Cassandra: https://hub.docker.com/_/cassandra
- Kafka: https://hub.docker.com/r/wurstmeister/kafka/
- ZooKeepeer: https://hub.docker.com/r/wurstmeister/zookeeper/
- Zeek with connection plugin to Kafka: https://hub.docker.com/r/thiagosordi/zeek
- Spark Streaming (Kafka Consumer): https://hub.docker.com/r/thiagosordi/spark-streaming

## Run Spark Streaming (TODO)
With all container running, now run the Spark Streaming container:

```sudo docker exec -ti spark-streaming bash```

After:

```/usr/local/spark/bin/spark-submit  --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.0 spark.py```


They are auto installed in Spark Streaming image, but:
Python package to use Spark (using ```pip install```):
- pyspark (2.4.4)

This project is not finished

