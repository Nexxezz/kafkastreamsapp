# kafkastreamsapp
## Prerequisites
* Kafka topic prepared according to 
https://github.com/Nexxezz/bigdata-homework4/blob/master/README.md

## Task 2
* Build maven project from pom.xml
```bash
mvn package
```
* Create jar with dependencies with assembly plugin
```bash
mvn assembly:single
```
* copy created jar file to docker hdp container: docker cp /path/to/jar/ sandbox-hdp:/home
* check that data exists in topic "weather-topic": cd /usr/hdp/3.0.1.0-187/kafka/bin/ && /
sh kafka-console-consumer.sh --zookeeper sandbox-hdp.hortonworks.com:2181 --topic weather-topic --from-beginning
* run jar file: java -cp jar_name.jar WeatherTestStream(if java is not installed in hdp run command java apt install default-jdk).
* check that data exists in the new topic: 
```bash
cd /usr/hdp/3.0.1.0-187/kafka/bin/ && sh kafka-console-consumer.sh --zookeeper sandbox-hdp.hortonworks.com:2181 --topic weather-topic-output --from-beginning
```





### TopicSaver2 usage
```
mvn clean package && docker cp target/kafkastreamsapp-1.0-SNAPSHOT-jar-with-dependencies.jar sandbox-hdp:/home/apps && echo OK
```
go to hdp:
```
docker exec -ti sandbox-hpd bash
```

from within hdp:
```
#delete old data:``
hdfs dfs -ls /tmp/test

#set variables
KAFKA_HOME=/usr/hdp/3.0.1.0-187/kafka
SPARK_HOME=/usr/hdp/3.0.1.0-187/spark2
APP_PATH=/home/apps/kafkastreamsapp-1.0-SNAPSHOT-jar-with-dependencies.jar:$KAFKA_HOME/libs/*:$SPARK_HOME/jars/*

#run the app
java -cp $APP_PATH spark.TopicSaver2 
#or
java -cp $APP_PATH spark.TopicSaver2 <TOPIC> <PATH> <LIMIT>

#for example:
java -cp $APP_PATH spark.TopicSaver spark.TopicSaver2 weather-topic hdfs://sandbox-hdp.hortonworks.com:8020/tmp/test 10000
```
go to HDFS and check data in parquet:
```
hdfs dfs -ls /tmp/test
```