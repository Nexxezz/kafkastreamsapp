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
