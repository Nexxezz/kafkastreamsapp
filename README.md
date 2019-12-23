# kafkastreamsapp
1. Build maven project from pom.xml.
2. Create jar with dependencies with assembly plugin.
3. copy created jar file to docker hdp container: docker cp /path/to/jar/ sandbox-hdp:/home
4. check that data exists in topic "weather-topic": cd /usr/hdp/3.0.1.0-187/kafka/bin/ && /
sh kafka-console-consumer.sh --zookeeper sandbox-hdp.hortonworks.com:2181 --topic weather-topic --from-beginning
4. run jar file: java -cp jar_name.jar WeatherTestStream(if java is not installed in hdp run command java apt install default-jdk).
5. check that data exists in the new topic: 
cd /usr/hdp/3.0.1.0-187/kafka/bin/ && sh kafka-console-consumer.sh --zookeeper sandbox-hdp.hortonworks.com:2181 --topic weather-topic-output --from-beginning
