# kafkastreamsapp
1. Download and unzip datasets from https://epam.sharepoint.com/sites/OrgCompetencyBigDataCoordinatorsUS/Big%20Data%20Competency%20Center/Forms/AllItems.aspx?id=%2Fsites%2FOrgCompetencyBigDataCoordinatorsUS%2FBig%20Data%20Competency%20Center%2FBig%20Data%20Trainings%2F2019%20BD%20Training%2F201%20HW%20Dataset&p=true&originalPath=aHR0cHM6Ly9lcGFtLnNoYXJlcG9pbnQuY29tLzpmOi9zL09yZ0NvbXBldGVuY3lCaWdEYXRhQ29vcmRpbmF0b3JzVVMvRWtaSFZ5TGd6ODVIaXhQSjFvNk5weXNCZG1DYjdrYmVaTENIYUF5Y2drQUxIdz9ydGltZT1EaE1hNTV5SDEwZw
2. Download sandbox-hdp container from...
3. Get bash console in hdp container : docker exec -it sandbox-hdp bash
4. Start hive via beeline command.
5. Create weather table from parquet files: 
CREATE EXTERNAL TABLE weather (lng double,lat double,avg_tmpr_f double,avg_tmpr_c double,wthr_date string) /
STORED AS PARQUET LOCATION 'hdfs://sandbox-hdp.hortonworks.com:8020/201db/dataset/weather/';
6. Download and add kafka handler:
ADD JAR /usr/hdp/3.0.1.0-187/hive/kafka-handler-3.1.0.3.1.0.6-1.jar
7. Create table for for kafka-connect:
CREATE EXTERNAL TABLE weather_kafka (lng double,lat double,avg_tmpr_f double,avg_tmpr_c double,wthr_date string) / 
STORED BY 'org.apache.hadoop.hive.kafka.KafkaStorageHandler' /
TBLPROPERTIES ("kafka.topic" = "weather-topic", "kafka.bootstrap.servers" = "sandbox-hdp.hortonworks.com:6667");
8. Load data form hive table to kafka topic:
INSERT INTO TABLE weather_kafka SELECT `lng`, `lat`, `avg_tmpr_f`,`avg_tmpr_c`, `wthr_date`, null as `__key`, /
null as `__partition`, -1 as `__offset`, null as `__timestamp` from weather;
9. Build maven project from pom.xml.
10. Create jar with dependencies with assembly plugin.
11. copy created jar file to docker hdp container: docker cp /path/to/jar/ sandbox-hdp:/home
12. check that data exists in topic "weather-topic": cd /usr/hdp/3.0.1.0-187/kafka/bin/ && /
sh kafka-console-consumer.sh --zookeeper sandbox-hdp.hortonworks.com:2181 --topic weather-topic --from-beginning
13. run jar file: java -cp jar_name.jar WeatherTestStream(if java is not installed in hdp run command java apt install default-jdk).
14. check that data exists in the new topic: 
cd /usr/hdp/3.0.1.0-187/kafka/bin/ && sh kafka-console-consumer.sh --zookeeper sandbox-hdp.hortonworks.com:2181 --topic weather-topic-output --from-beginning
