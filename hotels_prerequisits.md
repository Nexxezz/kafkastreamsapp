You should put hotels csv data to HDFS /tmp/dataset/hotels folder.
1. Start streamsets data collector container:
docker run --restart on-failure -p 18630:18630 -d --name streamsets-dc streamsets/datacollector
2. Add streamsets container to hdp network:
docker network connect cda streamsets-dc
3. open streamsets ui in browser:
sandbox-hdp.hortonworks.com:18630(log/pass : admin)
4. Download pipeline.zip from DATAFLOW homework
5. Select "Import Pipeline From Archive" in the blue selector in "Pipelines" tab:)
6. Open downloaded pipeline and install hdp and kafka library(Apache Kafka 2.0.0, HDP 3.1.0) via package manager(gift icon on the panel in the right corner)
7. Update jar for sample processor: Package Manager -> External Libraries -> click on jar checkbox ->click on "Install External Libraries"(right corner) -> provide path to jar file
8. restart streamsets container.
9. Start pipeline. Data from csv should be stored in "hotels-topic" topic.

