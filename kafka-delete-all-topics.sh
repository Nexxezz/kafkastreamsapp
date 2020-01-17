#!/bin/bash

TOPICS=$(sh $KAFKA_HOME/bin/kafka-topics.sh --zookeeper sandbox-hdp.hortonworks.com:2181 --list )

for T in $TOPICS
do
  if [ "$T" != "__consumer_offsets" ]; then
    sh $KAFKA_HOME/bin/kafka-topics.sh --zookeeper sandbox-hdp.hortonworks.com:2181 -delete --topic $T
  fi
done
