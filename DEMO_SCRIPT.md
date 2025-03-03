# Setup, once only please

Start from the root of the project.
Takes time so only do once.

```
./gradlew clean releaseTarGz
```

# Setup, before each demo run

Run this first before running the demo.

```
rm -r state-dir1
rm -r state-dir2
rm -r /tmp/kraft-combined-logs
mkdir state-dir1
mkdir state-dir2
```

# TAB1: Set up server
```
export KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/server.properties --standalone
bin/kafka-server-start.sh config/server.properties
```

# TAB2: Run streams application 1
```
bin/kafka-topics.sh --create --topic streams-plaintext-input --partitions 2 --replication-factor 1 --bootstrap-server localhost:9092
bin/kafka-run-class.sh org.apache.kafka.streams.examples.wordcount.WordCountDemo streams1.properties
```

# TAB3: Run streams application 2
```
bin/kafka-run-class.sh org.apache.kafka.streams.examples.wordcount.WordCountDemo streams2.properties
```

# TAB4: Show command-line tools
```
bin/kafka-groups.sh --list --bootstrap-server localhost:9092
bin/kafka-streams-groups.sh --members --describe --group streams-wordcount --bootstrap-server localhost:9092  --verbose
```

# TAB5: Enable standby replicas
```
kafka-configs.sh --bootstrap-server localhost:9092 --alter --entity-type groups
  --entity-name my_streams_app --add-config streams.num.standby.replicas=1
```