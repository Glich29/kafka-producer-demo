Run of container
```shell
docker compose start
```

Get list of topics
```shell
docker exec -ti kafka_simple /usr/bin/kafka-topics --list --bootstrap-server localhost:9091
```

Send a message
Every line - one message. To break - Ctrl + Z
```shell
docker exec -ti kafka_simple /usr/bin/kafka-console-producer --topic topic1 --bootstrap-server localhost:9092
```

Receive message
```shell
docker exec -ti kafka_simple /usr/bin/kafka-console-consumer --from-beginning --topic topic1 --bootstrap-server localhost:9092
```
Receive message how consumer1
```shell
docker exec -ti kafka_simple /usr/bin/kafka-console-consumer --group consumer1 --topic topic1 --bootstrap-server localhost:9092
```

Send message with key-value separator : (key:value)
```shell
docker exec -ti kafka_simple /usr/bin/kafka-console-producer --topic kafka.demo.with.key --property "parse.key=true" --property "key.separator=:" --bootstrap-server localhost:9092 
```


