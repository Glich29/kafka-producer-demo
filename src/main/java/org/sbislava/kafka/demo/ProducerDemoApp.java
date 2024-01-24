package org.sbislava.kafka.demo;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoApp {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoApp.class.getSimpleName());
    private static final String KAFKA_HOST = "127.0.0.1:9092";
    private static final String TOPIC = "kafka.demo";
    public static void main(String[] args) {
        log.info("Kafka producer demo app started...");
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_HOST);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "400");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        //        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        for (int i=0; i<10; i++) {
            for (int j = 0; j < 10; j++) {
                //create producer
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC, "Test msg 123!!! - msg#" + j);

                //send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        //executes every time a record successfully sent or an exception
                        if (e == null) {
                            log.info("Receive new metadata \n" +
                                    "Topic: " + recordMetadata.topic() + "\n" +
                                    "Partition: " + recordMetadata.partition() + "\n" +
                                    "Offset: " + recordMetadata.offset() + "\n" +
                                    "Timestamp: " + recordMetadata.timestamp());
                        } else {
                            log.error("Error while production", e);
                        }
                    }
                });
            }
        }

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //flush - tell the producer to send all data and block until done -- synchronous
        producer.flush();
        //flush and close the producer
        producer.close();

    }
}
