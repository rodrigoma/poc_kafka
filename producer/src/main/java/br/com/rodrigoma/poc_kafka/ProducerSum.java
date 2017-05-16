package br.com.rodrigoma.poc_kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerSum {

    public static void main(String[] args) {

        // producer configuration
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");

        String topic = "sum";

        int values[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

        // create producer
        Producer<String, Integer> producer = new KafkaProducer(props);

        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, Integer> record = new ProducerRecord(topic, "sum_key", values[i]);

            producer.send(record); // dispatch message to broker
        }

        System.out.println("Messages published!");

        producer.close();
    }
}