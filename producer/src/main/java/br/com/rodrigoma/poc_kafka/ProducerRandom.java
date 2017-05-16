package br.com.rodrigoma.poc_kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

import static java.lang.String.valueOf;
import static java.lang.System.out;
import static java.lang.Thread.sleep;
import static java.time.LocalTime.now;

public class ProducerRandom {

    public static void main(String[] args) throws InterruptedException {

        // producer configuration
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");

        String topic = "random";

        // create producer
        Producer<String, Integer> producer = new KafkaProducer(props);

        while (true) {
            int nextKey = new Random().nextInt(5);
            int nextInt = new Random().nextInt(4);

            ProducerRecord<String, Integer> record = new ProducerRecord(topic, valueOf(nextKey), nextInt);

            if (nextInt == 3) {
                out.println(now() + " - " + nextKey + " - " + nextInt);
            }

            producer.send(record); // dispatch message to broker

            sleep(2000l);
        }
    }
}