package br.com.rodrigoma.poc_kafka;

import avro.generate.AccessLogLine;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerAvroClicks {

    public static void main(String[] args) throws InterruptedException {
        if (args.length != 1) {
            System.out.println("Please provide command line arguments: schemaRegistryUrl");
            System.exit(-1);
        }

        String schemaUrl = args[0];

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url", schemaUrl);

        // Hard coding topic too.
        String topic = "access";

        // Hard coding wait between events so demo experience will be uniformly nice
        int wait = 500;

        Producer<String, AccessLogLine> producer = new KafkaProducer(props);

        // We keep producing new events and waiting between them until someone ctrl-c
        while (true) {
            AccessLogLine event = EventGenerator.getNext();
            System.out.println("Generated event " + event.toString());

            // Using IP as key, so events from same IP will go to same partition
            ProducerRecord<String, AccessLogLine> record = new ProducerRecord(topic, event.getIp().toString(), event);
            producer.send(record);
            Thread.sleep(wait);
        }
    }
}