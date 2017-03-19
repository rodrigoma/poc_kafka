package br.com.rodrigoma.poc_kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static java.lang.String.format;
import static java.lang.System.out;
import static java.time.Instant.ofEpochMilli;
import static java.time.ZoneId.systemDefault;
import static java.time.format.DateTimeFormatter.ofLocalizedDateTime;
import static java.time.format.FormatStyle.FULL;

public class ProducerNumSeq {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer(props);

        IntStream.rangeClosed(1, 100)
                .boxed()
                .map(number -> new ProducerRecord<>("topic-1", number.toString(), number.toString()))
                .map(record -> {
                    try {
                        Thread.sleep(1000L);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    return producer.send(record);
                })
                .forEach(result -> printMetadata(result));
        producer.close();
    }

    private static void printMetadata(Future<RecordMetadata> f) {
        try {
            DateTimeFormatter formatter = ofLocalizedDateTime(FULL).withZone(systemDefault());
            RecordMetadata metadata = f.get();
            out.println(format("offset=>%s ts=>%s", metadata.offset(), formatter.format(ofEpochMilli(metadata.timestamp()))));
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}