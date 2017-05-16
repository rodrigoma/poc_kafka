package br.com.rodrigoma.poc_kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.common.serialization.Serdes.Integer;
import static org.apache.kafka.common.serialization.Serdes.String;
import static org.apache.kafka.streams.StreamsConfig.*;
import static org.apache.kafka.streams.kstream.TimeWindows.of;

public class ConsumerRandom {

    public static void main(String args[]) {

        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(GROUP_ID_CONFIG, "random-test-grp");
        streamsConfiguration.put(APPLICATION_ID_CONFIG, "random-test-app");
        streamsConfiguration.put(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        streamsConfiguration.put(ZOOKEEPER_CONNECT_CONFIG, "127.0.0.1:2181");
        streamsConfiguration.put(KEY_SERDE_CLASS_CONFIG, String().getClass().getName());
        streamsConfiguration.put(VALUE_SERDE_CLASS_CONFIG, Integer().getClass().getName());

        KafkaStreams randomStreams = new KafkaStreams(randomTopology(), streamsConfiguration);
        randomStreams.setUncaughtExceptionHandler((thread, ex) -> System.out.println("[beckenbauer] [RandomStream] error: " + ex.getMessage()));
        randomStreams.start();
    }

    private static TopologyBuilder randomTopology() {
        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, Integer> source = builder.stream(String(), Integer(), "random");
        source
                .filter((key, value) -> value == 3)
                .groupByKey()
                .count(of(MINUTES.toMillis(1)),"counts")
                .toStream()
                .map(reindexKey())
                .to(Serdes.String(), Serdes.Long(),"groupRandom");

        return builder;
    }

    private static KeyValueMapper<Windowed<String>, Long, KeyValue<String, Long>> reindexKey() {
        return (key, value) -> new KeyValue<String, Long>(key.key(), value);
    }
}