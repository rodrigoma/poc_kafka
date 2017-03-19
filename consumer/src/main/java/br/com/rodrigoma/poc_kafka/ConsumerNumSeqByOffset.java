package br.com.rodrigoma.poc_kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;
import java.util.Set;

import static java.lang.System.out;
import static java.util.Arrays.asList;

public class ConsumerNumSeqByOffset {
    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = createConsumer();
        consumer.subscribe(asList(TOPIC));

        boolean flag = true;

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            if (flag) {
                Set<TopicPartition> assignments = consumer.assignment();
                assignments.forEach(topicPartition -> consumer.seek(topicPartition,90));
                flag = false;
            }

            for (ConsumerRecord<String, String> record : records) {
                out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }
    }

    static final String TOPIC = "topic-1";

    static KafkaConsumer createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        return new KafkaConsumer(props);
    }
}