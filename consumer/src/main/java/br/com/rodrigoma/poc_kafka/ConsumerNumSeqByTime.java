package br.com.rodrigoma.poc_kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static br.com.rodrigoma.poc_kafka.ConsumerNumSeqByOffset.TOPIC;
import static br.com.rodrigoma.poc_kafka.ConsumerNumSeqByOffset.createConsumer;
import static java.lang.System.out;
import static java.time.Instant.ofEpochMilli;
import static java.time.ZoneId.systemDefault;
import static java.time.format.DateTimeFormatter.ofLocalizedDateTime;
import static java.time.format.FormatStyle.FULL;
import static java.util.Arrays.asList;

public class ConsumerNumSeqByTime {
    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = createConsumer();
        consumer.subscribe(asList(TOPIC));

        boolean flag = true;

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);

            if (flag) {
                Set<TopicPartition> assignments = consumer.assignment();
                Map<TopicPartition, Long> query = new HashMap();
                for (TopicPartition topicPartition : assignments) {
                    query.put(
                            topicPartition,
                            ZonedDateTime.of(2017, 3, 19, 16, 48, 34, 0, ZoneId.systemDefault()).toInstant().toEpochMilli()
//                            now().minus(8, MINUTES).toEpochMilli()
                    );
                }

                Map<TopicPartition, OffsetAndTimestamp> result = consumer.offsetsForTimes(query);

                result.entrySet()
                        .stream()
                        .forEach(entry ->
                                consumer.seek(
                                        entry.getKey(),
                                        Optional.ofNullable(entry.getValue())
                                                .map(OffsetAndTimestamp::offset)
                                                .orElse(new Long(0))));

                flag = false;
            }

            DateTimeFormatter formatter = ofLocalizedDateTime(FULL).withZone(systemDefault());
            for (ConsumerRecord<String, String> record : records) {
                out.printf("offset = %d, key = %s, value = %s, ts = %s%n", record.offset(), record.key(), record.value(), formatter.format(ofEpochMilli(record.timestamp())));
            }
        }
    }
}