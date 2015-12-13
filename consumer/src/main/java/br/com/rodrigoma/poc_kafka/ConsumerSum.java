package br.com.rodrigoma.poc_kafka;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;

public class ConsumerSum {

    private static final int SO_TIMEOUT = 100000; // socket timeout
    private static final int BUFFER_SIZE = 64 * 1024; // maximum socket receive buffer in bytes
    private static final int FETCH_SIZE = 100000; // maximum bytes to fetch from topic

    public static void main(String args[]) {
        ConsumerSum consumerSum = new ConsumerSum();

        String topic = "sum"; // topic from which to consume messages

        String address = "192.168.0.136"; // broker address

        int port = 9092; // broker port

        int partition = 0; // partition that has the messages the application is interested in

        int offset = 0;   // from which point in the topic the consumer should start reading messages.
        // 0 means that the first published message is read and all subsequent ones.

        // keep replaying all messages from the partition until consumerSum.consume(...) returns true
        while (!consumerSum.consume(offset, address, port, topic, partition)) { }
    }

    private boolean consume(int offset, String address, int port, String topic, int partition) {
        try {
            String consumerGroup = "Client_" + topic + "_" + partition;

            SimpleConsumer consumer = new SimpleConsumer(address, port, SO_TIMEOUT, BUFFER_SIZE, consumerGroup);

            FetchRequest req = new FetchRequestBuilder()
                    .clientId(consumerGroup)
                    .addFetch(topic, partition, offset, FETCH_SIZE)
                    .build();

            FetchResponse fetchResponse = consumer.fetch(req); // fetch messages from broker

            int sum = 0;

            // iterate over all messages fetched from the topic
            for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partition)) {
                ByteBuffer key = messageAndOffset.message().key();
                byte[] bytes = new byte[key.limit()];
                key.get(bytes);

                ByteBuffer payload = messageAndOffset.message().payload();

                int value = payload.getInt();

                System.out.print("Offset = " + String.valueOf(messageAndOffset.offset()) + " -- Sum: " + sum + " + " + value + " = ");
                sum += value;
                System.out.println(sum);
            }

            if (consumer != null) consumer.close();
        } catch (Exception e) {
            return false;
        }

        return true;
    }
}