package br.com.rodrigoma.poc_kafka;

import avro.generate.AccessLogLine;
import io.confluent.kafka.serializers.KafkaAvroDecoder;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class ConsumerAvroClicks {
    private final ConsumerConnector consumer;
    private final KafkaProducer<String, AccessLogLine> producer;
    private final String inputTopic;
    private final String outputTopic;
    private final String zookeeper;
    private final String groupId;
    private final String url;
    private final Map<String, SessionState> state = new HashMap();
    private final int sessionLengthMs;
    private static ConsumerAvroClicks sessionizer;

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Please provide command line arguments: schemaRegistryUrl");
            System.exit(-1);
        }

        // currently hardcoding a lot of parameters, for simplicity
        String zookeeper = "192.168.0.136:2181";
        String groupId = "ConsumerAvroClicks";
        String inputTopic = "access";
        String outputTopic = "sessionized_access";
        String url = args[0];

        // Typically events are considered to be part of the same session if they are less than 30 minutes apart
        // To make this example show interesting results sooner, we limit the interval to 5 seconds
        int sessionLengthMs = 5*1000;


        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                sessionizer.consumer.shutdown();
            }
        });

        sessionizer = new ConsumerAvroClicks(zookeeper, groupId, inputTopic, outputTopic, url, sessionLengthMs);
        sessionizer.run();
    }

    public ConsumerAvroClicks(String zookeeper, String groupId, String inputTopic,
                              String outputTopic, String url, int sessionLengthMs) {
        this.consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                new ConsumerConfig(createConsumerConfig(zookeeper, groupId, url)));
        this.producer = getProducer(url);
        this.zookeeper = zookeeper;
        this.groupId = groupId;
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
        this.url = url;
        this.sessionLengthMs = sessionLengthMs;
    }

    private Properties createConsumerConfig(String zookeeper, String groupId, String url) {
        Properties props = new Properties();
        props.put(GROUP_ID_CONFIG, groupId);
        props.put("zookeeper.connect", zookeeper);
        props.put("schema.registry.url", url);
        props.put("specific.avro.reader", true);

        // We configure the consumer to avoid committing offsets and to always start consuming from beginning of topic
        // This is not a best practice, but we want the example consumer to show results when running it again and again
        props.put(ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(AUTO_OFFSET_RESET_CONFIG, "smallest");

        return props;
    }

    private void run() {
        Map<String, Integer> topicCountMap = new HashMap();

        // Hard coding single threaded consumer
        topicCountMap.put(inputTopic, 1);

        Properties props = createConsumerConfig(zookeeper, groupId, url);
        VerifiableProperties vProps = new VerifiableProperties(props);

        // Create decoders for key and value
        KafkaAvroDecoder avroDecoder = new KafkaAvroDecoder(vProps);
        StringDecoder stringDecoder = new StringDecoder(new VerifiableProperties());

        KafkaStream stream = consumer
                .createMessageStreams(topicCountMap, stringDecoder, avroDecoder)
                .get(inputTopic)
                .get(0);

        ConsumerIterator it = stream.iterator();
        System.out.println("Ready to start iterating wih properties: " + props.toString());
        System.out.println("Reading topic:" + inputTopic);

        while (it.hasNext()) {
            MessageAndMetadata messageAndMetadata = it.next();
            String ip = (String) messageAndMetadata.key();

            // Once we release a new version of the avro deserializer that can return SpecificData, the deep copy will be unnecessary
            GenericRecord genericEvent = (GenericRecord) messageAndMetadata.message();
            AccessLogLine event = (AccessLogLine) SpecificData.get().deepCopy(AccessLogLine.SCHEMA$, genericEvent);

            SessionState oldState = state.get(ip);
            int sessionId = 0;
            if (oldState == null) {
                state.put(ip, new SessionState(event.getTimestamp(), 0));
            } else {
                sessionId = oldState.getSessionId();

                // if the old timestamp is more than 30 minutes older than new one, we have a new session
                if (oldState.getLastConnection() < event.getTimestamp() - sessionLengthMs)
                    sessionId = sessionId + 1;

                SessionState newState = new SessionState(event.getTimestamp(), sessionId);
                state.put(ip, newState);
            }
            event.setSessionid(sessionId);
            System.out.println(event.toString());
            ProducerRecord<String, AccessLogLine> record = new ProducerRecord(outputTopic, event.getIp().toString(), event);
            producer.send(record);
        }
    }

    private KafkaProducer<String, AccessLogLine> getProducer(String url) {
        Properties props = new Properties();

        // hardcoding the Kafka server URI for this example
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.136:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url", url);

        KafkaProducer<String, AccessLogLine> producer = new KafkaProducer(props);
        return producer;
    }
}