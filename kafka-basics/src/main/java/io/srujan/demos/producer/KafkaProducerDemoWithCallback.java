package io.srujan.demos.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaProducerDemoWithCallback {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaProducerDemoWithCallback.class);

    public static void main(String[] args) {
        Properties properties = getProperties();

        List<String> subjects = new ArrayList<>();
        subjects.add("quantum physics");
        subjects.add("relativity physics");
        subjects.add("inorganic chemistry");
        subjects.add("organic chemistry");
        subjects.add("zoology");
        subjects.add("botany");
        subjects.add("biology");

        AtomicInteger i = new AtomicInteger(0);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            subjects.forEach(subject -> {
                i.set((i.intValue()/3));
                String key = String.valueOf(i.getAndIncrement());
                ProducerRecord<String, String> record = new ProducerRecord<>("subjects", key, subject);
                producer.send(record, (recordMetadata, e) -> {
                    LOG.info("message: {}", subject);
                    LOG.info("topic: {}", recordMetadata.topic());
                    LOG.info("key: {}", key);
                    LOG.info("partition: {}", recordMetadata.partition());
                    LOG.info("offset: {}", recordMetadata.offset());
                    LOG.info("hashCode: {}", recordMetadata.hashCode());
                    LOG.info("timestamp: {}", recordMetadata.timestamp());
                });
            });
        }


    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "equal-dinosaur-9300-eu2-kafka.upstash.io:9092");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"ZXF1YWwtZGlub3NhdXItOTMwMCQss2bIcvsOb31sNadtBUgiOMhNP-_I7VBu0Jo\" password=\"YzBlNjNjYWEtNmUxOC00ZGFjLTliNjctMjNkOWNmNGNiZTcx\";");

//        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "10");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }
}
