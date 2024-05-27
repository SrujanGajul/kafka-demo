package io.srujan.demos.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaProducerDemo {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaProducerDemo.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "equal-dinosaur-9300-eu2-kafka.upstash.io:9092");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"ZXF1YWwtZGlub3NhdXItOTMwMCQss2bIcvsOb31sNadtBUgiOMhNP-_I7VBu0Jo\" password=\"YzBlNjNjYWEtNmUxOC00ZGFjLTliNjctMjNkOWNmNGNiZTcx\";");

        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try(KafkaProducer<String, String> producer = new KafkaProducer<>(properties)){
            ProducerRecord<String, String> record = new ProducerRecord<>("subjects", "biology");
            producer.send(record);
        }
    }
}
