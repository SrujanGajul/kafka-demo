package io.srujan.demos.consumer;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class KafkaConsumerDemo {
    public static void main(String[] args) {
        Properties properties = getProperties();
        String topic = "subjects";
        try(KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)){
            consumer.subscribe(List.of(topic));
            while (true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record: records){
                    String key = record.key();
                    String value = record.value();
                    System.out.printf("Consumed event from topic %s: key = %-10s value = %s%n", topic, key, value);
                }
            }
        }
    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "equal-dinosaur-9300-eu2-kafka.upstash.io:9092");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"ZXF1YWwtZGlub3NhdXItOTMwMCQss2bIcvsOb31sNadtBUgiOMhNP-_I7VBu0Jo\" password=\"YzBlNjNjYWEtNmUxOC00ZGFjLTliNjctMjNkOWNmNGNiZTcx\";");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(CommonClientConfigs.GROUP_ID_CONFIG, "kafka-consumer-1");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return properties;
    }
}
