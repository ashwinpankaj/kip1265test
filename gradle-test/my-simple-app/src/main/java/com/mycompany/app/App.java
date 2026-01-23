package com.mycompany.app;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Kafka Producer Application
 *
 */
public class App
{
    public static void main( String[] args )
    {
        System.out.println( "Initializing Kafka Producer..." );

        // Configure producer properties
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "my-simple-app-producer");

        // Additional configuration for reliability
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);

        KafkaProducer<String, String> producer = null;

        try {
            // Initialize the producer
            producer = new KafkaProducer<>(props);
            System.out.println("Kafka Producer initialized successfully!");

            // Send a sample message
            String topic = "test-topic";
            String key = "sample-key";
            String value = "Hello, Kafka!";

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.printf("Message sent successfully to topic: %s, partition: %d, offset: %d%n",
                            metadata.topic(), metadata.partition(), metadata.offset());
                } else {
                    System.err.println("Error sending message: " + exception.getMessage());
                }
            });

            System.out.println("Message queued for sending...");

        } catch (Exception e) {
            System.err.println("Error initializing Kafka producer: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Clean up resources
            if (producer != null) {
                producer.flush(); // Ensure all messages are sent
                producer.close();
                System.out.println("Kafka Producer closed.");
            }
        }
    }
}
