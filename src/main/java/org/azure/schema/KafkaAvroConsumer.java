package org.azure.schema;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

public class KafkaAvroConsumer {

//./kafka-avro-console-consumer --topic prueba --bootstrap-server localhost:9092 --property schema.registry.url="http://localhost:8081"

    private final static String OFFSET_CONFIG =
            "earliest";//"latest"earliest;

    private Consumer<String, String> createConsumer() {
        Properties props = new Properties();

        try {
            props.load(getClass().getClassLoader().getResourceAsStream("kafka.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Create the consumer using props.
        final Consumer<String, String> consumer =
                new KafkaConsumer<String, String>(props);
        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(KafkaAvroProducer.TOPIC));
        return consumer;
    }


    private void runConsumer() throws InterruptedException, IOException {
        final Consumer<String, String> consumer = createConsumer();

        final int giveUp = 100;
        int noRecordsCount = 0;
        while (true) {
            final ConsumerRecords<String, String> consumerRecords =
                    consumer.poll(1000);
            if (consumerRecords.count() == 0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }
            consumerRecords.forEach(record -> {
                System.out.printf("Consumer Record:(%s, %s, %d, %d)\n",
                        record.key(), record.value(),
                        record.partition(), record.offset());

            });
            //consumer.commitAsync();
        }
        consumer.close();
        System.out.println("DONE");
    }

    public static void main(String... args) throws Exception {
        KafkaAvroConsumer consumer = new KafkaAvroConsumer();
        consumer.runConsumer();
    }
}
