package org.azure.schema;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ProducerCallBack implements org.apache.kafka.clients.producer.Callback {
    private static final Logger log = LoggerFactory.getLogger(ProducerCallBack.class);
    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (metadata != null) {
            log.info("topic: " + metadata.topic() + " offset: " + metadata.offset() +" partition: " + metadata.partition());
        } else if (exception != null)
        {
            log.error(exception.getMessage());
            exception.printStackTrace();
        }
    }
}
