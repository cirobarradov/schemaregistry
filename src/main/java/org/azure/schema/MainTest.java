package org.azure.schema;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class MainTest {

    private static final Logger log = LoggerFactory.getLogger(MainTest.class);


    public static void main (String[] args)
    {
        /**
         * Message
         */
        /*
        String json = "{\n" +
                "    \"id\": 1,\n" +
                "    \"name\": \"A green door\",\n" +
                "    \"price\": 12.50,\n" +
                "    \"tags\": [\"home\", \"green\"]\n" +
                "}\n"
                ;
*/
        String json=null;
        String avroSchema = null;
        try {

            json = IOUtils.toString(MainTest.class.getClassLoader().getResourceAsStream("input1.json"), StandardCharsets.UTF_8.name());


            /**
             * Generate Avro Schema from Json
             */
            avroSchema = new AvroConverter().convert(json);

            log.info(avroSchema);

            /**
             * Publish Avro Schema in Schema Registry Server
             */
            SchemaRegistry registry = new SchemaRegistry();
            /*
            JsonNode response = registry.getSubjectVersionSchema(KafkaAvroProducer.TOPIC+"-value");
            while (!response.toString().equals("{\"error_code\":40401,\"message\":\"Subject not found.\"}"))
            {
                response = registry.deleteSubject(KafkaAvroProducer.TOPIC+"-value");
            }

            response = registry.postSubject(KafkaAvroProducer.TOPIC+"-value",avroSchema);
            log.info(response.toString());
*/
            JsonNode response = registry.getSubjectVersions(KafkaAvroProducer.TOPIC+"-value");

            String id=null;
            if (!response.isArray())
            {
                registry.postSubject(KafkaAvroProducer.TOPIC+"-value",avroSchema);
            }
            id=response.get(response.size()-1).toString();
            log.info(response.toString());
            log.info(registry.getSchemaById(id).toString());

            /**
             * Send json message with avro serializer and schema id
             */
            KafkaAvroProducer producer = new KafkaAvroProducer();

            producer.sendIdSchema(KafkaAvroProducer.TOPIC,json,id);

            log.info(response.toString());



        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
