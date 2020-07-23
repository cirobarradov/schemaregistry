package org.azure.schema;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaAvroProducer {

    public final static String TOPIC = "prueba";

    private static final Logger log = LoggerFactory.getLogger(KafkaAvroProducer.class);

    SchemaRegistry registry= new SchemaRegistry();

    JsonConverter jsonConverter = new JsonConverter();

    public void sendIdSchema(String topic, String value,  String id)
    {

        try {
            JsonNode response = registry.getSchemaById(id);
            //parse json (getschema)
            //{"schema":"{\"type\":\"record\",\"name\":\"outer_record\",\"namespace\":\"com.azure.test\",\"fields\":[{\"name\":\"id\",\"type\":\"double\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"price\",\"type\":\"double\"},{\"name\":\"tags\",\"type\":{\"type\":\"array\",\"items\":\"string\"}}]}"}

            sendSchema(topic, value, jsonConverter.getSchema(response));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void sendSchema(String topic,String value, String userSchema) {
        KafkaProducer producer = null;
        try {
            Properties props = new Properties();

            props.load(getClass().getClassLoader().getResourceAsStream("kafka.properties"));

            producer = new KafkaProducer(props);

            Schema.Parser parser = new Schema.Parser();
            Schema schema = parser.parse(userSchema);



            final ProducerRecord<String, Object> record = new ProducerRecord(topic, getGenericRecord(value,schema));

            final Future<RecordMetadata> metadata =  producer.send(record, new ProducerCallBack());

            log.info("Sent:" + record);

            displayRecordMetaData(record, metadata);

        } catch (SerializationException e) {
            e.printStackTrace();
        } catch (IOException ioex){
            ioex.printStackTrace();
        }
        catch (Exception ex){
            ex.printStackTrace();
        }

        finally {
            producer.flush();
            producer.close();
        }
    }


    private GenericRecord getGenericRecord(String json, Schema schema) throws IOException
    {

        JsonDecoder jsonDecoder = DecoderFactory.get().jsonDecoder(schema, json);
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
        GenericRecord genericRecord = datumReader.read(null, jsonDecoder);

        return genericRecord;
    }


    private void displayRecordMetaData(final ProducerRecord record,
                                       final Future<RecordMetadata> future)
            throws InterruptedException, ExecutionException {
        final RecordMetadata recordMetadata = future.get();
        log.info(String.format("\n\t\t\tvalue=%s " +
                        "\n\t\t\tsent to topic=%s part=%d off=%d ",
                record,
                recordMetadata.topic(),
                recordMetadata.partition(),
                recordMetadata.offset(),
                recordMetadata.timestamp()
        ));
    }
    public static void main (String[] args)
    {
        KafkaAvroProducer producer= new KafkaAvroProducer();
        String json = "{\"f1\":\"ricardo\"}";
        String userSchema = "{\"type\":\"record\"," +
                "\"name\":\"myrecord\"," +
                "\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}";
        producer.sendSchema(TOPIC,json,userSchema);
    }

}