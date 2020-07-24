package org.azure.schema;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.io.IOUtils;
import scala.Tuple2;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

public class GpsFake {

    SchemaRegistry registry = new SchemaRegistry();

    JsonConverter jsonConverter = new JsonConverter();
    public static String TOPIC_1 ="topic1";
    public static String TOPIC_2 = "topic2";

    private Map<String, Tuple2<Integer,String>> gpsRegistry = new HashMap<>();

    public GpsFake ()
    {
        try {
            //cache input1.json schema associated to topic1
            updateInternalReg(TOPIC_1, "input1.json");
            //cache input2.json schema associated to topic1
            updateInternalReg(TOPIC_1, "input2.json");
            //cache input3.json schema associated to topic2
            updateInternalReg(TOPIC_2, "input3.json");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void updateInternalReg(String topic, String jsonPath ) throws IOException {
        //get json content from filepath
        String json = IOUtils.toString(MainTest.class.getClassLoader().getResourceAsStream(jsonPath), StandardCharsets.UTF_8.name());
        //get avro schema associated to json
        String schema = new AvroConverter().convert(json);
        //store schema in schema registry
        JsonNode response =registry.postSubject(topic+"-value",schema);
        //get Id from schema registry
        Integer id=jsonConverter.getId(response);
        //create tuple with id and topic
        gpsRegistry.put(jsonPath,new Tuple2(id,topic));
    }

    public void sendMessage ()
    {
        KafkaAvroProducer producer = new KafkaAvroProducer();

        IntStream.iterate(0, i -> i + 1)
                .limit(10)
                .forEach(i -> {
                    Iterator it = gpsRegistry.keySet().iterator();
                    while (it.hasNext())
                    {
                        try {
                            // key = filename
                            String key = (String)it.next();
                            // json = filename content
                            String json = IOUtils.toString(MainTest.class.getClassLoader().getResourceAsStream(key), StandardCharsets.UTF_8.name());
                            // sendIdSchema(String topic, String value,  String id)
                            producer.sendIdSchema(gpsRegistry.get(key)._2, json, gpsRegistry.get(key)._1.toString());
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                });
    }

    public static void main(String[] args)
    {

        GpsFake gps =  new GpsFake();
        gps.sendMessage();
    }
}
