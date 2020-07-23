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
            updateInternalReg(TOPIC_1, "input1.json");

            updateInternalReg(TOPIC_1, "input2.json");

            updateInternalReg(TOPIC_2, "input3.json");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void updateInternalReg(String topic, String jsonPath ) throws IOException {

        String json = IOUtils.toString(MainTest.class.getClassLoader().getResourceAsStream(jsonPath), StandardCharsets.UTF_8.name());
        String schema = new AvroConverter().convert(json);

        JsonNode response =registry.postSubject(topic+"-value",schema);

        Integer id=jsonConverter.getId(response);

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
                            String key = (String)it.next();
                            String json = IOUtils.toString(MainTest.class.getClassLoader().getResourceAsStream(key), StandardCharsets.UTF_8.name());
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
