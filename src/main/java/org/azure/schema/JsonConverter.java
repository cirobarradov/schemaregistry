package org.azure.schema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class JsonConverter {

    private static final Logger log = LoggerFactory.getLogger(JsonConverter.class);

    public String getSubject(String key, String value) throws IOException {
        return printJson(createJson(key,value));
    }

    public JsonNode createJson(String key, String value) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode rootNode = mapper.createObjectNode();
        ((ObjectNode) rootNode).put(key,value.replaceAll("(\\r|\\n)", ""));
        return rootNode;
    }

    public String printJson (JsonNode json)
    {
        ObjectMapper mapper = new ObjectMapper();
        String jsonString = null;
        try {
            jsonString = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return jsonString;
    }

    public JsonNode parseJson(String jsonString) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readTree(jsonString);
    }

    public String getSchema(JsonNode schema) throws IOException{
        return schema.get("schema").textValue();
    }

    public Integer getId(JsonNode response) throws IOException{
        return response.get("id").intValue();
    }

    public static void main (String[] args)
    {
        JsonConverter converter = new JsonConverter();
        String schema = "{\n" +
                "  \"namespace\" : \"com.azure.test\",\n" +
                "  \"name\" : \"outer_record\",\n" +
                "  \"type\" : \"record\",\n" +
                "  \"fields\" : [ {\n" +
                "    \"name\" : \"id\",\n" +
                "    \"type\" : \"double\"\n" +
                "  }, {\n" +
                "    \"name\" : \"name\",\n" +
                "    \"type\" : \"string\"\n" +
                "  }, {\n" +
                "    \"name\" : \"price\",\n" +
                "    \"type\" : \"double\"\n" +
                "  }, {\n" +
                "    \"name\" : \"tags\",\n" +
                "    \"type\" : {\n" +
                "      \"type\" : \"array\",\n" +
                "      \"items\" : \"string\"\n" +
                "    }\n" +
                "  } ]\n" +
                "}";
        try {
            JsonNode res = converter.createJson("schema",schema);
            String resString = converter.printJson(res);
            log.info(resString);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
