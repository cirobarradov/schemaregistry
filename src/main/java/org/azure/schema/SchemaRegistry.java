package org.azure.schema;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.http.client.methods.*;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;


public class SchemaRegistry {

    private static final Logger log = LoggerFactory.getLogger(SchemaRegistry.class);

    private AvroConverter avroConverter = new AvroConverter();

    private JsonConverter jsonConverter = new JsonConverter();

    private String schemaRegistryUrl;

    public SchemaRegistry() {
        Properties props = new Properties();
        try {
            props.load(getClass().getClassLoader().getResourceAsStream("kafka.properties"));
            schemaRegistryUrl = props.getProperty("schema.registry.url");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private JsonNode executeRequest(HttpRequestBase request) throws IOException  {
        String result = null;

        CloseableHttpClient httpClient = HttpClients.createDefault();
        CloseableHttpResponse response = httpClient.execute(request);


        result = EntityUtils.toString(response.getEntity());
        return jsonConverter.parseJson(result);
    }

    private JsonNode get(String endpoint)  throws IOException{

        HttpGet get = new HttpGet(schemaRegistryUrl + endpoint);

        return executeRequest(get);
    }
    private JsonNode delete(String endpoint)  throws IOException{

        HttpDelete delete = new HttpDelete(schemaRegistryUrl + endpoint);

        return executeRequest(delete);
    }

    private JsonNode post(String endpoint, String body) throws IOException {

        HttpPost post = new HttpPost(schemaRegistryUrl + endpoint);
        post.setHeader("Content-Type", "application/json; charset=utf8");
        StringEntity params =new StringEntity(body);
        post.setEntity(params);

        return executeRequest(post);
    }


    public JsonNode getSubjects() throws IOException {
        return get("/subjects");
    }

    public JsonNode deleteSubject(String subject) throws IOException {
        return delete(String.format("/subjects/%s", subject));
    }

    public JsonNode getSubjectVersions (String subject) throws IOException {
        return get(String.format("/subjects/%s/versions", subject));
    }
    public JsonNode getSubjectVersionSchema (String subject) throws IOException
    {
        return getSubjectVersionSchema(subject,"latest");
    }
    public JsonNode getSubjectVersionSchema (String subject, String version) throws IOException
    {
        return get(String.format("/subjects/%s/versions/%s", subject,version));
    }
    public JsonNode getSchemaById (String id) throws IOException
    {
        return get(String.format("/schemas/ids/%s",id));
    }


    public JsonNode deleteSubjectVersionSchema(String subject, String version) throws IOException {
        return delete(String.format("/subjects/%s/versions/%s", subject,version));
    }

    public JsonNode postSubject(String subject, String schema) throws IOException {

        return post(String.format("/subjects/%s/versions", subject),jsonConverter.getSubject("schema",schema));

    }




    public static void main(String[] args) {
        SchemaRegistry reg = new SchemaRegistry();


        try {
            reg.getSubjectVersionSchema("test-value","1");
            //reg.listAllSchemas("prueba");
            JsonNode subjects= reg.getSubjects();
            Iterator it = subjects.iterator();
            while (it.hasNext())
            {
                JsonNode subject = (JsonNode) it.next();
                JsonNode version = reg.getSubjectVersions(subject.textValue());
                //JsonNode schema = reg.getSubjectVersionSchema(subject.textValue(),version.textValue());
                JsonNode schema = reg.getSubjectVersionSchema(subject.textValue(),"latest");

                log.info(schema.toString());
                //{"schema": "{\"type\":\"record\",\"name\":\"Payment\",\"namespace\":\"my.examples\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"}]}"}

            }
            String testschema = "{\n" +
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
            log.info(testschema);

            JsonNode testversion = reg.postSubject("test-value", testschema);
            log.info(testversion.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
