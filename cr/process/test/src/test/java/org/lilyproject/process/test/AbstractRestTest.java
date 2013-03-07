/*
 * Copyright 2010 Outerthought bvba
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilyproject.process.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.JsonNode;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.lilyproject.lilyservertestfw.LilyProxy;
import org.lilyproject.util.json.JsonFormat;

public abstract class AbstractRestTest {
    protected static String BASE_URI;

    private static HttpClient httpClient;
    private static LilyProxy lilyProxy;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        lilyProxy = new LilyProxy();
        lilyProxy.start();

        httpClient = new DefaultHttpClient();

        BASE_URI = "http://localhost:12060/repository";
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        try {
            if (lilyProxy != null) {
                lilyProxy.stop();
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }



    protected void makeBookSchema() throws Exception {
        // Create field type
        String body = json("{name: 'b$title', valueType: 'STRING', " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'b' } }");
        ResponseAndContent response = put("/schema/fieldType/b$title?ns.b=org.lilyproject.resttest", body);
        assertTrue(isSuccess(response));

        // Create field type
        body = json("{name: 'b$summary', valueType: 'STRING', " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'b' } }");
        response = put("/schema/fieldType/b$summary?ns.b=org.lilyproject.resttest", body);
        assertTrue(isSuccess(response));

        // Create a record type
        body = json("{name: 'b$book', fields: [ {name: 'b$title'}, {name: 'b$summary'} ]," +
                "namespaces: { 'org.lilyproject.resttest': 'b' } }");
        response = put("/schema/recordType/b$book?ns.b=org.lilyproject.resttest", body);
        assertTrue(isSuccess(response));
    }

    protected boolean isSuccess(ResponseAndContent response) {
        int code = response.getResponse().getStatusLine().getStatusCode();
        return (code >= 200 && code < 210);
    }



    protected JsonNode getFieldValue(JsonNode recordJson, String fieldName) {
        String prefix = recordJson.get("namespaces").get("org.lilyproject.resttest").getTextValue();
        JsonNode fieldsNode = recordJson.get("fields");
        return fieldsNode.get(prefix + "$" + fieldName);
    }

    protected void assertStatus(int expectedStatus, ResponseAndContent response) throws IOException {
        StackTraceElement[] trace = new Exception().getStackTrace();
        if (expectedStatus != response.getResponse().getStatusLine().getStatusCode()) {
            System.err.println("Detected unexpected response status.");
            System.err.println("Expected status: " + expectedStatus);
            System.err.println("Got status: " + response.getResponse().getStatusLine().getStatusCode());
            System.err.println("Called from: " + trace[1].toString());
            System.err.println("Body of the response is:");
            printErrorResponse(response);
            assertEquals(expectedStatus, response.getResponse().getStatusLine().getStatusCode());
        }
    }

    protected void printErrorResponse(ResponseAndContent response) throws IOException {
        if (response.getResponse().getEntity() != null && response.getResponse().getEntity().getContentType() != null
                && "application/json".equals(response.getResponse().getEntity().getContentType().getValue())) {
            JsonNode json = JsonFormat.deserializeNonStd(response.getContent());
            System.err.println("Error:");
            System.err.println("  Description: " +
                    (json.get("description") != null ? json.get("description").getTextValue() : null));
            System.err.println("  Status: " + (json.get("status") != null ? json.get("status").getIntValue() : null));
            System.err.println("  StackTrace:");
            JsonNode stackTrace = json.get("stackTrace");
            System.out.println(stackTrace != null ? stackTrace.getValueAsText() : null);
        } else if (response.getContent() != null) {
            System.err.println(new String(response.getContent(), "UTF-8"));
        } else {
            System.err.println("response has no content)");
        }
    }

    protected String buildUri(String path) {
        return BASE_URI + path;
    }

    protected ResponseAndContent post(String path, String body) throws Exception {
        HttpPost post = new HttpPost(buildUri(path));
        post.setEntity(new StringEntity(body, "application/json", "UTF-8"));
        return processResponseAndContent(post);
    }

    protected ResponseAndContent postText(String path, String body) throws Exception {
        HttpPost post = new HttpPost(buildUri(path));
        post.setEntity(new StringEntity(body, "text/plain", "UTF-8"));
        return processResponseAndContent(post);
    }

    protected ResponseAndContent put(String path, String body) throws Exception {
        return putUri(buildUri(path), body);
    }

    protected ResponseAndContent putUri(String uri, String body) throws Exception {
        HttpPut put = new HttpPut(uri);
        put.setEntity(new StringEntity(body, "application/json", "UTF-8"));
        return processResponseAndContent(put);
    }

    protected ResponseAndContent get(String path) throws Exception {
       return getUri(buildUri(path));
    }

    protected ResponseAndContent getUri(String uri) throws Exception {
        return processResponseAndContent(new HttpGet(uri));
    }

    protected ResponseAndContent delete(String path) throws Exception {
        return deleteUri(buildUri(path));
    }

    protected ResponseAndContent deleteUri(String uri) throws Exception {
        return processResponseAndContent(new HttpDelete(uri));
    }

    public static JsonNode readJson(ResponseAndContent rac) throws IOException {
        return JsonFormat.deserializeNonStd(rac.getContent());
    }

    protected String json(String input) {
        return input.replaceAll("'", "\"");
    }


    protected ResponseAndContent processResponseAndContent(HttpUriRequest request) throws Exception {
        HttpResponse response = null;
        byte[] data = null;
        try {
            response = httpClient.execute(request);
            if (response.getEntity() != null) {
                data = IOUtils.toByteArray(response.getEntity().getContent());
            }
        } finally {
            if (response != null) {
                EntityUtils.consume(response.getEntity());
            }
        }

        return new ResponseAndContent(response, data);
    }

}
