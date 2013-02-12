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

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
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
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.lilyservertestfw.LilyProxy;
import org.lilyproject.util.json.JsonFormat;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class RestTest {
    private static String BASE_URI;

    private static HttpClient httpClient;
    private static LilyProxy lilyProxy;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        lilyProxy = new LilyProxy();
        lilyProxy.start();

        httpClient = new DefaultHttpClient();

        BASE_URI = "http://localhost:8899/repository";
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

    @Test
    public void testGeneralErrors() throws Exception {
        // Perform request to non-existing resource class
        ResponseAndContent response = get(BASE_URI + "/foobar");
        assertStatus(HttpStatus.SC_NOT_FOUND, response);

        // Submit invalid json
        String body = json("{ f [ }");
        response = post(BASE_URI + "/schema/fieldType", body);
        assertStatus(HttpStatus.SC_BAD_REQUEST, response);
    }

    @Test
    public void testFieldTypes() throws Exception {
        // Create field type using POST
        String body = json("{action: 'create', fieldType: {name: 'n$field1', valueType: 'STRING', " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        ResponseAndContent response = post(BASE_URI + "/schema/fieldTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        JsonNode json = readJson(response);

        // verify location header
        assertEquals(BASE_URI + "/schema/fieldTypeById/" + json.get("id").getValueAsText(), response.getLocationRef().toString());

        // verify name
        String prefix = json.get("namespaces").get("org.lilyproject.resttest").getTextValue();
        assertEquals(prefix + "$field1", json.get("name").getTextValue());

        // Create field type using POST on the name-based resource
        body = json("{action: 'create', fieldType: {name: 'n$field1a', valueType: 'STRING', " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        response = post(BASE_URI + "/schema/fieldType", body);
        assertStatus(HttpStatus.SC_CREATED, response);
        assertEquals(BASE_URI + "/schema/fieldType/n$field1a?ns.n=org.lilyproject.resttest", response.getLocationRef().toString());

        // Create field type using PUT
        body = json("{name: 'n$field2', valueType: 'STRING', " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } }");
        response = put(BASE_URI + "/schema/fieldType/n$field2?ns.n=org.lilyproject.resttest", body);
        String fieldType2Id = readJson(response).get("id").getValueAsText();
        assertStatus(HttpStatus.SC_CREATED, response);
        assertEquals(BASE_URI + "/schema/fieldType/n$field2?ns.n=org.lilyproject.resttest", response.getLocationRef().toString());

        // Update a field type - by name : change field2 to field3
        body = json("{name: 'n$field3', valueType: 'STRING', " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } }");
        response = put(BASE_URI + "/schema/fieldType/n$field2?ns.n=org.lilyproject.resttest", body);
        assertStatus(HttpStatus.SC_MOVED_PERMANENTLY, response);
        assertEquals(BASE_URI + "/schema/fieldType/n$field3?ns.n=org.lilyproject.resttest", response.getLocationRef().toString());

        response = get(BASE_URI + "/schema/fieldType/n$field2?ns.n=org.lilyproject.resttest");
        assertStatus(HttpStatus.SC_NOT_FOUND, response);

        response = get(BASE_URI + "/schema/fieldType/n$field3?ns.n=org.lilyproject.resttest");
        assertStatus(HttpStatus.SC_OK, response);

        // Update a field type - by ID : change field3 to field4
        body = json("{name: 'n$field4', valueType: 'STRING', "
                + "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } }");
        response = put(BASE_URI + "/schema/fieldTypeById/" + fieldType2Id, body);
        assertStatus(HttpStatus.SC_OK, response);

        // Test updating immutable properties
        body = json("{name: 'n$field4', valueType: 'INTEGER', "
                + "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } }");
        response = put(BASE_URI + "/schema/fieldTypeById/" + fieldType2Id, body);
        assertStatus(HttpStatus.SC_CONFLICT, response);

        body = json("{name: 'n$field4', valueType: 'STRING', "
                + "scope: 'non_versioned', namespaces: { 'org.lilyproject.resttest': 'n' } }");
        response = put(BASE_URI + "/schema/fieldTypeById/" + fieldType2Id, body);
        assertStatus(HttpStatus.SC_CONFLICT, response);

        body = json("{name: 'n$field4', valueType: 'LIST<STRING>', "
                + "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } }");
        response = put(BASE_URI + "/schema/fieldTypeById/" + fieldType2Id, body);
        assertStatus(HttpStatus.SC_CONFLICT, response);

        body = json("{name: 'n$field4', valueType: 'PATH<STRING>', "
                + "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } }");
        response = put(BASE_URI + "/schema/fieldType/n$field4?ns.n=org.lilyproject.resttest", body);
        assertStatus(HttpStatus.SC_CONFLICT, response);

        // Get list of field types
        response = get(BASE_URI + "/schema/fieldType");
        assertStatus(HttpStatus.SC_OK, response);
        json = readJson(response);
        assertTrue(json.get("results").size() > 0);

        response = get(BASE_URI + "/schema/fieldTypeById");
        assertStatus(HttpStatus.SC_OK, response);
        json = readJson(response);
        assertTrue(json.get("results").size() > 0);
    }

    @Test
    public void testRecordTypes() throws Exception {
        // Create some field types
        List<String> fieldTypeIds = new ArrayList<String>();
        for (int i = 1; i < 4; i++) {
            String body = json("{action: 'create', fieldType: {name: 'n$rt_field" + i +
                    "', valueType: 'STRING', " +
                    "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } } }");
            ResponseAndContent response = post(BASE_URI + "/schema/fieldTypeById", body);
            assertStatus(HttpStatus.SC_CREATED, response);
            JsonNode json = readJson(response);
            fieldTypeIds.add(json.get("id").getValueAsText());
        }

        // Create a record type using POST
        String body = json("{action: 'create', recordType: {name: 'n$recordType1', fields: [ {name: 'n$rt_field1'} ]," +
                "namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        ResponseAndContent response = post(BASE_URI + "/schema/recordTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        JsonNode json = readJson(response);

        // verify location header
        assertEquals(BASE_URI + "/schema/recordTypeById/" + json.get("id").getValueAsText(),
                response.getLocationRef().toString());

        // verify name
        String prefix = json.get("namespaces").get("org.lilyproject.resttest").getTextValue();
        assertEquals(prefix + "$recordType1", json.get("name").getTextValue());

        // verify the field
        assertEquals(fieldTypeIds.get(0), json.get("fields").get(0).get("id").getTextValue());
        assertFalse(json.get("fields").get(0).get("mandatory").getBooleanValue());

        // verify version
        assertEquals(1L, json.get("version").getLongValue());

        // Create a record type using POST on the name-based resource
        body = json("{action: 'create', recordType: {name: 'n$recordType1a', fields: [ {name: 'n$rt_field1'} ]," +
                "namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        response = post(BASE_URI + "/schema/recordType", body);
        assertStatus(HttpStatus.SC_CREATED, response);
        assertEquals(BASE_URI + "/schema/recordType/n$recordType1a?ns.n=org.lilyproject.resttest", response.getLocationRef().toString());

        // Create a record type using PUT
        body = json("{name: 'n$recordType2', fields: [ {name: 'n$rt_field1'} ]," +
                "namespaces: { 'org.lilyproject.resttest': 'n' } }");
        response = put(BASE_URI + "/schema/recordType/n$recordType2?ns.n=org.lilyproject.resttest", body);
        assertStatus(HttpStatus.SC_CREATED, response);
        assertEquals(BASE_URI + "/schema/recordType/n$recordType2?ns.n=org.lilyproject.resttest", response.getLocationRef().toString());
        json = readJson(response);
        String secondRtId = json.get("id").getValueAsText();

        // Update a record type - by ID
        body = json("{name: 'n$recordType2', " +
                "fields: [ {name: 'n$rt_field1', mandatory: true}, " +
                " {name : 'n$rt_field2', mandatory: true} ], namespaces: { 'org.lilyproject.resttest': 'n' } }");
        response = put(BASE_URI + "/schema/recordTypeById/" + secondRtId, body);
        assertStatus(HttpStatus.SC_OK, response);
        json = readJson(response);
        assertEquals(secondRtId, json.get("id").getTextValue());
        assertEquals(2L, json.get("version").getLongValue());
        assertEquals(2, json.get("fields").size());
        assertTrue(json.get("fields").get(0).get("mandatory").getBooleanValue());
        assertTrue(json.get("fields").get(1).get("mandatory").getBooleanValue());

        // Rename a record type via the name-based resource
        body = json("{name: 'n$recordType3', " +
                "fields: [ {name: 'n$rt_field1', mandatory: true}, " +
                " {name : 'n$rt_field2', mandatory: true} ], namespaces: { 'org.lilyproject.resttest': 'n' } }");
        response = put(BASE_URI + "/schema/recordType/n$recordType2?ns.n=org.lilyproject.resttest", body);
        assertStatus(HttpStatus.SC_MOVED_PERMANENTLY, response);
        assertEquals(BASE_URI + "/schema/recordType/n$recordType3?ns.n=org.lilyproject.resttest", response.getLocationRef().toString());

        // Get list of record types
        response = get(BASE_URI + "/schema/recordType");
        assertStatus(HttpStatus.SC_OK, response);
        json = readJson(response);
        assertTrue(json.get("results").size() > 0);

        response = get(BASE_URI + "/schema/recordTypeById");
        assertStatus(HttpStatus.SC_OK, response);
        json = readJson(response);
        assertTrue(json.get("results").size() > 0);

        //
        // Test mixins
        //

        // Create two mixin record types
        body = json("{action: 'create', recordType: {name: 'n$mixin1', fields: [ {name: 'n$rt_field2'} ]," +
                "namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        response = post(BASE_URI + "/schema/recordTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);
        String mixin1Id = readJson(response).get("id").getTextValue();

        body = json("{action: 'create', recordType: {name: 'n$mixin2', fields: [ {name: 'n$rt_field3'} ]," +
                "namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        response = post(BASE_URI + "/schema/recordTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);
        String mixin2Id = readJson(response).get("id").getTextValue();

        // Create a record type using the mixins
        body = json("{action: 'create', recordType: {name: 'n$mixinUser', fields: [ {name: 'n$rt_field1'} ]," +
                "mixins: [{name: 'n$mixin1', version: 1}, { id: '" + mixin2Id + "' } ], " +
                "namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        response = post(BASE_URI + "/schema/recordTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        response = get(response.getLocationRef().toString());
        assertStatus(HttpStatus.SC_OK, response);
        json = readJson(response);

        assertEquals(2, json.get("mixins").size());

        // Update to remove one of the mixins
        body = json("{name: 'n$mixinUser', fields: [ {name: 'n$rt_field1'} ]," +
                "mixins: [{name: 'n$mixin1', version: 1}], " +
                "namespaces: { 'org.lilyproject.resttest': 'n' } }");
        String mixinUserUri = BASE_URI + "/schema/recordType/n$mixinUser?ns.n=org.lilyproject.resttest";
        response = put(mixinUserUri, body);
        assertStatus(HttpStatus.SC_OK, response);

        response = get(mixinUserUri);
        assertStatus(HttpStatus.SC_OK, response);
        json = readJson(response);

        assertEquals(1, json.get("mixins").size());
    }

    private void makeBookSchema() throws Exception {
        // Create field type
        String body = json("{name: 'b$title', valueType: 'STRING', " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'b' } }");
        ResponseAndContent response = put(BASE_URI + "/schema/fieldType/b$title?ns.b=org.lilyproject.resttest", body);
        assertTrue(isSuccess(response));

        // Create field type
        body = json("{name: 'b$summary', valueType: 'STRING', " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'b' } }");
        response = put(BASE_URI + "/schema/fieldType/b$summary?ns.b=org.lilyproject.resttest", body);
        assertTrue(isSuccess(response));

        // Create a record type
        body = json("{name: 'b$book', fields: [ {name: 'b$title'}, {name: 'b$summary'} ]," +
                "namespaces: { 'org.lilyproject.resttest': 'b' } }");
        response = put(BASE_URI + "/schema/recordType/b$book?ns.b=org.lilyproject.resttest", body);
        assertTrue(isSuccess(response));
    }

    private boolean isSuccess(ResponseAndContent response) {
        int code = response.getResponse().getStatusLine().getStatusCode();
        return (code >= 200 && code < 210);
    }

    @Test
    public void testRecordBasics() throws Exception {
        makeBookSchema();

        // Create a record using PUT and a user ID
        String body = json("{ type: 'b$book', fields: { 'b$title' : 'Faster Fishing' }, namespaces : { 'org.lilyproject.resttest': 'b' } }");
        ResponseAndContent response = put(BASE_URI + "/record/USER.faster_fishing", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        // Read the record
        response = get(BASE_URI + "/record/USER.faster_fishing");
        assertStatus(HttpStatus.SC_OK, response);

        // Verify content
        JsonNode json = readJson(response);
        assertEquals(1L, json.get("fields").size());
        assertNull(json.get("schema")); // schema info should not be included by default

        // Read the record with schema info
        response = get(BASE_URI + "/record/USER.faster_fishing?schema=true");
        assertStatus(HttpStatus.SC_OK, response);

        // Verify content
        json = readJson(response);
        assertEquals(1L, json.get("fields").size());
        assertNotNull(json.get("schema"));
        assertEquals(1L, json.get("schema").size());

        // Read the record without namespace prefixes
        response = get(BASE_URI + "/record/USER.faster_fishing?nsprefixes=false");
        assertStatus(HttpStatus.SC_OK, response);

        // Verify content
        json = readJson(response);
        assertEquals("Faster Fishing", json.get("fields").get("{org.lilyproject.resttest}title").getTextValue());

        // Read the record as specific version
        response = get(BASE_URI + "/record/USER.faster_fishing/version/1");
        assertStatus(HttpStatus.SC_OK, response);

        // Read a non-existing version
        response = get(BASE_URI + "/record/USER.faster_fishing/version/10");
        assertStatus(HttpStatus.SC_NOT_FOUND, response);

        // Specify non-numeric version
        response = get(BASE_URI + "/record/USER.faster_fishing/version/abc");
        assertStatus(HttpStatus.SC_NOT_FOUND, response);

        // Update the record
        body = json("{ action: 'update', record: { type: 'b$book', fields: { 'b$title' : 'Faster Fishing (new)' }, " +
                "namespaces : { 'org.lilyproject.resttest': 'b' } } }");
        response = post(BASE_URI + "/record/USER.faster_fishing", body);
        assertStatus(HttpStatus.SC_OK, response);

        json = readJson(response);
        assertEquals(2L, json.get("version").getLongValue());

        response = get(BASE_URI + "/record/USER.faster_fishing/version/2");
        assertStatus(HttpStatus.SC_OK, response);

        // Update the record, specify the ID both in URI and json
        body = json("{ action: 'update', record: { id: 'USER.faster_fishing', type: 'b$book', " +
                "fields: { 'b$title' : 'Faster Fishing (new 2)' }, " +
                "namespaces : { 'org.lilyproject.resttest': 'b' } } }");
        response = post(BASE_URI + "/record/USER.faster_fishing", body);
        assertStatus(HttpStatus.SC_OK, response);

        // Update the record, specify the ID both in URI and json but both different -- should be a bad request
        body = json("{ action: 'update', record: { id: 'USER.faster_fishing_smth', type: 'b$book', " +
                "fields: { 'b$title' : 'Faster Fishing (new 2)' }, " +
                "namespaces : { 'org.lilyproject.resttest': 'b' } } }");
        response = post(BASE_URI + "/record/USER.faster_fishing", body);
        assertStatus(HttpStatus.SC_BAD_REQUEST, response);

        // Update a record which does not exist, should fail
        body = json("{ action: 'update', record: { type: 'b$book', fields: { 'b$title' : 'Faster Fishing (new 3)' }, " +
                "namespaces : { 'org.lilyproject.resttest': 'b' } } }");
        response = post(BASE_URI + "/record/USER.non_existing_record", body);
        assertStatus(HttpStatus.SC_NOT_FOUND, response);

        // Delete the record
        response = delete(BASE_URI + "/record/USER.faster_fishing");
        assertStatus(HttpStatus.SC_NO_CONTENT, response);

        // Verify deleted record is gone
        response = get(BASE_URI + "/record/USER.faster_fishing");
        assertStatus(HttpStatus.SC_NOT_FOUND, response);

        // Test delete of non-existing record
        response = delete(BASE_URI + "/record/USER.faster_fishing");
        assertStatus(HttpStatus.SC_NOT_FOUND, response);

        // Create a record using PUT and a client-specified UUID
        UUID uuid = UUID.randomUUID();
        body = json("{ type: 'b$book', fields: { 'b$title' : 'Title 1' }, namespaces : { 'org.lilyproject.resttest': 'b' } }");
        response = put(BASE_URI + "/record/UUID." + uuid.toString(), body);
        assertStatus(HttpStatus.SC_CREATED, response);

        response = get(BASE_URI + "/record/UUID." + uuid.toString());
        assertStatus(HttpStatus.SC_OK, response);

        // Create a record using POST and a client-specified UUID
        uuid = UUID.randomUUID();
        body = json("{ action: 'create', record: { id: 'UUID." + uuid.toString() + "', type: 'b$book', " +
                "fields: { 'b$title' : 'Title 1' }, namespaces : { 'org.lilyproject.resttest': 'b' } } }");
        response = post(BASE_URI + "/record", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        response = get(BASE_URI + "/record/UUID." + uuid.toString());
        assertStatus(HttpStatus.SC_OK, response);

        // Update this record, specify the ID both in URI and json
        body = json("{ action: 'update', record: { id: 'UUID." + uuid.toString() + "', type: 'b$book', " +
                "fields: { 'b$title' : 'Title 1 (new)' }, " +
                "namespaces : { 'org.lilyproject.resttest': 'b' } } }");
        response = post(BASE_URI + "/record/UUID." + uuid.toString(), body);
        assertStatus(HttpStatus.SC_OK, response);
    }

    @Test
    public void testDeleteFields() throws Exception {
        // Create two field types
        String body = json("{action: 'create', fieldType: {name: 'n$del_field1', valueType: 'STRING', " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        ResponseAndContent response = post(BASE_URI + "/schema/fieldTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        body = json("{action: 'create', fieldType: {name: 'n$del_field2', valueType: 'STRING', " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        response = post(BASE_URI + "/schema/fieldTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        // Create a record type
        body = json("{action: 'create', recordType: {name: 'n$del', " +
                "fields: [ {name: 'n$del_field1'}, {name: 'n$del_field2'} ]," +
                "namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        response = post(BASE_URI + "/schema/recordType", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        // Create a record with a value for the two fields
        body = json("{ action: 'create', record: { type: 'n$del', " +
                "fields: { 'n$del_field1' : 'foo', 'n$del_field2': 'bar' }, namespaces : { 'org.lilyproject.resttest': 'n' } } }");
        response = post(BASE_URI + "/record", body);
        assertStatus(HttpStatus.SC_CREATED, response);
        String uri = response.getLocationRef().toString();
        JsonNode json = readJson(response);
        assertEquals(2L, json.get("fields").size());

        // Delete one of the fields. Do this 2 times, the second time should have no effect.
        for (int i = 0; i < 2; i++) {
            body = json("{ fieldsToDelete: ['n$del_field2'], namespaces : { 'org.lilyproject.resttest': 'n' } }");
            response = put(uri, body);
            assertStatus(HttpStatus.SC_OK, response);

            // TODO FIXME we need to re-read the record here since the returned record contains only submitted fields
            response = get(uri);
            assertStatus(HttpStatus.SC_OK, response);
            json = readJson(response);
            assertEquals(1L, json.get("fields").size());
            assertEquals(2L, json.get("version").getLongValue());
        }
    }

    /**
     * Test versioning of record types and the creation of a record that uses the non-latest version of a record type.
     */
    @Test
    public void testVersionRecordType() throws Exception {
        // Create two field types
        String body = json("{action: 'create', fieldType: {name: 'n$vrt_field1', valueType: 'STRING', " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        ResponseAndContent response = post(BASE_URI + "/schema/fieldTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        body = json("{action: 'create', fieldType: {name: 'n$vrt_field2', valueType: 'STRING', " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        response = post(BASE_URI + "/schema/fieldTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        // Create a record type
        body = json("{action: 'create', recordType: {name: 'n$vrt', " +
                "fields: [ {name: 'n$vrt_field1'} ]," +
                "namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        response = post(BASE_URI + "/schema/recordTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);
        String rtIdUri = response.getLocationRef().toString();

        body = json("{name: 'n$vrt', " +
                "fields: [ {name: 'n$vrt_field1'}, {name: 'n$vrt_field2'} ]," +
                "namespaces: { 'org.lilyproject.resttest': 'n' } }");
        response = put(BASE_URI + "/schema/recordType/n$vrt?ns.n=org.lilyproject.resttest", body);
        assertStatus(HttpStatus.SC_OK, response);

        response = get(BASE_URI + "/schema/recordType/n$vrt?ns.n=org.lilyproject.resttest");
        assertStatus(HttpStatus.SC_OK, response);
        JsonNode json = readJson(response);
        assertEquals(2L, json.get("version").getLongValue());

        response = get(BASE_URI + "/schema/recordType/n$vrt/version/2?ns.n=org.lilyproject.resttest");
        assertStatus(HttpStatus.SC_OK, response);
        json = readJson(response);
        assertEquals(2L, json.get("version").getLongValue());

        response = get(rtIdUri + "/version/2");
        assertStatus(HttpStatus.SC_OK, response);
        json = readJson(response);
        assertEquals(2L, json.get("version").getLongValue());

        // Create record
        body = json("{ type: {name: 'n$vrt', version: 1}, " +
                "fields: { 'n$vrt_field1' : 'pink shoe laces' }, namespaces : { 'org.lilyproject.resttest': 'n' } }");
        response = put(BASE_URI + "/record/USER.pink_shoe_laces", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        json = readJson(response);
        assertEquals(1L, json.get("type").get("version").getLongValue());

        // Update record
        body = json("{ type: {name: 'n$vrt', version: 1}, " +
                "fields: { 'n$vrt_field1' : 'pink shoe laces 2' }, namespaces : { 'org.lilyproject.resttest': 'n' } }");
        response = put(BASE_URI + "/record/USER.pink_shoe_laces", body);
        assertStatus(HttpStatus.SC_OK, response);

        json = readJson(response);
        assertEquals(1L, json.get("type").get("version").getLongValue());

        // Update without specifying version, should move to last version
        body = json("{ type: {name: 'n$vrt'}, " +
                "fields: { 'n$vrt_field1' : 'pink shoe laces 3' }, namespaces : { 'org.lilyproject.resttest': 'n' } }");
        response = put(BASE_URI + "/record/USER.pink_shoe_laces", body);
        assertStatus(HttpStatus.SC_OK, response);

        json = readJson(response);
        assertEquals(2L, json.get("type").get("version").getLongValue());
    }

    /**
     * Tests reading and writing each type of field value.
     */
    @Test
    public void testTypes() throws Exception {
        String[] types = {"STRING", "INTEGER", "LONG", "DOUBLE", "DECIMAL", "BOOLEAN", "URI", "DATETIME", "DATE", "LINK"};

        for (String type : types) {
            String body = json("{action: 'create', fieldType: {name: 'n$f" + type +
                    "', valueType: '" + type + "', " +
                    "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } } }");
            ResponseAndContent response = post(BASE_URI + "/schema/fieldTypeById", body);
            assertStatus(HttpStatus.SC_CREATED, response);
        }

        String body = json("{action: 'create', recordType: {name: 'n$types', fields: [" +
                " {name: 'n$fSTRING'}, {name: 'n$fINTEGER'}, {name: 'n$fLONG'}, {name: 'n$fDOUBLE'}" +
                " , {name: 'n$fDECIMAL'}, {name: 'n$fBOOLEAN'}, {name: 'n$fURI'}, {name: 'n$fDATETIME'}, " +
                " {name: 'n$fDATE'}, {name: 'n$fLINK'} ]," +
                "namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        ResponseAndContent response = post(BASE_URI + "/schema/recordTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        body = json("{ action: 'create', record: { type: 'n$types', fields: { " +
                "'n$fSTRING' : 'a string'," +
                "'n$fINTEGER' : 55," +
                "'n$fLONG' : " + Long.MAX_VALUE + "," +
                "'n$fDOUBLE' : 33.26," +
                "'n$fDECIMAL' : 7.7777777777777777777777777," +
                "'n$fBOOLEAN' : true," +
                "'n$fURI' : 'http://www.lilyproject.org/'," +
                "'n$fDATETIME' : '2010-08-28T21:32:49Z'," +
                "'n$fDATE' : '2010-08-28'," +
                "'n$fLINK' : 'USER.foobar.!*,arg1=val1,+arg2,-arg3'" +
                "}, namespaces : { 'org.lilyproject.resttest': 'n' } } }");
        response = post(BASE_URI + "/record", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        response = get(response.getLocationRef().toString());
        assertStatus(HttpStatus.SC_OK, response);

        JsonNode json = readJson(response);
        ObjectNode fieldsNode = (ObjectNode)json.get("fields");
        String prefix = json.get("namespaces").get("org.lilyproject.resttest").getTextValue();
        assertEquals("a string", fieldsNode.get(prefix + "$fSTRING").getTextValue());
        assertEquals(55, fieldsNode.get(prefix + "$fINTEGER").getIntValue());
        assertEquals(Long.MAX_VALUE, fieldsNode.get(prefix + "$fLONG").getLongValue());
        assertEquals(33.26, fieldsNode.get(prefix + "$fDOUBLE").getDoubleValue(), 0.0001);
        assertEquals(new BigDecimal("7.7777777777777777777777777"), fieldsNode.get(prefix + "$fDECIMAL").getDecimalValue());
        assertTrue(fieldsNode.get(prefix + "$fBOOLEAN").getBooleanValue());
        assertEquals(new URI("http://www.lilyproject.org/"), new URI(fieldsNode.get(prefix + "$fURI").getTextValue()));
        assertEquals(new DateTime("2010-08-28T21:32:49Z"), new DateTime(fieldsNode.get(prefix + "$fDATETIME").getTextValue()));
        assertEquals(new LocalDate("2010-08-28"), new LocalDate(fieldsNode.get(prefix + "$fDATE").getTextValue()));
        assertEquals("USER.foobar.!*,arg1=val1,+arg2,-arg3", fieldsNode.get(prefix + "$fLINK").getTextValue());
    }

    @Test
    public void testMultiValueAndHierarchical() throws Exception {
        // Multi-value field
        String body = json("{action: 'create', fieldType: {name: 'n$multiValue', " +
                "valueType: 'LIST<STRING>', " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        ResponseAndContent response = post(BASE_URI + "/schema/fieldTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        // Hierarchical field
        body = json("{action: 'create', fieldType: {name: 'n$hierarchical', " +
                "valueType: 'PATH<STRING>', " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        response = post(BASE_URI + "/schema/fieldTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        // Multi-value + hierarchical field
        body = json("{action: 'create', fieldType: {name: 'n$multiValueHierarchical', " +
                "valueType: 'LIST<PATH<STRING>>', " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        response = post(BASE_URI + "/schema/fieldTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        // Combine them into a record type
        body = json("{action: 'create', recordType: {name: 'n$mvAndHier', " +
                "fields: [ {name: 'n$multiValue'}, {name: 'n$hierarchical'}, {name: 'n$multiValueHierarchical'} ]," +
                "namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        response = post(BASE_URI + "/schema/recordTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        // Create a record
        body = json("{ type: 'n$mvAndHier', " +
                "fields: { 'n$multiValue' : ['val1', 'val2', 'val3']," +
                " 'n$hierarchical' : ['part1', 'part2', 'part3'], " +
                " 'n$multiValueHierarchical' : [['partA', 'partB'],['partC','partD']]" +
                " }, namespaces : { 'org.lilyproject.resttest': 'n' } }");
        response = put(BASE_URI + "/record/USER.multiValueHierarchical", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        // Read the record
        response = get(BASE_URI + "/record/USER.multiValueHierarchical");
        assertStatus(HttpStatus.SC_OK, response);

        JsonNode json = readJson(response);
        JsonNode fieldsNode = json.get("fields");
        String prefix = json.get("namespaces").get("org.lilyproject.resttest").getTextValue();

        ArrayNode mv = (ArrayNode)fieldsNode.get(prefix + "$multiValue");
        assertEquals("val1", mv.get(0).getTextValue());
        assertEquals("val2", mv.get(1).getTextValue());
        assertEquals("val3", mv.get(2).getTextValue());

        ArrayNode hier = (ArrayNode)fieldsNode.get(prefix + "$hierarchical");
        assertEquals("part1", hier.get(0).getTextValue());
        assertEquals("part2", hier.get(1).getTextValue());
        assertEquals("part3", hier.get(2).getTextValue());

        ArrayNode mvAndHier = (ArrayNode)fieldsNode.get(prefix + "$multiValueHierarchical");
        assertEquals(2, mvAndHier.size());
        ArrayNode mv1 = (ArrayNode)mvAndHier.get(0);
        assertEquals("partA", mv1.get(0).getTextValue());
        assertEquals("partB", mv1.get(1).getTextValue());
        ArrayNode mv2 = (ArrayNode)mvAndHier.get(1);
        assertEquals("partC", mv2.get(0).getTextValue());
        assertEquals("partD", mv2.get(1).getTextValue());
    }

    @Test
    public void testBlobs() throws Exception {
        // Create a blob field type
        String body = json("{action: 'create', fieldType: {name: 'b$blob1', valueType: 'BLOB', " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'b' } } }");
        ResponseAndContent response = post(BASE_URI + "/schema/fieldTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        // Create a record type holding the blob field
        body = json("{action: 'create', recordType: {name: 'b$blobRT', fields: [ {name: 'b$blob1'} ]," +
                "namespaces: { 'org.lilyproject.resttest': 'b' } } }");
        response = post(BASE_URI + "/schema/recordTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        // Upload a blob
        String data = "Hello, blob world!";
        response = postText(BASE_URI + "/blob", data);

        assertStatus(HttpStatus.SC_OK, response);
        JsonNode jsonNode = readJson(response);
        String blobValue = jsonNode.get("value").getTextValue();
        assertEquals("text/plain", jsonNode.get("mediaType").getTextValue());
        assertEquals((long)data.length(), jsonNode.get("size").getLongValue());

        // Create a record with this blob
        ObjectNode recordNode = JsonNodeFactory.instance.objectNode();
        recordNode.put("type", "b$blobRT");
        ObjectNode fieldsNode = recordNode.putObject("fields");
        ObjectNode blobNode = fieldsNode.putObject("b$blob1");
        blobNode.put("size", data.length());
        blobNode.put("mediaType", "text/plain");
        blobNode.put("value", blobValue);
        blobNode.put("name", "helloworld.txt");
        ObjectNode nsNode = recordNode.putObject("namespaces");
        nsNode.put("org.lilyproject.resttest", "b");

        response = put(BASE_URI + "/record/USER.blob1", recordNode.toString());
        assertStatus(HttpStatus.SC_CREATED, response);

        // Read the record
        response = get(BASE_URI + "/record/USER.blob1");
        assertStatus(HttpStatus.SC_OK, response);

        jsonNode = readJson(response);
        String prefix = jsonNode.get("namespaces").get("org.lilyproject.resttest").getValueAsText();
        blobNode = (ObjectNode)jsonNode.get("fields").get(prefix + "$blob1");
        assertEquals("text/plain", blobNode.get("mediaType").getValueAsText());
        assertEquals(data.length(), blobNode.get("size").getLongValue());
        assertEquals(blobValue, blobNode.get("value").getTextValue());
        assertEquals("helloworld.txt", blobNode.get("name").getValueAsText());

        // Read the blob
        response = get(BASE_URI + "/record/USER.blob1/field/b$blob1/data?ns.b=org.lilyproject.resttest");
        assertStatus(HttpStatus.SC_OK, response);
        assertEquals(data, new String(response.getContent()));
    }

    @Test
    public void testMultiValueHierarchicalBlobs() throws Exception {
        // Create a blob field type
        String body = json("{action: 'create', fieldType: {name: 'b$blob2', valueType: 'LIST<PATH<BLOB>>', " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'b' } } }");
        ResponseAndContent response = post(BASE_URI + "/schema/fieldTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        // Create a record type holding the blob field
        body = json("{action: 'create', recordType: {name: 'b$blobRT2', fields: [ {name: 'b$blob2'} ]," +
                "namespaces: { 'org.lilyproject.resttest': 'b' } } }");
        response = post(BASE_URI + "/schema/recordTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        // Upload some blobs
        ObjectNode blob1Value = (ObjectNode)readJson(postText(BASE_URI + "/blob", "My blob 1"));
        ObjectNode blob2Value = (ObjectNode)readJson(postText(BASE_URI + "/blob", "My blob 2"));
        ObjectNode blob3Value = (ObjectNode)readJson(postText(BASE_URI + "/blob", "My blob 3"));
        ObjectNode blob4Value = (ObjectNode)readJson(postText(BASE_URI + "/blob", "My blob 4"));

        // Create a record with these blobs
        ObjectNode recordNode = JsonNodeFactory.instance.objectNode();
        recordNode.put("type", "b$blobRT2");
        ObjectNode fieldsNode = recordNode.putObject("fields");

        ArrayNode blobNode = fieldsNode.putArray("b$blob2");

        ArrayNode hierarchicalBlob1 = blobNode.addArray();
        hierarchicalBlob1.add(blob1Value);
        hierarchicalBlob1.add(blob2Value);

        ArrayNode hierarchicalBlob2 = blobNode.addArray();
        hierarchicalBlob2.add(blob3Value);
        hierarchicalBlob2.add(blob4Value);

        ObjectNode nsNode = recordNode.putObject("namespaces");
        nsNode.put("org.lilyproject.resttest", "b");

        response = put(BASE_URI + "/record/USER.blob2", recordNode.toString());
        assertStatus(HttpStatus.SC_CREATED, response);

        // Read the record
        response = get(BASE_URI + "/record/USER.blob2");
        assertStatus(HttpStatus.SC_OK, response);

        JsonNode jsonNode = readJson(response);
        String prefix = jsonNode.get("namespaces").get("org.lilyproject.resttest").getValueAsText();
        blobNode = (ArrayNode)jsonNode.get("fields").get(prefix + "$blob2");
        assertEquals(2, blobNode.size());

        // Read the blobs
        for (int mvIndex = 0; mvIndex < 2; mvIndex++) {
            for (int hIndex = 0; hIndex < 2; hIndex++) {
                response = get(BASE_URI + "/record/USER.blob2/field/b$blob2/data?ns.b=org.lilyproject.resttest" +
                        "&indexes=" + mvIndex + "," + hIndex);
                assertStatus(HttpStatus.SC_OK, response);
            }
        }

        // Read a blob at non-existing indexes
        response = get(BASE_URI + "/record/USER.blob2/field/b$blob2/data?ns.b=org.lilyproject.resttest&mvIndex=5&hIndex=3");
        assertStatus(HttpStatus.SC_NOT_FOUND, response);
    }

    @Test
    public void testVersionCollection() throws Exception {
        // Create some field types
        String body = json("{action: 'create', fieldType: {name: 'p$name', valueType: 'STRING', " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'p' } } }");
        ResponseAndContent response = post(BASE_URI + "/schema/fieldTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        body = json("{action: 'create', fieldType: {name: 'p$price', valueType: 'DOUBLE', " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'p' } } }");
        response = post(BASE_URI + "/schema/fieldTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        body = json("{action: 'create', fieldType: {name: 'p$colour', valueType: 'STRING', " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'p' } } }");
        response = post(BASE_URI + "/schema/fieldTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        // Create a record type
        body = json("{action: 'create', recordType: {name: 'p$product', fields: [ {name: 'p$name'}, " +
                "{name: 'p$price'}, {name: 'p$colour'} ], namespaces: { 'org.lilyproject.resttest': 'p' } } }");
        response = post(BASE_URI + "/schema/recordTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        // Create a record with some versions
        body = json("{ type: 'p$product', fields: { 'p$name' : 'Product 1' }, namespaces : { 'org.lilyproject.resttest': 'p' } }");
        response = put(BASE_URI + "/record/USER.product1", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        body = json("{ fields: { 'p$name' : 'Product 1', 'p$price': 5.5 }, namespaces : { 'org.lilyproject.resttest': 'p' } }");
        response = put(BASE_URI + "/record/USER.product1", body);
        assertStatus(HttpStatus.SC_OK, response);

        body = json("{ fields: { 'p$name' : 'Product 1', 'p$price': 5.5, 'p$colour': 'red' }, namespaces : { 'org.lilyproject.resttest': 'p' } }");
        response = put(BASE_URI + "/record/USER.product1", body);
        assertStatus(HttpStatus.SC_OK, response);

        // Get list of versions
        response = get(BASE_URI + "/record/USER.product1/version");
        assertStatus(HttpStatus.SC_OK, response);
        JsonNode json = readJson(response);
        assertEquals(3, json.get("results").size());
        JsonNode results = json.get("results");
        assertEquals(1, results.get(0).get("fields").size());
        assertEquals(2, results.get(1).get("fields").size());
        assertEquals(3, results.get(2).get("fields").size());

        response = get(BASE_URI + "/record/USER.product1/version?max-results=1");
        assertStatus(HttpStatus.SC_OK, response);
        json = readJson(response);
        assertEquals(1, json.get("results").size());
        results = json.get("results");
        assertEquals(1, results.get(0).get("version").getLongValue());

        response = get(BASE_URI + "/record/USER.product1/version?start-index=2&max-results=1");
        assertStatus(HttpStatus.SC_OK, response);
        json = readJson(response);
        assertEquals(1, json.get("results").size());
        results = json.get("results");
        assertEquals(2, results.get(0).get("version").getLongValue());

        // Retrieve only one field
        response = get(BASE_URI + "/record/USER.product1/version?fields=n$name&ns.n=org.lilyproject.resttest");
        assertStatus(HttpStatus.SC_OK, response);
        json = readJson(response);
        results = json.get("results");
        assertEquals(1, results.get(0).get("fields").size());
        assertEquals(1, results.get(1).get("fields").size());
        assertEquals(1, results.get(2).get("fields").size());
    }

    @Test
    public void testVariantCollection() throws Exception {
        makeBookSchema();

        String body = json("{ type: 'b$book', fields: { 'b$title' : 'Hunting' }, namespaces : { 'org.lilyproject.resttest': 'b' } }");
        ResponseAndContent response = put(BASE_URI + "/record/USER.hunting.lang=en", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        body = json("{ type: 'b$book', fields: { 'b$title' : 'Jagen' }, namespaces : { 'org.lilyproject.resttest': 'b' } }");
        response = put(BASE_URI + "/record/USER.hunting.lang=nl", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        body = json("{ type: 'b$book', fields: { 'b$title' : 'La chasse' }, namespaces : { 'org.lilyproject.resttest': 'b' } }");
        response = put(BASE_URI + "/record/USER.hunting.lang=fr", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        response = get(BASE_URI + "/record/USER.hunting/variant");
        assertStatus(HttpStatus.SC_OK, response);

        JsonNode jsonNode = readJson(response);
        ArrayNode resultsNode = (ArrayNode)jsonNode.get("results");

        assertEquals(3, resultsNode.size());

        // Keys are returned in storage order (though this is more of an implementation detail on which clients should not rely)
        assertEquals("USER.hunting.lang=en", resultsNode.get(0).get("id").getTextValue());
        assertEquals("USER.hunting.lang=fr", resultsNode.get(1).get("id").getTextValue());
        assertEquals("USER.hunting.lang=nl", resultsNode.get(2).get("id").getTextValue());
    }

    @Test
    public void testRecordByVTag() throws Exception {
        makeBookSchema();

        // Create 'active' vtag field
        String body = json("{action: 'create', fieldType: {name: 'v$active', valueType: 'LONG', " +
                "scope: 'non_versioned', namespaces: { 'org.lilyproject.vtag': 'v' } } }");
        ResponseAndContent response = post(BASE_URI + "/schema/fieldTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        body = json("{ type: 'b$book', fields: { 'b$title' : 'Title version 1' }, namespaces : { 'org.lilyproject.resttest': 'b' } }");
        response = put(BASE_URI + "/record/USER.vtagtest", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        body = json("{ type: 'b$book', fields: { 'b$title' : 'Title version 2', 'v$active': 1 }, " +
                "namespaces : { 'org.lilyproject.resttest': 'b', 'org.lilyproject.vtag': 'v' } }");
        response = put(BASE_URI + "/record/USER.vtagtest", body);
        assertStatus(HttpStatus.SC_OK, response);

        response = get(BASE_URI + "/record/USER.vtagtest/vtag/active");
        assertStatus(HttpStatus.SC_OK, response);
        assertEquals(1L, readJson(response).get("version").getLongValue());

        response = get(BASE_URI + "/record/USER.vtagtest/vtag/last");
        assertStatus(HttpStatus.SC_OK, response);
        assertEquals(2L, readJson(response).get("version").getLongValue());

        response = get(BASE_URI + "/record/USER.vtagtest/vtag/aNotExistingVTagName");
        assertStatus(HttpStatus.SC_NOT_FOUND, response);
    }

    @Test
    public void testVersionedMutableScope() throws Exception {
        // Create a versioned field type
        String body = json("{action: 'create', fieldType: {name: 'b$subject', valueType: 'STRING', " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'b' } } }");
        ResponseAndContent response = post(BASE_URI + "/schema/fieldTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        // Create a versioned-mutable field type
        body = json("{action: 'create', fieldType: {name: 'b$state', valueType: 'STRING', " +
                "scope: 'versioned_mutable', namespaces: { 'org.lilyproject.resttest': 'b' } } }");
        response = post(BASE_URI + "/schema/fieldTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        // Create a record type
        body = json("{action: 'create', recordType: {name: 'b$article', " +
                "fields: [ {name: 'b$subject'}, {name: 'b$state'} ]," +
                "namespaces: { 'org.lilyproject.resttest': 'b' } } }");
        response = post(BASE_URI + "/schema/recordTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        // Create a record
        body = json("{ type: 'b$article', fields: { 'b$subject': 'Peace', 'b$state': 'draft' }, namespaces : { 'org.lilyproject.resttest': 'b' } }");
        response = put(BASE_URI + "/record/USER.peace", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        // Read the record
        response = get(BASE_URI + "/record/USER.peace");
        assertStatus(HttpStatus.SC_OK, response);

        JsonNode json = readJson(response);
        JsonNode fieldsNode = json.get("fields");
        String prefix = json.get("namespaces").get("org.lilyproject.resttest").getTextValue();

        assertEquals("draft", fieldsNode.get(prefix + "$state").getValueAsText());
        assertEquals("Peace", fieldsNode.get(prefix + "$subject").getValueAsText());

        // Update the versioned-mutable field
        body = json("{ fields: { 'b$state': 'in_review' }, namespaces : { 'org.lilyproject.resttest': 'b' } }");
        response = put(BASE_URI + "/record/USER.peace/version/1", body);
        assertStatus(HttpStatus.SC_OK, response);

        response = get(BASE_URI + "/record/USER.peace");
        assertStatus(HttpStatus.SC_OK, response);

        json = readJson(response);
        fieldsNode = json.get("fields");
        prefix = json.get("namespaces").get("org.lilyproject.resttest").getTextValue();

        assertEquals(1L, json.get("version").getLongValue()); // no new version created
        assertEquals("in_review", fieldsNode.get(prefix + "$state").getValueAsText());

        // Update versioned-mutable field without changes, change to versioned field should be ignored
        body = json("{ fields: { 'b$subject': 'Peace2', 'b$state': 'in_review' }, namespaces : { 'org.lilyproject.resttest': 'b' } }");
        response = put(BASE_URI + "/record/USER.peace/version/1", body);
        assertStatus(HttpStatus.SC_OK, response);

        response = get(BASE_URI + "/record/USER.peace");
        assertStatus(HttpStatus.SC_OK, response);

        json = readJson(response);
        fieldsNode = json.get("fields");
        prefix = json.get("namespaces").get("org.lilyproject.resttest").getTextValue();

        assertEquals(1L, json.get("version").getLongValue()); // no new version created
        assertEquals("Peace", fieldsNode.get(prefix + "$subject").getValueAsText());
    }

    @Test
    public void testConditionUpdate() throws Exception {
        makeBookSchema();

        String body = json("{ type: 'b$book', fields: { 'b$title' : 'ABC1' }, namespaces : { 'org.lilyproject.resttest': 'b' } }");
        ResponseAndContent response = put(BASE_URI + "/record/USER.ABC", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        // Test update with failing condition
        body = json("{ action: 'update', record: { fields: { 'b$title' : 'ABC2' } }, " +
                "conditions: [{field: 'b$title', value: 'ABC5'}], " +
                "namespaces : { 'org.lilyproject.resttest': 'b' } }");

        response = post(BASE_URI + "/record/USER.ABC", body);
        assertStatus(HttpStatus.SC_CONFLICT, response);

        // Verify update did not go through
        response = get(BASE_URI + "/record/USER.ABC");
        assertStatus(HttpStatus.SC_OK, response);

        JsonNode json = readJson(response);
        assertEquals("ABC1", getFieldValue(json, "title").getTextValue());

        // Test update with succeeding condition
        body = json("{ action: 'update', record: { fields: { 'b$title' : 'ABC2' } }, " +
                "conditions: [{field: 'b$title', value: 'ABC1'}], " +
                "namespaces : { 'org.lilyproject.resttest': 'b' } }");

        response = post(BASE_URI + "/record/USER.ABC", body);

        assertStatus(HttpStatus.SC_OK, response);

        // Verify update did not through
        response = get(BASE_URI + "/record/USER.ABC");
        assertStatus(HttpStatus.SC_OK, response);

        json = readJson(response);
        assertEquals("ABC2", getFieldValue(json, "title").getTextValue());

        //
        // Test with custom compare operator
        //
        body = json("{ action: 'update', record: { fields: { 'b$title' : 'ABC3' } }, " +
                "conditions: [{field: 'b$title', value: 'ABC2', operator: 'not_equal'}], " +
                "namespaces : { 'org.lilyproject.resttest': 'b' } }");

        response = post(BASE_URI + "/record/USER.ABC", body);
        assertStatus(HttpStatus.SC_CONFLICT, response);

        //
        // Test allowMissing flag
        //
        body = json("{ action: 'update', record: { fields: { 'b$title' : 'ABC3' } }, " +
                "conditions: [{field: 'b$summary', value: 'some summary', allowMissing: false}], " +
                "namespaces : { 'org.lilyproject.resttest': 'b' } }");

        response = post(BASE_URI + "/record/USER.ABC", body);
        assertStatus(HttpStatus.SC_CONFLICT, response);

        body = json("{ action: 'update', record: { fields: { 'b$title' : 'ABC3' } }, " +
                "conditions: [{field: 'b$summary', value: 'some summary', allowMissing: true}], " +
                "namespaces : { 'org.lilyproject.resttest': 'b' } }");

        response = post(BASE_URI + "/record/USER.ABC", body);
        assertStatus(HttpStatus.SC_OK, response);

        //
        // Test null value
        //
        // First remove summary field again
        body = json("{ action: 'update', record: { " +
                "fieldsToDelete: ['b$summary'], " +
                "namespaces : { 'org.lilyproject.resttest': 'b' } } }");
        response = post(BASE_URI + "/record/USER.ABC", body);
        assertStatus(HttpStatus.SC_OK, response);

        body = json("{ action: 'update', record: { fields: { 'b$title' : 'ABC4' } }, " +
                "conditions: [{field: 'b$summary', value: null, operator: 'not_equal'}], " +
                "namespaces : { 'org.lilyproject.resttest': 'b' } } }");

        response = post(BASE_URI + "/record/USER.ABC", body);
        assertStatus(HttpStatus.SC_CONFLICT, response);

        body = json("{ action: 'update', record: { fields: { 'b$title' : 'ABC4' } }, " +
                "conditions: [{field: 'b$summary', value: null, operator: 'equal'}], " +
                "namespaces : { 'org.lilyproject.resttest': 'b' } } }");

        response = post(BASE_URI + "/record/USER.ABC", body);
        assertStatus(HttpStatus.SC_OK, response);

        //
        // Test system field check
        //
        // Create a new record
        body = json("{ type: 'b$book', fields: { 'b$title' : 'ABC1' }, namespaces : { 'org.lilyproject.resttest': 'b' } }");
        response = put(BASE_URI + "/record/USER.sysfieldcheck", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        // Test update with failing condition
        body = json("{ action: 'update', record: { fields: { 'b$title' : 'ABC2' } }, " +
                "conditions: [{field: 's$version', value: 2}], " +
                "namespaces : { 'org.lilyproject.resttest': 'b', 'org.lilyproject.system': 's' } }");

        response = post(BASE_URI + "/record/USER.sysfieldcheck", body);
        assertStatus(HttpStatus.SC_CONFLICT, response);

        // Test update with succeeding condition
        body = json("{ action: 'update', record: { fields: { 'b$title' : 'ABC2' } }, " +
                "conditions: [{field: 's$version', value: 1}], " +
                "namespaces : { 'org.lilyproject.resttest': 'b', 'org.lilyproject.system': 's' } }");

        response = post(BASE_URI + "/record/USER.sysfieldcheck", body);
        assertStatus(HttpStatus.SC_OK, response);
    }

    @Test
    public void testConditionDelete() throws Exception {
        makeBookSchema();

        String body = json("{ type: 'b$book', fields: { 'b$title' : 'CondDel' }, namespaces : { 'org.lilyproject.resttest': 'b' } }");
        ResponseAndContent response = put(BASE_URI + "/record/USER.ConDel", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        body = json("{ action: 'delete'," +
                "conditions: [{field: 'b$title', value: 'foo'}], " +
                "namespaces : { 'org.lilyproject.resttest': 'b' } } }");

        response = post(BASE_URI + "/record/USER.ConDel", body);
        assertStatus(HttpStatus.SC_CONFLICT, response);

        body = json("{ action: 'delete'," +
                "conditions: [{field: 'b$title', value: 'CondDel'}], " +
                "namespaces : { 'org.lilyproject.resttest': 'b' } } }");

        response = post(BASE_URI + "/record/USER.ConDel", body);
        assertStatus(HttpStatus.SC_NO_CONTENT, response);
    }

    private void setupRecordScannerTest () throws Exception{
        makeBookSchema();

        // Create a record using PUT and a user ID
        //TODO: validate response
        String body = json("{ type: 'b$book', fields: { 'b$title' : 'Faster Fishing' }, namespaces : { 'org.lilyproject.resttest': 'b' } }");
        ResponseAndContent response = put(BASE_URI + "/record/USER.scan_faster_fishing", body);
        body = json("{ type: 'b$book', fields: { 'b$title' : 'Fister Fashing' }, namespaces : { 'org.lilyproject.resttest': 'b' } }");
        response = put(BASE_URI + "/record/USER.scan_fister_fashing", body);
        body = json("{ type: 'b$book', fields: { 'b$title' : 'Fly fishing with Flash' }, namespaces : { 'org.lilyproject.resttest': 'b' } }");
        response = put(BASE_URI + "/record/USER.scan_fly_fishing_with_flash", body);
    }

    @Test
    public void testRecordScanPost() throws Exception {
        setupRecordScannerTest();

        // Create a scanner to see if it gest created. Check the location header
        String body = json("{'recordFilter' : { '@class' : 'org.lilyproject.repository.api.filter.RecordIdPrefixFilter', " +
                "'recordId' : 'USER.scan_'}}, 'caching' : 1024, 'cacheBlocks' : false}");
        ResponseAndContent response = post(BASE_URI + "/scan", body);
        assertStatus(HttpStatus.SC_CREATED, response);
        String location = response.getLocationRef().toString();
        assertTrue(location.contains("/repository/scan/"));
    }

    @Test
    public void testRecordScanGet() throws Exception {
        setupRecordScannerTest();

     // Create a scanner to see if it gest created. Check the location header
        String body = json("{'recordFilter' : { '@class' : 'org.lilyproject.repository.api.filter.RecordIdPrefixFilter', " +
                "'recordId' : 'USER.scan_'}}, 'caching' : 1024, 'cacheBlocks' : false}");
        ResponseAndContent response = post(BASE_URI + "/scan", body);
        assertStatus(HttpStatus.SC_CREATED, response);
        String location = response.getLocationRef().toString();

     // Check if the scanner can be retrieved. Retrieve 1 record by default
        response = get(location);
        assertStatus(HttpStatus.SC_OK, response);
        JsonNode json = readJson(response);
        assertTrue(json.get("results").size() == 1);

        // Check to see if the batch parameter gets more records
        response = get(location + "?batch=2");
        assertStatus(HttpStatus.SC_OK, response);
        json = readJson(response);
        assertTrue(json.get("results").size() == 2);

        // When the scanner runs out send 204 NO CONTENT
        response = get(location);
        assertStatus(HttpStatus.SC_NO_CONTENT, response);

        // GETs on non existing scanners get 404
        response = get(location + "blablabla");
        assertStatus(HttpStatus.SC_NOT_FOUND, response);

    }

    @Test
    public void testRecordScanDelete() throws Exception {
        setupRecordScannerTest();

        // Create a scanner to see if it gest created. Check the location header
        String body = json("{'recordFilter' : { '@class' : 'org.lilyproject.repository.api.filter.RecordIdPrefixFilter', " +
                "'recordId' : 'USER.scan_'}}, 'caching' : 1024, 'cacheBlocks' : false}");
        ResponseAndContent response = post(BASE_URI + "/scan", body);
        assertStatus(HttpStatus.SC_CREATED, response);
        String location = response.getLocationRef().toString();

        // Delete scanner
        response = delete(BASE_URI + location);
        assertStatus(HttpStatus.SC_OK, response);

        // Check that scanner is gone
        response = get(BASE_URI + location);
        assertStatus(HttpStatus.SC_NOT_FOUND, response);

        // Delete a non existing scanner gets a 404
        response = delete(BASE_URI + location + "not_here");
        assertStatus(HttpStatus.SC_NOT_FOUND, response);
    }

    private JsonNode getFieldValue(JsonNode recordJson, String fieldName) {
        String prefix = recordJson.get("namespaces").get("org.lilyproject.resttest").getTextValue();
        JsonNode fieldsNode = recordJson.get("fields");
        return fieldsNode.get(prefix + "$" + fieldName);
    }

    private void assertStatus(int expectedStatus, ResponseAndContent response) throws IOException {
        if (expectedStatus != response.getResponse().getStatusLine().getStatusCode()) {
            System.err.println("Detected unexpected response status, body of the response is:");
            printErrorResponse(response);
            assertEquals(expectedStatus, response.getResponse().getStatusLine().getStatusCode());
        }
    }

    private void printErrorResponse(ResponseAndContent response) throws IOException {
        if (response.getResponse().getEntity() != null && "application/json".equals(response.getResponse().getEntity().getContentType().getValue())) {
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

    private ResponseAndContent post(String uri, String body) throws Exception {
        HttpPost post = new HttpPost(uri);
        post.setEntity(new StringEntity(body, "application/json", "UTF-8"));
        return processResponseAndContent(post);
    }

    private ResponseAndContent postText(String uri, String body) throws Exception {
        HttpPost post = new HttpPost(uri);
        post.setEntity(new StringEntity(body, "text/plain", "UTF-8"));
        return processResponseAndContent(post);
    }

    private ResponseAndContent put(String uri, String body) throws Exception {
        HttpPut put = new HttpPut(uri);
        put.setEntity(new StringEntity(body, "application/json", "UTF-8"));
        return processResponseAndContent(put);
    }

    private ResponseAndContent get(String uri) throws Exception {
        return processResponseAndContent(new HttpGet(uri));
    }

    private ResponseAndContent delete(String uri) throws Exception {
        return processResponseAndContent(new HttpDelete(uri));
    }

    public static JsonNode readJson(ResponseAndContent rac) throws IOException {
        return JsonFormat.deserialize(rac.getContent());
    }

    private String json(String input) {
        return input.replaceAll("'", "\"");
    }

    private ResponseAndContent processResponseAndContent(HttpUriRequest request) throws Exception {
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
