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

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

import org.lilyproject.lilyservertestfw.KauriTestUtility;
import org.lilyproject.hadooptestfw.HBaseProxy;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.json.JsonFormat;
import org.restlet.Client;
import org.restlet.Request;
import org.restlet.Response;
import org.restlet.data.MediaType;
import org.restlet.data.Method;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class RestTest {
    private static HBaseProxy HBASE_PROXY;
    private final static KauriTestUtility KAURI_TEST_UTIL = new KauriTestUtility("../server/");
    private static String BASE_URI;

    private static Client CLIENT;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        HBASE_PROXY = new HBaseProxy();
        HBASE_PROXY.start();

        KAURI_TEST_UTIL.createDefaultConf(HBASE_PROXY);
        KAURI_TEST_UTIL.start();

        CLIENT = KAURI_TEST_UTIL.getClient();

        BASE_URI = "http://localhost:" + KAURI_TEST_UTIL.getPort() + "/repository";
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        try {
            KAURI_TEST_UTIL.stop();
        } catch (Throwable t) {
            t.printStackTrace();
        }

        try {
            HBASE_PROXY.stop();
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    @Test
    public void testGeneralErrors() throws Exception {
        // Perform request to non-existing resource class
        Response response = get(BASE_URI + "/foobar");
        assertStatus(Status.CLIENT_ERROR_NOT_FOUND, response);

        // Submit invalid json
        String body = json("{ f [ }");
        response = post(BASE_URI + "/schema/fieldType", body);
        assertStatus(Status.CLIENT_ERROR_BAD_REQUEST, response);
    }

    @Test
    public void testFieldTypes() throws Exception {
        // Create field type using POST
        String body = json("{action: 'create', fieldType: {name: 'n$field1', valueType: { primitive: 'STRING' }, " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        Response response = post(BASE_URI + "/schema/fieldTypeById", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        JsonNode json = readJson(response.getEntity());

        // verify location header
        assertEquals(BASE_URI + "/schema/fieldTypeById/" + json.get("id").getValueAsText(), response.getLocationRef().toString());

        // verify name
        String prefix = json.get("namespaces").get("org.lilyproject.resttest").getTextValue();
        assertEquals(prefix + "$field1", json.get("name").getTextValue());

        // Create field type using POST on the name-based resource
        body = json("{action: 'create', fieldType: {name: 'n$field1a', valueType: { primitive: 'STRING' }, " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        response = post(BASE_URI + "/schema/fieldType", body);
        assertStatus(Status.SUCCESS_CREATED, response);
        assertEquals(BASE_URI + "/schema/fieldType/n$field1a?ns.n=org.lilyproject.resttest", response.getLocationRef().toString());

        // Create field type using PUT
        body = json("{name: 'n$field2', valueType: { primitive: 'STRING' }, " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } }");
        response = put(BASE_URI + "/schema/fieldType/n$field2?ns.n=org.lilyproject.resttest", body);
        String fieldType2Id = readJson(response.getEntity()).get("id").getValueAsText();
        assertStatus(Status.SUCCESS_CREATED, response);
        assertEquals(BASE_URI + "/schema/fieldType/n$field2?ns.n=org.lilyproject.resttest", response.getLocationRef().toString());

        // Update a field type - by name : change field2 to field3
        body = json("{name: 'n$field3', valueType: { primitive: 'STRING' }, " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } }");
        response = put(BASE_URI + "/schema/fieldType/n$field2?ns.n=org.lilyproject.resttest", body);
        assertStatus(Status.REDIRECTION_PERMANENT, response);
        assertEquals(BASE_URI + "/schema/fieldType/n$field3?ns.n=org.lilyproject.resttest", response.getLocationRef().toString());

        response = get(BASE_URI + "/schema/fieldType/n$field2?ns.n=org.lilyproject.resttest");
        assertStatus(Status.CLIENT_ERROR_NOT_FOUND, response);

        response = get(BASE_URI + "/schema/fieldType/n$field3?ns.n=org.lilyproject.resttest");
        assertStatus(Status.SUCCESS_OK, response);

        // Update a field type - by ID : change field3 to field4
        body = json("{name: 'n$field4', valueType: { primitive: 'STRING' }, " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } }");
        response = put(BASE_URI + "/schema/fieldTypeById/" + fieldType2Id, body);
        assertStatus(Status.SUCCESS_OK, response);

        // Test updating immutable properties
        body = json("{name: 'n$field4', valueType: { primitive: 'INTEGER' }, " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } }");
        response = put(BASE_URI + "/schema/fieldTypeById/" + fieldType2Id, body);
        assertStatus(Status.CLIENT_ERROR_CONFLICT, response);

        body = json("{name: 'n$field4', valueType: { primitive: 'STRING' }, " +
                "scope: 'non_versioned', namespaces: { 'org.lilyproject.resttest': 'n' } }");
        response = put(BASE_URI + "/schema/fieldTypeById/" + fieldType2Id, body);
        assertStatus(Status.CLIENT_ERROR_CONFLICT, response);

        body = json("{name: 'n$field4', valueType: { primitive: 'STRING', multiValue: true }, " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } }");
        response = put(BASE_URI + "/schema/fieldTypeById/" + fieldType2Id, body);
        assertStatus(Status.CLIENT_ERROR_CONFLICT, response);

        body = json("{name: 'n$field4', valueType: { primitive: 'STRING', hierarchical: true }, " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } }");
        response = put(BASE_URI + "/schema/fieldType/n$field4?ns.n=org.lilyproject.resttest", body);
        assertStatus(Status.CLIENT_ERROR_CONFLICT, response);

        // Get list of field types
        response = get(BASE_URI + "/schema/fieldType");
        assertStatus(Status.SUCCESS_OK, response);
        json = readJson(response.getEntity());
        assertTrue(json.get("results").size() > 0);

        response = get(BASE_URI + "/schema/fieldTypeById");
        assertStatus(Status.SUCCESS_OK, response);
        json = readJson(response.getEntity());
        assertTrue(json.get("results").size() > 0);
    }

    @Test
    public void testRecordTypes() throws Exception {
        // Create some field types
        List<String> fieldTypeIds = new ArrayList<String>();
        for (int i = 1; i < 4; i++) {
            String body = json("{action: 'create', fieldType: {name: 'n$rt_field" + i +
                    "', valueType: { primitive: 'STRING' }, " +
                    "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } } }");
            Response response = post(BASE_URI + "/schema/fieldTypeById", body);
            assertStatus(Status.SUCCESS_CREATED, response);
            JsonNode json = readJson(response.getEntity());
            fieldTypeIds.add(json.get("id").getValueAsText());
        }

        // Create a record type using POST
        String body = json("{action: 'create', recordType: {name: 'n$recordType1', fields: [ {name: 'n$rt_field1'} ]," +
                "namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        Response response = post(BASE_URI + "/schema/recordTypeById", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        JsonNode json = readJson(response.getEntity());

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
        assertStatus(Status.SUCCESS_CREATED, response);
        assertEquals(BASE_URI + "/schema/recordType/n$recordType1a?ns.n=org.lilyproject.resttest", response.getLocationRef().toString());

        // Create a record type using PUT
        body = json("{name: 'n$recordType2', fields: [ {name: 'n$rt_field1'} ]," +
                "namespaces: { 'org.lilyproject.resttest': 'n' } }");
        response = put(BASE_URI + "/schema/recordType/n$recordType2?ns.n=org.lilyproject.resttest", body);
        assertStatus(Status.SUCCESS_CREATED, response);
        assertEquals(BASE_URI + "/schema/recordType/n$recordType2?ns.n=org.lilyproject.resttest", response.getLocationRef().toString());
        json = readJson(response.getEntity());
        String secondRtId = json.get("id").getValueAsText();

        // Update a record type - by ID
        body = json("{name: 'n$recordType2', " +
                "fields: [ {name: 'n$rt_field1', mandatory: true}, " +
                " {name : 'n$rt_field2', mandatory: true} ], namespaces: { 'org.lilyproject.resttest': 'n' } }");
        response = put(BASE_URI + "/schema/recordTypeById/" + secondRtId, body);
        assertStatus(Status.SUCCESS_OK, response);
        json = readJson(response.getEntity());
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
        assertStatus(Status.REDIRECTION_PERMANENT, response);
        assertEquals(BASE_URI + "/schema/recordType/n$recordType3?ns.n=org.lilyproject.resttest", response.getLocationRef().toString());

        // Get list of record types
        response = get(BASE_URI + "/schema/recordType");
        assertStatus(Status.SUCCESS_OK, response);
        json = readJson(response.getEntity());
        assertTrue(json.get("results").size() > 0);

        response = get(BASE_URI + "/schema/recordTypeById");
        assertStatus(Status.SUCCESS_OK, response);
        json = readJson(response.getEntity());
        assertTrue(json.get("results").size() > 0);

        //
        // Test mixins
        //

        // Create two mixin record types
        body = json("{action: 'create', recordType: {name: 'n$mixin1', fields: [ {name: 'n$rt_field2'} ]," +
                "namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        response = post(BASE_URI + "/schema/recordTypeById", body);
        assertStatus(Status.SUCCESS_CREATED, response);
        String mixin1Id = readJson(response.getEntity()).get("id").getTextValue();

        body = json("{action: 'create', recordType: {name: 'n$mixin2', fields: [ {name: 'n$rt_field3'} ]," +
                "namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        response = post(BASE_URI + "/schema/recordTypeById", body);
        assertStatus(Status.SUCCESS_CREATED, response);
        String mixin2Id = readJson(response.getEntity()).get("id").getTextValue();

        // Create a record type using the mixins
        body = json("{action: 'create', recordType: {name: 'n$mixinUser', fields: [ {name: 'n$rt_field1'} ]," +
                "mixins: [{name: 'n$mixin1', version: 1}, { id: '" + mixin2Id + "' } ], " +
                "namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        response = post(BASE_URI + "/schema/recordTypeById", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        response = get(response.getLocationRef().toString());
        assertStatus(Status.SUCCESS_OK, response);
        json = readJson(response.getEntity());

        assertEquals(2, json.get("mixins").size());

        // Update to remove one of the mixins
        body = json("{name: 'n$mixinUser', fields: [ {name: 'n$rt_field1'} ]," +
                "mixins: [{name: 'n$mixin1', version: 1}], " +
                "namespaces: { 'org.lilyproject.resttest': 'n' } }");
        String mixinUserUri = BASE_URI + "/schema/recordType/n$mixinUser?ns.n=org.lilyproject.resttest";
        response = put(mixinUserUri, body);
        assertStatus(Status.SUCCESS_OK, response);

        response = get(mixinUserUri);
        assertStatus(Status.SUCCESS_OK, response);
        json = readJson(response.getEntity());

        assertEquals(1, json.get("mixins").size());
    }

    private void makeBookSchema() throws IOException {
        // Create field type
        String body = json("{name: 'b$title', valueType: { primitive: 'STRING' }, " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'b' } }");
        Response response = put(BASE_URI + "/schema/fieldType/b$title?ns.b=org.lilyproject.resttest", body);
        assertTrue(response.getStatus().isSuccess());

        // Create field type
        body = json("{name: 'b$summary', valueType: { primitive: 'STRING' }, " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'b' } }");
        response = put(BASE_URI + "/schema/fieldType/b$summary?ns.b=org.lilyproject.resttest", body);
        assertTrue(response.getStatus().isSuccess());

        // Create a record type
        body = json("{name: 'b$book', fields: [ {name: 'b$title'}, {name: 'b$summary'} ]," +
                "namespaces: { 'org.lilyproject.resttest': 'b' } }");
        response = put(BASE_URI + "/schema/recordType/b$book?ns.b=org.lilyproject.resttest", body);
        assertTrue(response.getStatus().isSuccess());
    }

    @Test
    public void testRecordBasics() throws Exception {
        makeBookSchema();

        // Create a record using PUT and a user ID
        String body = json("{ type: 'b$book', fields: { 'b$title' : 'Faster Fishing' }, namespaces : { 'org.lilyproject.resttest': 'b' } }");
        Response response = put(BASE_URI + "/record/USER.faster_fishing", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        // Read the record
        response = get(BASE_URI + "/record/USER.faster_fishing");
        assertStatus(Status.SUCCESS_OK, response);

        // Verify content
        JsonNode json = readJson(response.getEntity());
        assertEquals(1L, json.get("fields").size());
        assertNull(json.get("schema")); // schema info should not be included by default

        // Read the record with schema info
        response = get(BASE_URI + "/record/USER.faster_fishing?schema=true");
        assertStatus(Status.SUCCESS_OK, response);

        // Verify content
        json = readJson(response.getEntity());
        assertEquals(1L, json.get("fields").size());
        assertNotNull(json.get("schema"));
        assertEquals(1L, json.get("schema").size());

        // Read the record as specific version
        response = get(BASE_URI + "/record/USER.faster_fishing/version/1");
        assertStatus(Status.SUCCESS_OK, response);

        // Read a non-existing version
        response = get(BASE_URI + "/record/USER.faster_fishing/version/10");
        assertStatus(Status.CLIENT_ERROR_NOT_FOUND, response);

        // Specify non-numeric version
        response = get(BASE_URI + "/record/USER.faster_fishing/version/abc");
        assertStatus(Status.CLIENT_ERROR_NOT_FOUND, response);

        // Update the record
        body = json("{ action: 'update', record: { type: 'b$book', fields: { 'b$title' : 'Faster Fishing (new)' }, " +
                "namespaces : { 'org.lilyproject.resttest': 'b' } } }");
        response = post(BASE_URI + "/record/USER.faster_fishing", body);
        assertStatus(Status.SUCCESS_OK, response);

        json = readJson(response.getEntity());
        assertEquals(2L, json.get("version").getLongValue());

        response = get(BASE_URI + "/record/USER.faster_fishing/version/2");
        assertStatus(Status.SUCCESS_OK, response);

        // Update the record, specify the ID both in URI and json
        body = json("{ action: 'update', record: { id: 'USER.faster_fishing', type: 'b$book', " +
                "fields: { 'b$title' : 'Faster Fishing (new 2)' }, " +
                "namespaces : { 'org.lilyproject.resttest': 'b' } } }");
        response = post(BASE_URI + "/record/USER.faster_fishing", body);
        assertStatus(Status.SUCCESS_OK, response);

        // Update the record, specify the ID both in URI and json but both different -- should be a bad request
        body = json("{ action: 'update', record: { id: 'USER.faster_fishing_smth', type: 'b$book', " +
                "fields: { 'b$title' : 'Faster Fishing (new 2)' }, " +
                "namespaces : { 'org.lilyproject.resttest': 'b' } } }");
        response = post(BASE_URI + "/record/USER.faster_fishing", body);
        assertStatus(Status.CLIENT_ERROR_BAD_REQUEST, response);

        // Update a record which does not exist, should fail
        body = json("{ action: 'update', record: { type: 'b$book', fields: { 'b$title' : 'Faster Fishing (new 3)' }, " +
                "namespaces : { 'org.lilyproject.resttest': 'b' } } }");
        response = post(BASE_URI + "/record/USER.non_existing_record", body);
        assertStatus(Status.CLIENT_ERROR_NOT_FOUND, response);

        // Delete the record
        response = delete(BASE_URI + "/record/USER.faster_fishing");
        assertStatus(Status.SUCCESS_NO_CONTENT, response);

        // Verify deleted record is gone
        response = get(BASE_URI + "/record/USER.faster_fishing");
        assertStatus(Status.CLIENT_ERROR_NOT_FOUND, response);

        // Test delete of non-existing record
        response = delete(BASE_URI + "/record/USER.faster_fishing");
        assertStatus(Status.CLIENT_ERROR_NOT_FOUND, response);

        // Create a record using PUT and a client-specified UUID
        UUID uuid = UUID.randomUUID();
        body = json("{ type: 'b$book', fields: { 'b$title' : 'Title 1' }, namespaces : { 'org.lilyproject.resttest': 'b' } }");
        response = put(BASE_URI + "/record/UUID." + uuid.toString(), body);
        assertStatus(Status.SUCCESS_CREATED, response);

        response = get(BASE_URI + "/record/UUID." + uuid.toString());
        assertStatus(Status.SUCCESS_OK, response);

        // Create a record using POST and a client-specified UUID
        uuid = UUID.randomUUID();
        body = json("{ action: 'create', record: { id: 'UUID." + uuid.toString() + "', type: 'b$book', " +
                "fields: { 'b$title' : 'Title 1' }, namespaces : { 'org.lilyproject.resttest': 'b' } } }");
        response = post(BASE_URI + "/record", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        response = get(BASE_URI + "/record/UUID." + uuid.toString());
        assertStatus(Status.SUCCESS_OK, response);

        // Update this record, specify the ID both in URI and json
        body = json("{ action: 'update', record: { id: 'UUID." + uuid.toString() + "', type: 'b$book', " +
                "fields: { 'b$title' : 'Title 1 (new)' }, " +
                "namespaces : { 'org.lilyproject.resttest': 'b' } } }");
        response = post(BASE_URI + "/record/UUID." + uuid.toString(), body);
        assertStatus(Status.SUCCESS_OK, response);
    }

    @Test
    public void testDeleteFields() throws Exception {
        // Create two field types
        String body = json("{action: 'create', fieldType: {name: 'n$del_field1', valueType: { primitive: 'STRING' }, " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        Response response = post(BASE_URI + "/schema/fieldTypeById", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        body = json("{action: 'create', fieldType: {name: 'n$del_field2', valueType: { primitive: 'STRING' }, " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        response = post(BASE_URI + "/schema/fieldTypeById", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        // Create a record type
        body = json("{action: 'create', recordType: {name: 'n$del', " +
                "fields: [ {name: 'n$del_field1'}, {name: 'n$del_field2'} ]," +
                "namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        response = post(BASE_URI + "/schema/recordType", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        // Create a record with a value for the two fields
        body = json("{ action: 'create', record: { type: 'n$del', " +
                "fields: { 'n$del_field1' : 'foo', 'n$del_field2': 'bar' }, namespaces : { 'org.lilyproject.resttest': 'n' } } }");
        response = post(BASE_URI + "/record", body);
        assertStatus(Status.SUCCESS_CREATED, response);
        String uri = response.getLocationRef().toString();
        JsonNode json = readJson(response.getEntity());
        assertEquals(2L, json.get("fields").size());

        // Delete one of the fields. Do this 2 times, the second time should have no effect.
        for (int i = 0; i < 2; i++) {
            body = json("{ fieldsToDelete: ['n$del_field2'], namespaces : { 'org.lilyproject.resttest': 'n' } }");
            response = put(uri, body);
            assertStatus(Status.SUCCESS_OK, response);

            // TODO FIXME we need to re-read the record here since the returned record contains only submitted fields
            response = get(uri);
            assertStatus(Status.SUCCESS_OK, response);
            json = readJson(response.getEntity());
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
        String body = json("{action: 'create', fieldType: {name: 'n$vrt_field1', valueType: { primitive: 'STRING' }, " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        Response response = post(BASE_URI + "/schema/fieldTypeById", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        body = json("{action: 'create', fieldType: {name: 'n$vrt_field2', valueType: { primitive: 'STRING' }, " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        response = post(BASE_URI + "/schema/fieldTypeById", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        // Create a record type
        body = json("{action: 'create', recordType: {name: 'n$vrt', " +
                "fields: [ {name: 'n$vrt_field1'} ]," +
                "namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        response = post(BASE_URI + "/schema/recordTypeById", body);
        assertStatus(Status.SUCCESS_CREATED, response);
        String rtIdUri = response.getLocationRef().toString();

        body = json("{name: 'n$vrt', " +
                "fields: [ {name: 'n$vrt_field1'}, {name: 'n$vrt_field2'} ]," +
                "namespaces: { 'org.lilyproject.resttest': 'n' } }");
        response = put(BASE_URI + "/schema/recordType/n$vrt?ns.n=org.lilyproject.resttest", body);
        assertStatus(Status.SUCCESS_OK, response);

        response = get(BASE_URI + "/schema/recordType/n$vrt?ns.n=org.lilyproject.resttest");
        assertStatus(Status.SUCCESS_OK, response);
        JsonNode json = readJson(response.getEntity());
        assertEquals(2L, json.get("version").getLongValue());

        response = get(BASE_URI + "/schema/recordType/n$vrt/version/2?ns.n=org.lilyproject.resttest");
        assertStatus(Status.SUCCESS_OK, response);
        json = readJson(response.getEntity());
        assertEquals(2L, json.get("version").getLongValue());

        response = get(rtIdUri + "/version/2");
        assertStatus(Status.SUCCESS_OK, response);
        json = readJson(response.getEntity());
        assertEquals(2L, json.get("version").getLongValue());

        // Create record
        body = json("{ type: {name: 'n$vrt', version: 1}, " +
                "fields: { 'n$vrt_field1' : 'pink shoe laces' }, namespaces : { 'org.lilyproject.resttest': 'n' } }");
        response = put(BASE_URI + "/record/USER.pink_shoe_laces", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        json = readJson(response.getEntity());
        assertEquals(1L, json.get("type").get("version").getLongValue());

        // Update record
        body = json("{ type: {name: 'n$vrt', version: 1}, " +
                "fields: { 'n$vrt_field1' : 'pink shoe laces 2' }, namespaces : { 'org.lilyproject.resttest': 'n' } }");
        response = put(BASE_URI + "/record/USER.pink_shoe_laces", body);
        assertStatus(Status.SUCCESS_OK, response);

        json = readJson(response.getEntity());
        assertEquals(1L, json.get("type").get("version").getLongValue());

        // Update without specifying version, should move to last version
        body = json("{ type: {name: 'n$vrt'}, " +
                "fields: { 'n$vrt_field1' : 'pink shoe laces 3' }, namespaces : { 'org.lilyproject.resttest': 'n' } }");
        response = put(BASE_URI + "/record/USER.pink_shoe_laces", body);
        assertStatus(Status.SUCCESS_OK, response);

        json = readJson(response.getEntity());
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
                    "', valueType: { primitive: '" + type + "' }, " +
                    "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } } }");
            Response response = post(BASE_URI + "/schema/fieldTypeById", body);
            assertStatus(Status.SUCCESS_CREATED, response);
        }

        String body = json("{action: 'create', recordType: {name: 'n$types', fields: [" +
                " {name: 'n$fSTRING'}, {name: 'n$fINTEGER'}, {name: 'n$fLONG'}, {name: 'n$fDOUBLE'}" +
                " , {name: 'n$fDECIMAL'}, {name: 'n$fBOOLEAN'}, {name: 'n$fURI'}, {name: 'n$fDATETIME'}, " +
                " {name: 'n$fDATE'}, {name: 'n$fLINK'} ]," +
                "namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        Response response = post(BASE_URI + "/schema/recordTypeById", body);
        assertStatus(Status.SUCCESS_CREATED, response);

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
        assertStatus(Status.SUCCESS_CREATED, response);

        response = get(response.getLocationRef().toString());
        assertStatus(Status.SUCCESS_OK, response);

        JsonNode json = readJson(response.getEntity());
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
                "valueType: { primitive: 'STRING', multiValue: true }, " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        Response response = post(BASE_URI + "/schema/fieldTypeById", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        // Hierarchical field
        body = json("{action: 'create', fieldType: {name: 'n$hierarchical', " +
                "valueType: { primitive: 'STRING', hierarchical: true }, " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        response = post(BASE_URI + "/schema/fieldTypeById", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        // Multi-value + hierarchical field
        body = json("{action: 'create', fieldType: {name: 'n$multiValueHierarchical', " +
                "valueType: { primitive: 'STRING', multiValue: true, hierarchical: true }, " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        response = post(BASE_URI + "/schema/fieldTypeById", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        // Combine them into a record type
        body = json("{action: 'create', recordType: {name: 'n$mvAndHier', " +
                "fields: [ {name: 'n$multiValue'}, {name: 'n$hierarchical'}, {name: 'n$multiValueHierarchical'} ]," +
                "namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        response = post(BASE_URI + "/schema/recordTypeById", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        // Create a record
        body = json("{ type: 'n$mvAndHier', " +
                "fields: { 'n$multiValue' : ['val1', 'val2', 'val3']," +
                " 'n$hierarchical' : ['part1', 'part2', 'part3'], " +
                " 'n$multiValueHierarchical' : [['partA', 'partB'],['partC','partD']]" +
                " }, namespaces : { 'org.lilyproject.resttest': 'n' } }");
        response = put(BASE_URI + "/record/USER.multiValueHierarchical", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        // Read the record
        response = get(BASE_URI + "/record/USER.multiValueHierarchical");
        assertStatus(Status.SUCCESS_OK, response);

        JsonNode json = readJson(response.getEntity());
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
        String body = json("{action: 'create', fieldType: {name: 'b$blob1', valueType: { primitive: 'BLOB' }, " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'b' } } }");
        Response response = post(BASE_URI + "/schema/fieldTypeById", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        // Create a record type holding the blob field
        body = json("{action: 'create', recordType: {name: 'b$blobRT', fields: [ {name: 'b$blob1'} ]," +
                "namespaces: { 'org.lilyproject.resttest': 'b' } } }");
        response = post(BASE_URI + "/schema/recordTypeById", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        // Upload a blob
        Request req = new Request(Method.POST, BASE_URI + "/blob");
        String data = "Hello, blob world!";
        Representation blobRepr = new StringRepresentation(data, MediaType.TEXT_PLAIN);
        req.setEntity(blobRepr);
        response = CLIENT.handle(req);

        assertStatus(Status.SUCCESS_OK, response);
        JsonNode jsonNode = readJson(response.getEntity());
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
        assertStatus(Status.SUCCESS_CREATED, response);

        // Read the record
        response = get(BASE_URI + "/record/USER.blob1");
        assertStatus(Status.SUCCESS_OK, response);

        jsonNode = readJson(response.getEntity());
        String prefix = jsonNode.get("namespaces").get("org.lilyproject.resttest").getValueAsText();
        blobNode = (ObjectNode)jsonNode.get("fields").get(prefix + "$blob1");
        assertEquals("text/plain", blobNode.get("mediaType").getValueAsText());
        assertEquals(data.length(), blobNode.get("size").getLongValue());
        assertEquals(blobValue, blobNode.get("value").getTextValue());
        assertEquals("helloworld.txt", blobNode.get("name").getValueAsText());

        // Read the blob
        response = get(BASE_URI + "/record/USER.blob1/field/b$blob1/data?ns.b=org.lilyproject.resttest");
        assertStatus(Status.SUCCESS_OK, response);
        assertEquals(data, response.getEntityAsText());
    }

    @Test
    public void testMultiValueHierarchicalBlobs() throws Exception {
        // Create a blob field type
        String body = json("{action: 'create', fieldType: {name: 'b$blob2', valueType: " +
                "{ primitive: 'BLOB', multiValue: true, hierarchical: true }, " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'b' } } }");
        Response response = post(BASE_URI + "/schema/fieldTypeById", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        // Create a record type holding the blob field
        body = json("{action: 'create', recordType: {name: 'b$blobRT2', fields: [ {name: 'b$blob2'} ]," +
                "namespaces: { 'org.lilyproject.resttest': 'b' } } }");
        response = post(BASE_URI + "/schema/recordTypeById", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        // Upload some blobs
        ObjectNode blob1Value = createTextBlob("My blob 1");
        ObjectNode blob2Value = createTextBlob("My blob 2");
        ObjectNode blob3Value = createTextBlob("My blob 3");
        ObjectNode blob4Value = createTextBlob("My blob 4");

        // Create a record with these blobs
        ObjectNode recordNode = JsonNodeFactory.instance.objectNode();
        recordNode.put("type", "b$blobRT");
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
        assertStatus(Status.SUCCESS_CREATED, response);

        // Read the record
        response = get(BASE_URI + "/record/USER.blob2");
        assertStatus(Status.SUCCESS_OK, response);

        JsonNode jsonNode = readJson(response.getEntity());
        String prefix = jsonNode.get("namespaces").get("org.lilyproject.resttest").getValueAsText();
        blobNode = (ArrayNode)jsonNode.get("fields").get(prefix + "$blob2");
        assertEquals(2, blobNode.size());

        // Read the blobs
        for (int mvIndex = 0; mvIndex < 2; mvIndex++) {
            for (int hIndex = 0; hIndex < 2; hIndex++) {
                response = get(BASE_URI + "/record/USER.blob2/field/b$blob2/data?ns.b=org.lilyproject.resttest&mvIndex=" +
                        mvIndex + "&hIndex=" + hIndex);
                assertStatus(Status.SUCCESS_OK, response);
            }
        }

        // Read a blob at non-existing indexes
        response = get(BASE_URI + "/record/USER.blob2/field/b$blob2/data?ns.b=org.lilyproject.resttest&mvIndex=5&hIndex=3");
        assertStatus(Status.CLIENT_ERROR_NOT_FOUND, response);
    }

    private ObjectNode createTextBlob(String data) throws IOException {
        Request req = new Request(Method.POST, BASE_URI + "/blob");
        Representation blobRepr = new StringRepresentation(data, MediaType.TEXT_PLAIN);
        req.setEntity(blobRepr);
        Response response = CLIENT.handle(req);

        assertStatus(Status.SUCCESS_OK, response);
        JsonNode jsonNode = readJson(response.getEntity());
        return (ObjectNode)jsonNode;
    }

    @Test
    public void testVersionCollection() throws Exception {
        // Create some field types
        String body = json("{action: 'create', fieldType: {name: 'p$name', valueType: { primitive: 'STRING' }, " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'p' } } }");
        Response response = post(BASE_URI + "/schema/fieldTypeById", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        body = json("{action: 'create', fieldType: {name: 'p$price', valueType: { primitive: 'DOUBLE' }, " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'p' } } }");
        response = post(BASE_URI + "/schema/fieldTypeById", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        body = json("{action: 'create', fieldType: {name: 'p$colour', valueType: { primitive: 'STRING' }, " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'p' } } }");
        response = post(BASE_URI + "/schema/fieldTypeById", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        // Create a record type
        body = json("{action: 'create', recordType: {name: 'p$product', fields: [ {name: 'p$name'}, " +
                "{name: 'p$price'}, {name: 'p$colour'} ], namespaces: { 'org.lilyproject.resttest': 'p' } } }");
        response = post(BASE_URI + "/schema/recordTypeById", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        // Create a record with some versions
        body = json("{ type: 'p$product', fields: { 'p$name' : 'Product 1' }, namespaces : { 'org.lilyproject.resttest': 'p' } }");
        response = put(BASE_URI + "/record/USER.product1", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        body = json("{ fields: { 'p$name' : 'Product 1', 'p$price': 5.5 }, namespaces : { 'org.lilyproject.resttest': 'p' } }");
        response = put(BASE_URI + "/record/USER.product1", body);
        assertStatus(Status.SUCCESS_OK, response);

        body = json("{ fields: { 'p$name' : 'Product 1', 'p$price': 5.5, 'p$colour': 'red' }, namespaces : { 'org.lilyproject.resttest': 'p' } }");
        response = put(BASE_URI + "/record/USER.product1", body);
        assertStatus(Status.SUCCESS_OK, response);

        // Get list of versions
        response = get(BASE_URI + "/record/USER.product1/version");
        assertStatus(Status.SUCCESS_OK, response);
        JsonNode json = readJson(response.getEntity());
        assertEquals(3, json.get("results").size());
        JsonNode results = json.get("results");
        assertEquals(1, results.get(0).get("fields").size());
        assertEquals(2, results.get(1).get("fields").size());
        assertEquals(3, results.get(2).get("fields").size());

        response = get(BASE_URI + "/record/USER.product1/version?max-results=1");
        assertStatus(Status.SUCCESS_OK, response);
        json = readJson(response.getEntity());
        assertEquals(1, json.get("results").size());
        results = json.get("results");
        assertEquals(1, results.get(0).get("version").getLongValue());

        response = get(BASE_URI + "/record/USER.product1/version?start-index=2&max-results=1");
        assertStatus(Status.SUCCESS_OK, response);
        json = readJson(response.getEntity());
        assertEquals(1, json.get("results").size());
        results = json.get("results");
        assertEquals(2, results.get(0).get("version").getLongValue());

        // Retrieve only one field
        response = get(BASE_URI + "/record/USER.product1/version?fields=n$name&ns.n=org.lilyproject.resttest");
        assertStatus(Status.SUCCESS_OK, response);
        json = readJson(response.getEntity());
        results = json.get("results");
        assertEquals(1, results.get(0).get("fields").size());
        assertEquals(1, results.get(1).get("fields").size());
        assertEquals(1, results.get(2).get("fields").size());
    }

    @Test
    public void testVariantCollection() throws Exception {
        makeBookSchema();

        String body = json("{ type: 'b$book', fields: { 'b$title' : 'Hunting' }, namespaces : { 'org.lilyproject.resttest': 'b' } }");
        Response response = put(BASE_URI + "/record/USER.hunting.lang=en", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        body = json("{ type: 'b$book', fields: { 'b$title' : 'Jagen' }, namespaces : { 'org.lilyproject.resttest': 'b' } }");
        response = put(BASE_URI + "/record/USER.hunting.lang=nl", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        body = json("{ type: 'b$book', fields: { 'b$title' : 'La chasse' }, namespaces : { 'org.lilyproject.resttest': 'b' } }");
        response = put(BASE_URI + "/record/USER.hunting.lang=fr", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        response = get(BASE_URI + "/record/USER.hunting/variant");
        assertStatus(Status.SUCCESS_OK, response);

        JsonNode jsonNode = readJson(response.getEntity());
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
        String body = json("{action: 'create', fieldType: {name: 'v$active', valueType: { primitive: 'LONG' }, " +
                "scope: 'non_versioned', namespaces: { 'org.lilyproject.vtag': 'v' } } }");
        Response response = post(BASE_URI + "/schema/fieldTypeById", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        body = json("{ type: 'b$book', fields: { 'b$title' : 'Title version 1' }, namespaces : { 'org.lilyproject.resttest': 'b' } }");
        response = put(BASE_URI + "/record/USER.vtagtest", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        body = json("{ type: 'b$book', fields: { 'b$title' : 'Title version 2', 'v$active': 1 }, " +
                "namespaces : { 'org.lilyproject.resttest': 'b', 'org.lilyproject.vtag': 'v' } }");
        response = put(BASE_URI + "/record/USER.vtagtest", body);
        assertStatus(Status.SUCCESS_OK, response);

        response = get(BASE_URI + "/record/USER.vtagtest/vtag/active");
        assertStatus(Status.SUCCESS_OK, response);
        assertEquals(1L, readJson(response.getEntity()).get("version").getLongValue());

        response = get(BASE_URI + "/record/USER.vtagtest/vtag/last");
        assertStatus(Status.SUCCESS_OK, response);
        assertEquals(2L, readJson(response.getEntity()).get("version").getLongValue());

        response = get(BASE_URI + "/record/USER.vtagtest/vtag/aNotExistingVTagName");
        assertStatus(Status.CLIENT_ERROR_NOT_FOUND, response);
    }

    @Test
    public void testVersionedMutableScope() throws Exception {
        // Create a versioned field type
        String body = json("{action: 'create', fieldType: {name: 'b$subject', valueType: { primitive: 'STRING' }, " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'b' } } }");
        Response response = post(BASE_URI + "/schema/fieldTypeById", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        // Create a versioned-mutable field type
        body = json("{action: 'create', fieldType: {name: 'b$state', valueType: { primitive: 'STRING' }, " +
                "scope: 'versioned_mutable', namespaces: { 'org.lilyproject.resttest': 'b' } } }");
        response = post(BASE_URI + "/schema/fieldTypeById", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        // Create a record type
        body = json("{action: 'create', recordType: {name: 'b$article', " +
                "fields: [ {name: 'b$subject'}, {name: 'b$state'} ]," +
                "namespaces: { 'org.lilyproject.resttest': 'b' } } }");
        response = post(BASE_URI + "/schema/recordTypeById", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        // Create a record
        body = json("{ type: 'b$article', fields: { 'b$subject': 'Peace', 'b$state': 'draft' }, namespaces : { 'org.lilyproject.resttest': 'b' } }");
        response = put(BASE_URI + "/record/USER.peace", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        // Read the record
        response = get(BASE_URI + "/record/USER.peace");
        assertStatus(Status.SUCCESS_OK, response);

        JsonNode json = readJson(response.getEntity());
        JsonNode fieldsNode = json.get("fields");
        String prefix = json.get("namespaces").get("org.lilyproject.resttest").getTextValue();

        assertEquals("draft", fieldsNode.get(prefix + "$state").getValueAsText());
        assertEquals("Peace", fieldsNode.get(prefix + "$subject").getValueAsText());

        // Update the versioned-mutable field
        body = json("{ fields: { 'b$state': 'in_review' }, namespaces : { 'org.lilyproject.resttest': 'b' } }");
        response = put(BASE_URI + "/record/USER.peace/version/1", body);
        assertStatus(Status.SUCCESS_OK, response);

        response = get(BASE_URI + "/record/USER.peace");
        assertStatus(Status.SUCCESS_OK, response);

        json = readJson(response.getEntity());
        fieldsNode = json.get("fields");
        prefix = json.get("namespaces").get("org.lilyproject.resttest").getTextValue();

        assertEquals(1L, json.get("version").getLongValue()); // no new version created
        assertEquals("in_review", fieldsNode.get(prefix + "$state").getValueAsText());

        // Update versioned-mutable field without changes, change to versioned field should be ignored
        body = json("{ fields: { 'b$subject': 'Peace2', 'b$state': 'in_review' }, namespaces : { 'org.lilyproject.resttest': 'b' } }");
        response = put(BASE_URI + "/record/USER.peace/version/1", body);
        assertStatus(Status.SUCCESS_OK, response);

        response = get(BASE_URI + "/record/USER.peace");
        assertStatus(Status.SUCCESS_OK, response);

        json = readJson(response.getEntity());
        fieldsNode = json.get("fields");
        prefix = json.get("namespaces").get("org.lilyproject.resttest").getTextValue();

        assertEquals(1L, json.get("version").getLongValue()); // no new version created
        assertEquals("Peace", fieldsNode.get(prefix + "$subject").getValueAsText());
    }

    @Test
    public void testConditionUpdate() throws Exception {
        makeBookSchema();

        String body = json("{ type: 'b$book', fields: { 'b$title' : 'ABC1' }, namespaces : { 'org.lilyproject.resttest': 'b' } }");
        Response response = put(BASE_URI + "/record/USER.ABC", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        // Test update with failing condition
        body = json("{ action: 'update', record: { fields: { 'b$title' : 'ABC2' } }, " +
                "conditions: [{field: 'b$title', value: 'ABC5'}], " +
                "namespaces : { 'org.lilyproject.resttest': 'b' } }");

        response = post(BASE_URI + "/record/USER.ABC", body);
        assertStatus(Status.CLIENT_ERROR_CONFLICT, response);

        // Verify update did not go through
        response = get(BASE_URI + "/record/USER.ABC");
        assertStatus(Status.SUCCESS_OK, response);

        JsonNode json = readJson(response.getEntity());
        assertEquals("ABC1", getFieldValue(json, "title").getTextValue());

        // Test update with succeeding condition
        body = json("{ action: 'update', record: { fields: { 'b$title' : 'ABC2' } }, " +
                "conditions: [{field: 'b$title', value: 'ABC1'}], " +
                "namespaces : { 'org.lilyproject.resttest': 'b' } }");

        response = post(BASE_URI + "/record/USER.ABC", body);

        assertStatus(Status.SUCCESS_OK, response);

        // Verify update did not through
        response = get(BASE_URI + "/record/USER.ABC");
        assertStatus(Status.SUCCESS_OK, response);

        json = readJson(response.getEntity());
        assertEquals("ABC2", getFieldValue(json, "title").getTextValue());

        //
        // Test with custom compare operator
        //
        body = json("{ action: 'update', record: { fields: { 'b$title' : 'ABC3' } }, " +
                "conditions: [{field: 'b$title', value: 'ABC2', operator: 'not_equal'}], " +
                "namespaces : { 'org.lilyproject.resttest': 'b' } }");

        response = post(BASE_URI + "/record/USER.ABC", body);
        assertStatus(Status.CLIENT_ERROR_CONFLICT, response);

        //
        // Test allowMissing flag
        //
        body = json("{ action: 'update', record: { fields: { 'b$title' : 'ABC3' } }, " +
                "conditions: [{field: 'b$summary', value: 'some summary', allowMissing: false}], " +
                "namespaces : { 'org.lilyproject.resttest': 'b' } }");

        response = post(BASE_URI + "/record/USER.ABC", body);
        assertStatus(Status.CLIENT_ERROR_CONFLICT, response);

        body = json("{ action: 'update', record: { fields: { 'b$title' : 'ABC3' } }, " +
                "conditions: [{field: 'b$summary', value: 'some summary', allowMissing: true}], " +
                "namespaces : { 'org.lilyproject.resttest': 'b' } }");

        response = post(BASE_URI + "/record/USER.ABC", body);
        assertStatus(Status.SUCCESS_OK, response);

        //
        // Test null value
        //
        // First remove summary field again
        body = json("{ action: 'update', record: { " +
                "fieldsToDelete: ['b$summary'], " +
                "namespaces : { 'org.lilyproject.resttest': 'b' } } }");
        response = post(BASE_URI + "/record/USER.ABC", body);
        assertStatus(Status.SUCCESS_OK, response);

        body = json("{ action: 'update', record: { fields: { 'b$title' : 'ABC4' } }, " +
                "conditions: [{field: 'b$summary', value: null, operator: 'not_equal'}], " +
                "namespaces : { 'org.lilyproject.resttest': 'b' } } }");

        response = post(BASE_URI + "/record/USER.ABC", body);
        assertStatus(Status.CLIENT_ERROR_CONFLICT, response);

        body = json("{ action: 'update', record: { fields: { 'b$title' : 'ABC4' } }, " +
                "conditions: [{field: 'b$summary', value: null, operator: 'equal'}], " +
                "namespaces : { 'org.lilyproject.resttest': 'b' } } }");

        response = post(BASE_URI + "/record/USER.ABC", body);
        assertStatus(Status.SUCCESS_OK, response);

        //
        // Test system field check
        //
        // Create a new record
        body = json("{ type: 'b$book', fields: { 'b$title' : 'ABC1' }, namespaces : { 'org.lilyproject.resttest': 'b' } }");
        response = put(BASE_URI + "/record/USER.sysfieldcheck", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        // Test update with failing condition
        body = json("{ action: 'update', record: { fields: { 'b$title' : 'ABC2' } }, " +
                "conditions: [{field: 's$version', value: 2}], " +
                "namespaces : { 'org.lilyproject.resttest': 'b', 'org.lilyproject.system': 's' } }");

        response = post(BASE_URI + "/record/USER.sysfieldcheck", body);
        assertStatus(Status.CLIENT_ERROR_CONFLICT, response);

        // Test update with succeeding condition
        body = json("{ action: 'update', record: { fields: { 'b$title' : 'ABC2' } }, " +
                "conditions: [{field: 's$version', value: 1}], " +
                "namespaces : { 'org.lilyproject.resttest': 'b', 'org.lilyproject.system': 's' } }");

        response = post(BASE_URI + "/record/USER.sysfieldcheck", body);
        assertStatus(Status.SUCCESS_OK, response);
    }

    @Test
    public void testConditionDelete() throws Exception {
        makeBookSchema();

        String body = json("{ type: 'b$book', fields: { 'b$title' : 'CondDel' }, namespaces : { 'org.lilyproject.resttest': 'b' } }");
        Response response = put(BASE_URI + "/record/USER.ConDel", body);
        assertStatus(Status.SUCCESS_CREATED, response);

        body = json("{ action: 'delete'," +
                "conditions: [{field: 'b$title', value: 'foo'}], " +
                "namespaces : { 'org.lilyproject.resttest': 'b' } } }");

        response = post(BASE_URI + "/record/USER.ConDel", body);
        assertStatus(Status.CLIENT_ERROR_CONFLICT, response);

        body = json("{ action: 'delete'," +
                "conditions: [{field: 'b$title', value: 'CondDel'}], " +
                "namespaces : { 'org.lilyproject.resttest': 'b' } } }");

        response = post(BASE_URI + "/record/USER.ConDel", body);
        assertStatus(Status.SUCCESS_NO_CONTENT, response);
    }

    private JsonNode getFieldValue(JsonNode recordJson, String fieldName) {
        String prefix = recordJson.get("namespaces").get("org.lilyproject.resttest").getTextValue();
        JsonNode fieldsNode = recordJson.get("fields");
        return fieldsNode.get(prefix + "$" + fieldName);
    }

    private void assertStatus(Status expectedStatus, Response response) throws IOException {
        if (!expectedStatus.equals(response.getStatus())) {
            System.err.println("Detected unexpected response status, body of the response is:");
            printErrorResponse(response);
            assertEquals(expectedStatus, response.getStatus());
        }
    }

    private void printErrorResponse(Response response) throws IOException {
        if (response.getEntity() != null && MediaType.APPLICATION_JSON.equals(response.getEntity().getMediaType())) {
            JsonNode json = readJson(response.getEntity());
            System.err.println("Error:");
            System.err.println("  Description: " +
                    (json.get("description") != null ? json.get("description").getTextValue() : null));
            System.err.println("  Status: " + (json.get("status") != null ? json.get("status").getIntValue() : null));
            System.err.println("  StackTrace:");
            JsonNode stackTrace = json.get("stackTrace");
            System.out.println(stackTrace != null ? stackTrace.getValueAsText() : null);
        } else {
            System.err.println(response.getEntityAsText());
        }
    }
    
    private Response post(String uri, String body) {
        Request req = new Request(Method.POST, uri);
        req.setEntity(body, MediaType.APPLICATION_JSON);
        return CLIENT.handle(req);
    }

    private Response put(String uri, String body) {
        Request req = new Request(Method.PUT, uri);
        req.setEntity(body, MediaType.APPLICATION_JSON);
        return CLIENT.handle(req);
    }

    private Response get(String uri) {
        return CLIENT.handle(new Request(Method.GET, uri));
    }

    private Response delete(String uri) {
        return CLIENT.handle(new Request(Method.DELETE, uri));
    }

    public static JsonNode readJson(Representation representation) throws IOException {
        InputStream is = representation.getStream();
        try {
            return JsonFormat.deserializeNonStd(is);
        } finally {
            Closer.close(is);
        }
    }

    private String json(String input) {
        return input.replaceAll("'", "\"");
    }

}
