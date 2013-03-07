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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.http.HttpStatus;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.junit.Test;

public class SchemaRestTest extends AbstractRestTest {


    @Test
    public void testGeneralErrors() throws Exception {
        // Perform request to non-existing resource class
        ResponseAndContent response = get("/foobar");
        assertStatus(HttpStatus.SC_NOT_FOUND, response);

        // Submit invalid json
        String body = json("{ f [ }");
        response = post("/schema/fieldType", body);
        assertStatus(HttpStatus.SC_BAD_REQUEST, response);
    }

    @Test
    public void testFieldTypes() throws Exception {
        // Create field type using POST
        String body = json("{action: 'create', fieldType: {name: 'n$field1', valueType: 'STRING', " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        ResponseAndContent response = post("/schema/fieldTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        JsonNode json = readJson(response);

        // verify location header
        assertEquals(buildUri("/schema/fieldTypeById/" + json.get("id").getValueAsText()), response.getLocationRef().toString());

        // verify name
        String prefix = json.get("namespaces").get("org.lilyproject.resttest").getTextValue();
        assertEquals(prefix + "$field1", json.get("name").getTextValue());

        // Create field type using POST on the name-based resource
        body = json("{action: 'create', fieldType: {name: 'n$field1a', valueType: 'STRING', " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        response = post("/schema/fieldType", body);
        assertStatus(HttpStatus.SC_CREATED, response);
        assertEquals(buildUri("/schema/fieldType/n$field1a?ns.n=org.lilyproject.resttest"), response.getLocationRef().toString());

        // Create field type using PUT
        body = json("{name: 'n$field2', valueType: 'STRING', " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } }");
        response = put("/schema/fieldType/n$field2?ns.n=org.lilyproject.resttest", body);
        String fieldType2Id = readJson(response).get("id").getValueAsText();
        assertStatus(HttpStatus.SC_CREATED, response);
        assertEquals(buildUri("/schema/fieldType/n$field2?ns.n=org.lilyproject.resttest"), response.getLocationRef().toString());

        // Update a field type - by name : change field2 to field3
        body = json("{name: 'n$field3', valueType: 'STRING', " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } }");
        response = put("/schema/fieldType/n$field2?ns.n=org.lilyproject.resttest", body);
        assertStatus(HttpStatus.SC_MOVED_PERMANENTLY, response);
        assertEquals(buildUri("/schema/fieldType/n$field3?ns.n=org.lilyproject.resttest"), response.getLocationRef().toString());

        response = get("/schema/fieldType/n$field2?ns.n=org.lilyproject.resttest");
        assertStatus(HttpStatus.SC_NOT_FOUND, response);

        response = get("/schema/fieldType/n$field3?ns.n=org.lilyproject.resttest");
        assertStatus(HttpStatus.SC_OK, response);

        // Update a field type - by ID : change field3 to field4
        body = json("{name: 'n$field4', valueType: 'STRING', "
                + "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } }");
        response = put("/schema/fieldTypeById/" + fieldType2Id, body);
        assertStatus(HttpStatus.SC_OK, response);

        // Test updating immutable properties
        body = json("{name: 'n$field4', valueType: 'INTEGER', "
                + "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } }");
        response = put("/schema/fieldTypeById/" + fieldType2Id, body);
        assertStatus(HttpStatus.SC_CONFLICT, response);

        body = json("{name: 'n$field4', valueType: 'STRING', "
                + "scope: 'non_versioned', namespaces: { 'org.lilyproject.resttest': 'n' } }");
        response = put("/schema/fieldTypeById/" + fieldType2Id, body);
        assertStatus(HttpStatus.SC_CONFLICT, response);

        body = json("{name: 'n$field4', valueType: 'LIST<STRING>', "
                + "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } }");
        response = put("/schema/fieldTypeById/" + fieldType2Id, body);
        assertStatus(HttpStatus.SC_CONFLICT, response);

        body = json("{name: 'n$field4', valueType: 'PATH<STRING>', "
                + "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } }");
        response = put("/schema/fieldType/n$field4?ns.n=org.lilyproject.resttest", body);
        assertStatus(HttpStatus.SC_CONFLICT, response);

        // Get list of field types
        response = get("/schema/fieldType");
        assertStatus(HttpStatus.SC_OK, response);
        json = readJson(response);
        assertTrue(json.get("results").size() > 0);

        response = get("/schema/fieldTypeById");
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
            ResponseAndContent response = post("/schema/fieldTypeById", body);
            assertStatus(HttpStatus.SC_CREATED, response);
            JsonNode json = readJson(response);
            fieldTypeIds.add(json.get("id").getValueAsText());
        }

        // Create a record type using POST
        String body = json("{action: 'create', recordType: {name: 'n$recordType1', fields: [ {name: 'n$rt_field1'} ]," +
                "namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        ResponseAndContent response = post("/schema/recordTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        JsonNode json = readJson(response);

        // verify location header
        assertEquals(buildUri("/schema/recordTypeById/" + json.get("id").getValueAsText()),
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
        response = post("/schema/recordType", body);
        assertStatus(HttpStatus.SC_CREATED, response);
        assertEquals(
                buildUri("/schema/recordType/n$recordType1a?ns.n=org.lilyproject.resttest"),
                response.getLocationRef().toString());

        // Create a record type using PUT
        body = json("{name: 'n$recordType2', fields: [ {name: 'n$rt_field1'} ]," +
                "namespaces: { 'org.lilyproject.resttest': 'n' } }");
        response = put("/schema/recordType/n$recordType2?ns.n=org.lilyproject.resttest", body);
        assertStatus(HttpStatus.SC_CREATED, response);
        assertEquals(buildUri("/schema/recordType/n$recordType2?ns.n=org.lilyproject.resttest"), response.getLocationRef().toString());
        json = readJson(response);
        String secondRtId = json.get("id").getValueAsText();

        // Update a record type - by ID
        body = json("{name: 'n$recordType2', " +
                "fields: [ {name: 'n$rt_field1', mandatory: true}, " +
                " {name : 'n$rt_field2', mandatory: true} ], namespaces: { 'org.lilyproject.resttest': 'n' } }");
        response = put("/schema/recordTypeById/" + secondRtId, body);
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
        response = put("/schema/recordType/n$recordType2?ns.n=org.lilyproject.resttest", body);
        assertStatus(HttpStatus.SC_MOVED_PERMANENTLY, response);
        assertEquals(buildUri("/schema/recordType/n$recordType3?ns.n=org.lilyproject.resttest"), response.getLocationRef().toString());

        // Get list of record types
        response = get("/schema/recordType");
        assertStatus(HttpStatus.SC_OK, response);
        json = readJson(response);
        assertTrue(json.get("results").size() > 0);

        response = get("/schema/recordTypeById");
        assertStatus(HttpStatus.SC_OK, response);
        json = readJson(response);
        assertTrue(json.get("results").size() > 0);

        //
        // Test supertypes
        //

        // Create two supertype record types
        body = json("{action: 'create', recordType: {name: 'n$supertype1', fields: [ {name: 'n$rt_field2'} ]," +
                "namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        response = post("/schema/recordTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);
        String supertype1Id = readJson(response).get("id").getTextValue();

        body = json("{action: 'create', recordType: {name: 'n$supertype2', fields: [ {name: 'n$rt_field3'} ]," +
                "namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        response = post("/schema/recordTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);
        String supertype2Id = readJson(response).get("id").getTextValue();

        // Create a record type using the supertypes
        body = json("{action: 'create', recordType: {name: 'n$subtype', fields: [ {name: 'n$rt_field1'} ]," +
                "supertypes: [{name: 'n$supertype1', version: 1}, { id: '" + supertype2Id + "' } ], " +
                "namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        response = post("/schema/recordTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        response = getUri(response.getLocationRef().toString());
        assertStatus(HttpStatus.SC_OK, response);
        json = readJson(response);

        assertEquals(2, json.get("supertypes").size());

        // Update to remove one of the supertypes
        body = json("{name: 'n$subtype', fields: [ {name: 'n$rt_field1'} ]," +
                "supertypes: [{name: 'n$supertype1', version: 1}], " +
                "namespaces: { 'org.lilyproject.resttest': 'n' } }");
        String subtypeUri = "/schema/recordType/n$subtype?ns.n=org.lilyproject.resttest";
        response = put(subtypeUri, body);
        assertStatus(HttpStatus.SC_OK, response);

        response = get(subtypeUri);
        assertStatus(HttpStatus.SC_OK, response);
        json = readJson(response);

        assertEquals(1, json.get("supertypes").size());

        // Update supertype1 with refreshSubtypes=true
        body = json("{name: 'n$supertype1', fields: [ {name: 'n$rt_field1'}, {name: 'n$rt_field2'} ]," +
                "namespaces: { 'org.lilyproject.resttest': 'n' } }");
        String supertype1Uri = "/schema/recordType/n$supertype1?ns.n=org.lilyproject.resttest&refreshSubtypes=true";
        response = put(supertype1Uri, body);
        assertStatus(HttpStatus.SC_OK, response);

        // Check subtype got updated (because refresSubtypes=true)
        response = get(subtypeUri);
        assertStatus(HttpStatus.SC_OK, response);
        json = readJson(response);
        assertEquals(2, json.get("supertypes").get(0).get("version").getIntValue());
        assertEquals(3, json.get("version").getIntValue());
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
            ResponseAndContent response = post("/schema/fieldTypeById", body);
            assertStatus(HttpStatus.SC_CREATED, response);
        }

        String body = json("{action: 'create', recordType: {name: 'n$types', fields: [" +
                " {name: 'n$fSTRING'}, {name: 'n$fINTEGER'}, {name: 'n$fLONG'}, {name: 'n$fDOUBLE'}" +
                " , {name: 'n$fDECIMAL'}, {name: 'n$fBOOLEAN'}, {name: 'n$fURI'}, {name: 'n$fDATETIME'}, " +
                " {name: 'n$fDATE'}, {name: 'n$fLINK'} ]," +
                "namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        ResponseAndContent response = post("/schema/recordTypeById", body);
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
        response = post("/record", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        response = getUri(response.getLocationRef().toString());
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
        ResponseAndContent response = post("/schema/fieldTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        // Hierarchical field
        body = json("{action: 'create', fieldType: {name: 'n$hierarchical', " +
                "valueType: 'PATH<STRING>', " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        response = post("/schema/fieldTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        // Multi-value + hierarchical field
        body = json("{action: 'create', fieldType: {name: 'n$multiValueHierarchical', " +
                "valueType: 'LIST<PATH<STRING>>', " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        response = post("/schema/fieldTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        // Combine them into a record type
        body = json("{action: 'create', recordType: {name: 'n$mvAndHier', " +
                "fields: [ {name: 'n$multiValue'}, {name: 'n$hierarchical'}, {name: 'n$multiValueHierarchical'} ]," +
                "namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        response = post("/schema/recordTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        // Create a record
        body = json("{ type: 'n$mvAndHier', " +
                "fields: { 'n$multiValue' : ['val1', 'val2', 'val3']," +
                " 'n$hierarchical' : ['part1', 'part2', 'part3'], " +
                " 'n$multiValueHierarchical' : [['partA', 'partB'],['partC','partD']]" +
                " }, namespaces : { 'org.lilyproject.resttest': 'n' } }");
        response = put("/record/USER.multiValueHierarchical", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        // Read the record
        response = get("/record/USER.multiValueHierarchical");
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
    public void testVersionCollection() throws Exception {
        // Create some field types
        String body = json("{action: 'create', fieldType: {name: 'p$name', valueType: 'STRING', " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'p' } } }");
        ResponseAndContent response = post("/schema/fieldTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        body = json("{action: 'create', fieldType: {name: 'p$price', valueType: 'DOUBLE', " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'p' } } }");
        response = post("/schema/fieldTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        body = json("{action: 'create', fieldType: {name: 'p$colour', valueType: 'STRING', " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'p' } } }");
        response = post("/schema/fieldTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        // Create a record type
        body = json("{action: 'create', recordType: {name: 'p$product', fields: [ {name: 'p$name'}, " +
                "{name: 'p$price'}, {name: 'p$colour'} ], namespaces: { 'org.lilyproject.resttest': 'p' } } }");
        response = post("/schema/recordTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        // Create a record with some versions
        body = json("{ type: 'p$product', fields: { 'p$name' : 'Product 1' }, namespaces : { 'org.lilyproject.resttest': 'p' } }");
        response = put("/record/USER.product1", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        body = json("{ fields: { 'p$name' : 'Product 1', 'p$price': 5.5 }, namespaces : { 'org.lilyproject.resttest': 'p' } }");
        response = put("/record/USER.product1", body);
        assertStatus(HttpStatus.SC_OK, response);

        body = json("{ fields: { 'p$name' : 'Product 1', 'p$price': 5.5, 'p$colour': 'red' }, namespaces : { 'org.lilyproject.resttest': 'p' } }");
        response = put("/record/USER.product1", body);
        assertStatus(HttpStatus.SC_OK, response);

        // Get list of versions
        response = get("/record/USER.product1/version");
        assertStatus(HttpStatus.SC_OK, response);
        JsonNode json = readJson(response);
        assertEquals(3, json.get("results").size());
        JsonNode results = json.get("results");
        assertEquals(1, results.get(0).get("fields").size());
        assertEquals(2, results.get(1).get("fields").size());
        assertEquals(3, results.get(2).get("fields").size());

        response = get("/record/USER.product1/version?max-results=1");
        assertStatus(HttpStatus.SC_OK, response);
        json = readJson(response);
        assertEquals(1, json.get("results").size());
        results = json.get("results");
        assertEquals(1, results.get(0).get("version").getLongValue());

        response = get("/record/USER.product1/version?start-index=2&max-results=1");
        assertStatus(HttpStatus.SC_OK, response);
        json = readJson(response);
        assertEquals(1, json.get("results").size());
        results = json.get("results");
        assertEquals(2, results.get(0).get("version").getLongValue());

        // Retrieve only one field
        response = get("/record/USER.product1/version?fields=n$name&ns.n=org.lilyproject.resttest");
        assertStatus(HttpStatus.SC_OK, response);
        json = readJson(response);
        results = json.get("results");
        assertEquals(1, results.get(0).get("fields").size());
        assertEquals(1, results.get(1).get("fields").size());
        assertEquals(1, results.get(2).get("fields").size());
    }



    @Test
    public void testTables() throws Exception {
        ResponseAndContent getResponse = get("/table");
        assertStatus(HttpStatus.SC_OK, getResponse);

        List<String> tableNames = getTableNameList(readJson(getResponse));
        assertEquals(Lists.newArrayList("record"), tableNames);

        ResponseAndContent postResponse = post("/table", "{\"name\": \"resttesttable\"}");
        assertStatus(HttpStatus.SC_OK, postResponse);

        getResponse = get("/table");
        tableNames = getTableNameList(readJson(getResponse));
        Collections.sort(tableNames);

        assertEquals(Lists.newArrayList("record", "resttesttable"), tableNames);

        ResponseAndContent deleteResponse = delete("/table/resttesttable");
        assertStatus(HttpStatus.SC_OK, deleteResponse);

        getResponse = get("/table");
        tableNames = getTableNameList(readJson(getResponse));

        assertEquals(Lists.newArrayList("record"), tableNames);

        deleteResponse = delete("/table/resttesttable");
        assertStatus(HttpStatus.SC_NOT_FOUND, deleteResponse);

    }

    private List<String> getTableNameList(JsonNode json) {
        if (!json.isArray()) {
            throw new RuntimeException("Supplied JSON is not an array: " +  json.toString());
        }
        List<String> stringList = Lists.newArrayList();
        for (int i = 0; i < json.size(); i++) {
            stringList.add(json.get(i).get("name").asText());
        }
        return stringList;
    }

}
