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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.UUID;

import org.junit.BeforeClass;

import org.apache.http.HttpStatus;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.Test;

public class RecordRestTest extends AbstractRestTest {


    @Test
    public void testRecordBasics() throws Exception {
        makeBookSchema();

        // Create a record using PUT and a user ID
        String body = json("{ type: 'b$book', fields: { 'b$title' : 'Faster Fishing' }, namespaces : { 'org.lilyproject.resttest': 'b' } }");
        ResponseAndContent response = put("/record/USER.faster_fishing", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        // Read the record
        response = get("/record/USER.faster_fishing");
        assertStatus(HttpStatus.SC_OK, response);

        // Verify content
        JsonNode json = readJson(response);
        assertEquals(1L, json.get("fields").size());
        assertNull(json.get("schema")); // schema info should not be included by default

        // Read the record with schema info
        response = get("/record/USER.faster_fishing?schema=true");
        assertStatus(HttpStatus.SC_OK, response);

        // Verify content
        json = readJson(response);
        assertEquals(1L, json.get("fields").size());
        assertNotNull(json.get("schema"));
        assertEquals(1L, json.get("schema").size());

        // Read the record without namespace prefixes
        response = get("/record/USER.faster_fishing?nsprefixes=false");
        assertStatus(HttpStatus.SC_OK, response);

        // Verify content
        json = readJson(response);
        assertEquals("Faster Fishing", json.get("fields").get("{org.lilyproject.resttest}title").getTextValue());

        // Read the record as specific version
        response = get("/record/USER.faster_fishing/version/1");
        assertStatus(HttpStatus.SC_OK, response);

        // Read a non-existing version
        response = get("/record/USER.faster_fishing/version/10");
        assertStatus(HttpStatus.SC_NOT_FOUND, response);

        // Specify non-numeric version
        response = get("/record/USER.faster_fishing/version/abc");
        assertStatus(HttpStatus.SC_NOT_FOUND, response);

        // Update the record
        body = json("{ action: 'update', record: { type: 'b$book', fields: { 'b$title' : 'Faster Fishing (new)' }, " +
                "namespaces : { 'org.lilyproject.resttest': 'b' } } }");
        response = post("/record/USER.faster_fishing", body);
        assertStatus(HttpStatus.SC_OK, response);

        json = readJson(response);
        assertEquals(2L, json.get("version").getLongValue());

        response = get("/record/USER.faster_fishing/version/2");
        assertStatus(HttpStatus.SC_OK, response);

        // Update the record, specify the ID both in URI and json
        body = json("{ action: 'update', record: { id: 'USER.faster_fishing', type: 'b$book', " +
                "fields: { 'b$title' : 'Faster Fishing (new 2)' }, " +
                "namespaces : { 'org.lilyproject.resttest': 'b' } } }");
        response = post("/record/USER.faster_fishing", body);
        assertStatus(HttpStatus.SC_OK, response);

        // Update the record, specify the ID both in URI and json but both different -- should be a bad request
        body = json("{ action: 'update', record: { id: 'USER.faster_fishing_smth', type: 'b$book', " +
                "fields: { 'b$title' : 'Faster Fishing (new 2)' }, " +
                "namespaces : { 'org.lilyproject.resttest': 'b' } } }");
        response = post("/record/USER.faster_fishing", body);
        assertStatus(HttpStatus.SC_BAD_REQUEST, response);

        // Update a record which does not exist, should fail
        body = json("{ action: 'update', record: { type: 'b$book', fields: { 'b$title' : 'Faster Fishing (new 3)' }, " +
                "namespaces : { 'org.lilyproject.resttest': 'b' } } }");
        response = post("/record/USER.non_existing_record", body);
        assertStatus(HttpStatus.SC_NOT_FOUND, response);

        // Delete the record
        response = delete("/record/USER.faster_fishing");
        assertStatus(HttpStatus.SC_NO_CONTENT, response);

        // Verify deleted record is gone
        response = get("/record/USER.faster_fishing");
        assertStatus(HttpStatus.SC_NOT_FOUND, response);

        // Test delete of non-existing record
        response = delete("/record/USER.faster_fishing");
        assertStatus(HttpStatus.SC_NOT_FOUND, response);

        // Create a record using PUT and a client-specified UUID
        UUID uuid = UUID.randomUUID();
        body = json("{ type: 'b$book', fields: { 'b$title' : 'Title 1' }, namespaces : { 'org.lilyproject.resttest': 'b' } }");
        response = put("/record/UUID." + uuid.toString(), body);
        assertStatus(HttpStatus.SC_CREATED, response);

        response = get("/record/UUID." + uuid.toString());
        assertStatus(HttpStatus.SC_OK, response);

        // Create a record using POST and a client-specified UUID
        uuid = UUID.randomUUID();
        body = json("{ action: 'create', record: { id: 'UUID." + uuid.toString() + "', type: 'b$book', " +
                "fields: { 'b$title' : 'Title 1' }, namespaces : { 'org.lilyproject.resttest': 'b' } } }");
        response = post("/record", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        response = get("/record/UUID." + uuid.toString());
        assertStatus(HttpStatus.SC_OK, response);

        // Update this record, specify the ID both in URI and json
        body = json("{ action: 'update', record: { id: 'UUID." + uuid.toString() + "', type: 'b$book', " +
                "fields: { 'b$title' : 'Title 1 (new)' }, " +
                "namespaces : { 'org.lilyproject.resttest': 'b' } } }");
        response = post("/record/UUID." + uuid.toString(), body);
        assertStatus(HttpStatus.SC_OK, response);
    }


    /**
     * Test versioning of record types and the creation of a record that uses the non-latest version of a record type.
     */
    @Test
    public void testVersionRecordType() throws Exception {
        // Create two field types
        String body = json("{action: 'create', fieldType: {name: 'n$vrt_field1', valueType: 'STRING', " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        ResponseAndContent response = post("/schema/fieldTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        body = json("{action: 'create', fieldType: {name: 'n$vrt_field2', valueType: 'STRING', " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        response = post("/schema/fieldTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        // Create a record type
        body = json("{action: 'create', recordType: {name: 'n$vrt', " +
                "fields: [ {name: 'n$vrt_field1'} ]," +
                "namespaces: { 'org.lilyproject.resttest': 'n' } } }");
        response = post("/schema/recordTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);
        String rtIdUri = response.getLocationRef().toString();

        body = json("{name: 'n$vrt', " +
                "fields: [ {name: 'n$vrt_field1'}, {name: 'n$vrt_field2'} ]," +
                "namespaces: { 'org.lilyproject.resttest': 'n' } }");
        response = put("/schema/recordType/n$vrt?ns.n=org.lilyproject.resttest", body);
        assertStatus(HttpStatus.SC_OK, response);

        response = get("/schema/recordType/n$vrt?ns.n=org.lilyproject.resttest");
        assertStatus(HttpStatus.SC_OK, response);
        JsonNode json = readJson(response);
        assertEquals(2L, json.get("version").getLongValue());

        response = get("/schema/recordType/n$vrt/version/2?ns.n=org.lilyproject.resttest");
        assertStatus(HttpStatus.SC_OK, response);
        json = readJson(response);
        assertEquals(2L, json.get("version").getLongValue());

        response = getUri(rtIdUri + "/version/2");
        assertStatus(HttpStatus.SC_OK, response);
        json = readJson(response);
        assertEquals(2L, json.get("version").getLongValue());

        // Create record
        body = json("{ type: {name: 'n$vrt', version: 1}, " +
                "fields: { 'n$vrt_field1' : 'pink shoe laces' }, namespaces : { 'org.lilyproject.resttest': 'n' } }");
        response = put("/record/USER.pink_shoe_laces", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        json = readJson(response);
        assertEquals(1L, json.get("type").get("version").getLongValue());

        // Update record
        body = json("{ type: {name: 'n$vrt', version: 1}, " +
                "fields: { 'n$vrt_field1' : 'pink shoe laces 2' }, namespaces : { 'org.lilyproject.resttest': 'n' } }");
        response = put("/record/USER.pink_shoe_laces", body);
        assertStatus(HttpStatus.SC_OK, response);

        json = readJson(response);
        assertEquals(1L, json.get("type").get("version").getLongValue());

        // Update without specifying version, should move to last version
        body = json("{ type: {name: 'n$vrt'}, " +
                "fields: { 'n$vrt_field1' : 'pink shoe laces 3' }, namespaces : { 'org.lilyproject.resttest': 'n' } }");
        response = put("/record/USER.pink_shoe_laces", body);
        assertStatus(HttpStatus.SC_OK, response);

        json = readJson(response);
        assertEquals(2L, json.get("type").get("version").getLongValue());
    }



    @Test
    public void testBlobs() throws Exception {
        // Create a blob field type
        String body = json("{action: 'create', fieldType: {name: 'b$blob1', valueType: 'BLOB', " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'b' } } }");
        ResponseAndContent response = post("/schema/fieldTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        // Create a record type holding the blob field
        body = json("{action: 'create', recordType: {name: 'b$blobRT', fields: [ {name: 'b$blob1'} ]," +
                "namespaces: { 'org.lilyproject.resttest': 'b' } } }");
        response = post("/schema/recordTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        // Upload a blob
        String data = "Hello, blob world!";
        response = postText("/blob", data);

        assertStatus(HttpStatus.SC_OK, response);
        JsonNode jsonNode = readJson(response);
        String blobValue = jsonNode.get("value").getTextValue();
        assertEquals("text/plain", jsonNode.get("mediaType").getTextValue());
        assertEquals(data.length(), jsonNode.get("size").getLongValue());

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

        response = put("/record/USER.blob1", recordNode.toString());
        assertStatus(HttpStatus.SC_CREATED, response);

        // Read the record
        response = get("/record/USER.blob1");
        assertStatus(HttpStatus.SC_OK, response);

        jsonNode = readJson(response);
        String prefix = jsonNode.get("namespaces").get("org.lilyproject.resttest").getValueAsText();
        blobNode = (ObjectNode)jsonNode.get("fields").get(prefix + "$blob1");
        assertEquals("text/plain", blobNode.get("mediaType").getValueAsText());
        assertEquals(data.length(), blobNode.get("size").getLongValue());
        assertEquals(blobValue, blobNode.get("value").getTextValue());
        assertEquals("helloworld.txt", blobNode.get("name").getValueAsText());

        // Read the blob
        response = get("/record/USER.blob1/field/b$blob1/data?ns.b=org.lilyproject.resttest");
        assertStatus(HttpStatus.SC_OK, response);
        assertEquals(data, new String(response.getContent()));
    }

    @Test
    public void testMultiValueHierarchicalBlobs() throws Exception {
        // Create a blob field type
        String body = json("{action: 'create', fieldType: {name: 'b$blob2', valueType: 'LIST<PATH<BLOB>>', " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'b' } } }");
        ResponseAndContent response = post("/schema/fieldTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        // Create a record type holding the blob field
        body = json("{action: 'create', recordType: {name: 'b$blobRT2', fields: [ {name: 'b$blob2'} ]," +
                "namespaces: { 'org.lilyproject.resttest': 'b' } } }");
        response = post("/schema/recordTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        // Upload some blobs
        ObjectNode blob1Value = (ObjectNode)readJson(postText("/blob", "My blob 1"));
        ObjectNode blob2Value = (ObjectNode)readJson(postText("/blob", "My blob 2"));
        ObjectNode blob3Value = (ObjectNode)readJson(postText("/blob", "My blob 3"));
        ObjectNode blob4Value = (ObjectNode)readJson(postText("/blob", "My blob 4"));

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

        response = put("/record/USER.blob2", recordNode.toString());
        assertStatus(HttpStatus.SC_CREATED, response);

        // Read the record
        response = get("/record/USER.blob2");
        assertStatus(HttpStatus.SC_OK, response);

        JsonNode jsonNode = readJson(response);
        String prefix = jsonNode.get("namespaces").get("org.lilyproject.resttest").getValueAsText();
        blobNode = (ArrayNode)jsonNode.get("fields").get(prefix + "$blob2");
        assertEquals(2, blobNode.size());

        // Read the blobs
        for (int mvIndex = 0; mvIndex < 2; mvIndex++) {
            for (int hIndex = 0; hIndex < 2; hIndex++) {
                response = get("/record/USER.blob2/field/b$blob2/data?ns.b=org.lilyproject.resttest" +
                        "&indexes=" + mvIndex + "," + hIndex);
                assertStatus(HttpStatus.SC_OK, response);
            }
        }

        // Read a blob at non-existing indexes
        response = get("/record/USER.blob2/field/b$blob2/data?ns.b=org.lilyproject.resttest&mvIndex=5&hIndex=3");
        assertStatus(HttpStatus.SC_NOT_FOUND, response);
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
    public void testVariantCollection() throws Exception {
        makeBookSchema();

        String body = json("{ type: 'b$book', fields: { 'b$title' : 'Hunting' }, namespaces : { 'org.lilyproject.resttest': 'b' } }");
        ResponseAndContent response = put("/record/USER.hunting.lang=en", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        body = json("{ type: 'b$book', fields: { 'b$title' : 'Jagen' }, namespaces : { 'org.lilyproject.resttest': 'b' } }");
        response = put("/record/USER.hunting.lang=nl", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        body = json("{ type: 'b$book', fields: { 'b$title' : 'La chasse' }, namespaces : { 'org.lilyproject.resttest': 'b' } }");
        response = put("/record/USER.hunting.lang=fr", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        response = get("/record/USER.hunting/variant");
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
        ResponseAndContent response = post("/schema/fieldTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        body = json("{ type: 'b$book', fields: { 'b$title' : 'Title version 1' }, namespaces : { 'org.lilyproject.resttest': 'b' } }");
        response = put("/record/USER.vtagtest", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        body = json("{ type: 'b$book', fields: { 'b$title' : 'Title version 2', 'v$active': 1 }, " +
                "namespaces : { 'org.lilyproject.resttest': 'b', 'org.lilyproject.vtag': 'v' } }");
        response = put("/record/USER.vtagtest", body);
        assertStatus(HttpStatus.SC_OK, response);

        response = get("/record/USER.vtagtest/vtag/active");
        assertStatus(HttpStatus.SC_OK, response);
        assertEquals(1L, readJson(response).get("version").getLongValue());

        response = get("/record/USER.vtagtest/vtag/last");
        assertStatus(HttpStatus.SC_OK, response);
        assertEquals(2L, readJson(response).get("version").getLongValue());

        response = get("/record/USER.vtagtest/vtag/aNotExistingVTagName");
        assertStatus(HttpStatus.SC_NOT_FOUND, response);
    }

    @Test
    public void testVersionedMutableScope() throws Exception {
        // Create a versioned field type
        String body = json("{action: 'create', fieldType: {name: 'b$subject', valueType: 'STRING', " +
                "scope: 'versioned', namespaces: { 'org.lilyproject.resttest': 'b' } } }");
        ResponseAndContent response = post("/schema/fieldTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        // Create a versioned-mutable field type
        body = json("{action: 'create', fieldType: {name: 'b$state', valueType: 'STRING', " +
                "scope: 'versioned_mutable', namespaces: { 'org.lilyproject.resttest': 'b' } } }");
        response = post("/schema/fieldTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        // Create a record type
        body = json("{action: 'create', recordType: {name: 'b$article', " +
                "fields: [ {name: 'b$subject'}, {name: 'b$state'} ]," +
                "namespaces: { 'org.lilyproject.resttest': 'b' } } }");
        response = post("/schema/recordTypeById", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        // Create a record
        body = json("{ type: 'b$article', fields: { 'b$subject': 'Peace', 'b$state': 'draft' }, namespaces : { 'org.lilyproject.resttest': 'b' } }");
        response = put("/record/USER.peace", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        // Read the record
        response = get("/record/USER.peace");
        assertStatus(HttpStatus.SC_OK, response);

        JsonNode json = readJson(response);
        JsonNode fieldsNode = json.get("fields");
        String prefix = json.get("namespaces").get("org.lilyproject.resttest").getTextValue();

        assertEquals("draft", fieldsNode.get(prefix + "$state").getValueAsText());
        assertEquals("Peace", fieldsNode.get(prefix + "$subject").getValueAsText());

        // Update the versioned-mutable field
        body = json("{ fields: { 'b$state': 'in_review' }, namespaces : { 'org.lilyproject.resttest': 'b' } }");
        response = put("/record/USER.peace/version/1", body);
        assertStatus(HttpStatus.SC_OK, response);

        response = get("/record/USER.peace");
        assertStatus(HttpStatus.SC_OK, response);

        json = readJson(response);
        fieldsNode = json.get("fields");
        prefix = json.get("namespaces").get("org.lilyproject.resttest").getTextValue();

        assertEquals(1L, json.get("version").getLongValue()); // no new version created
        assertEquals("in_review", fieldsNode.get(prefix + "$state").getValueAsText());

        // Update versioned-mutable field without changes, change to versioned field should be ignored
        body = json("{ fields: { 'b$subject': 'Peace2', 'b$state': 'in_review' }, namespaces : { 'org.lilyproject.resttest': 'b' } }");
        response = put("/record/USER.peace/version/1", body);
        assertStatus(HttpStatus.SC_OK, response);

        response = get("/record/USER.peace");
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
        ResponseAndContent response = put("/record/USER.ABC", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        // Test update with failing condition
        body = json("{ action: 'update', record: { fields: { 'b$title' : 'ABC2' } }, " +
                "conditions: [{field: 'b$title', value: 'ABC5'}], " +
                "namespaces : { 'org.lilyproject.resttest': 'b' } }");

        response = post("/record/USER.ABC", body);
        assertStatus(HttpStatus.SC_CONFLICT, response);

        // Verify update did not go through
        response = get("/record/USER.ABC");
        assertStatus(HttpStatus.SC_OK, response);

        JsonNode json = readJson(response);
        assertEquals("ABC1", getFieldValue(json, "title").getTextValue());

        // Test update with succeeding condition
        body = json("{ action: 'update', record: { fields: { 'b$title' : 'ABC2' } }, " +
                "conditions: [{field: 'b$title', value: 'ABC1'}], " +
                "namespaces : { 'org.lilyproject.resttest': 'b' } }");

        response = post("/record/USER.ABC", body);

        assertStatus(HttpStatus.SC_OK, response);

        // Verify update did not through
        response = get("/record/USER.ABC");
        assertStatus(HttpStatus.SC_OK, response);

        json = readJson(response);
        assertEquals("ABC2", getFieldValue(json, "title").getTextValue());

        //
        // Test with custom compare operator
        //
        body = json("{ action: 'update', record: { fields: { 'b$title' : 'ABC3' } }, " +
                "conditions: [{field: 'b$title', value: 'ABC2', operator: 'not_equal'}], " +
                "namespaces : { 'org.lilyproject.resttest': 'b' } }");

        response = post("/record/USER.ABC", body);
        assertStatus(HttpStatus.SC_CONFLICT, response);

        //
        // Test allowMissing flag
        //
        body = json("{ action: 'update', record: { fields: { 'b$title' : 'ABC3' } }, " +
                "conditions: [{field: 'b$summary', value: 'some summary', allowMissing: false}], " +
                "namespaces : { 'org.lilyproject.resttest': 'b' } }");

        response = post("/record/USER.ABC", body);
        assertStatus(HttpStatus.SC_CONFLICT, response);

        body = json("{ action: 'update', record: { fields: { 'b$title' : 'ABC3' } }, " +
                "conditions: [{field: 'b$summary', value: 'some summary', allowMissing: true}], " +
                "namespaces : { 'org.lilyproject.resttest': 'b' } }");

        response = post("/record/USER.ABC", body);
        assertStatus(HttpStatus.SC_OK, response);

        //
        // Test null value
        //
        // First remove summary field again
        body = json("{ action: 'update', record: { " +
                "fieldsToDelete: ['b$summary'], " +
                "namespaces : { 'org.lilyproject.resttest': 'b' } } }");
        response = post("/record/USER.ABC", body);
        assertStatus(HttpStatus.SC_OK, response);

        body = json("{ action: 'update', record: { fields: { 'b$title' : 'ABC4' } }, " +
                "conditions: [{field: 'b$summary', value: null, operator: 'not_equal'}], " +
                "namespaces : { 'org.lilyproject.resttest': 'b' } } }");

        response = post("/record/USER.ABC", body);
        assertStatus(HttpStatus.SC_CONFLICT, response);

        body = json("{ action: 'update', record: { fields: { 'b$title' : 'ABC4' } }, " +
                "conditions: [{field: 'b$summary', value: null, operator: 'equal'}], " +
                "namespaces : { 'org.lilyproject.resttest': 'b' } } }");

        response = post("/record/USER.ABC", body);
        assertStatus(HttpStatus.SC_OK, response);

        //
        // Test system field check
        //
        // Create a new record
        body = json("{ type: 'b$book', fields: { 'b$title' : 'ABC1' }, namespaces : { 'org.lilyproject.resttest': 'b' } }");
        response = put("/record/USER.sysfieldcheck", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        // Test update with failing condition
        body = json("{ action: 'update', record: { fields: { 'b$title' : 'ABC2' } }, " +
                "conditions: [{field: 's$version', value: 2}], " +
                "namespaces : { 'org.lilyproject.resttest': 'b', 'org.lilyproject.system': 's' } }");

        response = post("/record/USER.sysfieldcheck", body);
        assertStatus(HttpStatus.SC_CONFLICT, response);

        // Test update with succeeding condition
        body = json("{ action: 'update', record: { fields: { 'b$title' : 'ABC2' } }, " +
                "conditions: [{field: 's$version', value: 1}], " +
                "namespaces : { 'org.lilyproject.resttest': 'b', 'org.lilyproject.system': 's' } }");

        response = post("/record/USER.sysfieldcheck", body);
        assertStatus(HttpStatus.SC_OK, response);
    }

    @Test
    public void testConditionDelete() throws Exception {
        makeBookSchema();

        String body = json("{ type: 'b$book', fields: { 'b$title' : 'CondDel' }, namespaces : { 'org.lilyproject.resttest': 'b' } }");
        ResponseAndContent response = put("/record/USER.ConDel", body);
        assertStatus(HttpStatus.SC_CREATED, response);

        body = json("{ action: 'delete'," +
                "conditions: [{field: 'b$title', value: 'foo'}], " +
                "namespaces : { 'org.lilyproject.resttest': 'b' } } }");

        response = post("/record/USER.ConDel", body);
        assertStatus(HttpStatus.SC_CONFLICT, response);

        body = json("{ action: 'delete'," +
                "conditions: [{field: 'b$title', value: 'CondDel'}], " +
                "namespaces : { 'org.lilyproject.resttest': 'b' } } }");

        response = post("/record/USER.ConDel", body);
        assertStatus(HttpStatus.SC_NO_CONTENT, response);
    }

    private void setupRecordScannerTest () throws Exception{
        makeBookSchema();

        // Create a record using PUT and a user ID
        //TODO: validate response
        String body = json("{ type: 'b$book', fields: { 'b$title' : 'Faster Fishing' }, namespaces : { 'org.lilyproject.resttest': 'b' } }");
        ResponseAndContent response = put("/record/USER.scan_faster_fishing", body);
        body = json("{ type: 'b$book', fields: { 'b$title' : 'Fister Fashing' }, namespaces : { 'org.lilyproject.resttest': 'b' } }");
        response = put("/record/USER.scan_fister_fashing", body);
        body = json("{ type: 'b$book', fields: { 'b$title' : 'Fly fishing with Flash' }, namespaces : { 'org.lilyproject.resttest': 'b' } }");
        response = put("/record/USER.scan_fly_fishing_with_flash", body);
    }

    @Test
    public void testRecordScanPost() throws Exception {
        setupRecordScannerTest();

        // Create a scanner to see if it gest created. Check the location header
        String body = json("{'recordFilter' : { '@class' : 'org.lilyproject.repository.api.filter.RecordIdPrefixFilter', " +
                "'recordId' : 'USER.scan_'}}, 'caching' : 1024, 'cacheBlocks' : false}");
        ResponseAndContent response = post("/scan", body);
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
        ResponseAndContent response = post("/scan", body);
        assertStatus(HttpStatus.SC_CREATED, response);
        String location = response.getLocationRef().toString();

     // Check if the scanner can be retrieved. Retrieve 1 record by default
        response = getUri(location);
        assertStatus(HttpStatus.SC_OK, response);
        JsonNode json = readJson(response);
        assertTrue(json.get("results").size() == 1);

        // Check to see if the batch parameter gets more records
        response = getUri(location + "?batch=2");
        assertStatus(HttpStatus.SC_OK, response);
        json = readJson(response);
        assertTrue(json.get("results").size() == 2);

        // When the scanner runs out send 204 NO CONTENT
        response = getUri(location);
        assertStatus(HttpStatus.SC_NO_CONTENT, response);

        // GETs on non existing scanners get 404
        response = getUri(location + "blablabla");
        assertStatus(HttpStatus.SC_NOT_FOUND, response);

    }

    @Test
    public void testRecordScanDelete() throws Exception {
        setupRecordScannerTest();

        // Create a scanner to see if it gest created. Check the location header
        String body = json("{'recordFilter' : { '@class' : 'org.lilyproject.repository.api.filter.RecordIdPrefixFilter', " +
                "'recordId' : 'USER.scan_'}}, 'caching' : 1024, 'cacheBlocks' : false}");
        ResponseAndContent response = post("/scan", body);
        assertStatus(HttpStatus.SC_CREATED, response);
        String location = response.getLocationRef().toString();

        // Delete scanner
        response = deleteUri(location);
        assertStatus(HttpStatus.SC_OK, response);

        // Check that scanner is gone
        response = getUri(location);
        assertStatus(HttpStatus.SC_NOT_FOUND, response);

        // Delete a non existing scanner gets a 404
        response = deleteUri(location + "not_here");
        assertStatus(HttpStatus.SC_NOT_FOUND, response);
    }



}
