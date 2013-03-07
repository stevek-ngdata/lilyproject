/*
 * Copyright 2012 NGDATA nv
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
package org.lilyproject.tools.import_.json.test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.bytes.api.ByteArray;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.Metadata;
import org.lilyproject.repository.api.MetadataBuilder;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.RecordScan;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.repository.api.ReturnFields;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.api.filter.FieldValueFilter;
import org.lilyproject.repository.api.filter.RecordFilterList;
import org.lilyproject.repository.api.filter.RecordIdPrefixFilter;
import org.lilyproject.repository.api.filter.RecordTypeFilter;
import org.lilyproject.repository.api.filter.RecordVariantFilter;
import org.lilyproject.repository.impl.id.IdGeneratorImpl;
import org.lilyproject.repotestfw.RepositorySetup;
import org.lilyproject.tools.import_.cli.JsonImport;
import org.lilyproject.tools.import_.json.JsonFormatException;
import org.lilyproject.tools.import_.json.NamespacesImpl;
import org.lilyproject.tools.import_.json.RecordReader;
import org.lilyproject.tools.import_.json.RecordScanReader;
import org.lilyproject.tools.import_.json.RecordScanWriter;
import org.lilyproject.tools.import_.json.RecordWriter;
import org.lilyproject.tools.import_.json.WriteOptions;
import org.lilyproject.util.hbase.LilyHBaseSchema.Table;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.json.JsonFormat;

public class JsonConversionTest {
    private final static RepositorySetup repoSetup = new RepositorySetup();
    private static RepositoryManager repositoryManager;
    private static Repository repository;
    private RecordScanWriter writer;
    private RecordScanReader reader;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        repoSetup.setupCore();
        repoSetup.setupRepository();

        repositoryManager = repoSetup.getRepositoryManager();
        repository = repoSetup.getRepositoryManager().getRepository(Table.RECORD.name);

        TypeManager typeManager = repository.getTypeManager();
        typeManager.createFieldType("STRING", new QName("ns", "stringField"), Scope.NON_VERSIONED);
        typeManager.createFieldType("LIST<STRING>", new QName("ns", "stringListField"), Scope.NON_VERSIONED);
        typeManager.createFieldType("LONG", new QName("ns", "longField"), Scope.NON_VERSIONED);
        typeManager.recordTypeBuilder()
                .defaultNamespace("ns")
                .name("rt")
                .fieldEntry()
                .name("stringField")
                .add()
                .fieldEntry()
                .name("stringListField")
                .add()
                .fieldEntry()
                .name("longField")
                .add()
                .create();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        Closer.close(repoSetup);
    }

    @Before
    public void setup() {
        writer = new RecordScanWriter();
        reader = new RecordScanReader();
    }

    /**
     * Tests that namespaces can be declared globally and/or locally.
     */
    @Test
    public void testNamespaceContexts() throws Exception {
        JsonImport.load(repository, getClass().getResourceAsStream("nscontexttest.json"), false);

        Record record1 = repository.read(repository.getIdGenerator().fromString("USER.record1"));
        assertEquals("value1", record1.getField(new QName("import1", "f1")));
        assertEquals(new Integer(55), record1.getField(new QName("import2", "f2")));
    }

    private byte[] scanToBytes(RecordScan scan) throws RepositoryException, InterruptedException, IOException {
        return JsonFormat.serializeAsBytes(
                writer.toJson(scan, new WriteOptions(), new NamespacesImpl(false), repositoryManager));
    }

    private RecordScan scanFromBytes(byte[] data) throws IOException, RepositoryException, JsonFormatException,
            InterruptedException {
        return reader.fromJson(JsonFormat.deserializeNonStd(data), new NamespacesImpl(false), repositoryManager);
    }

    @Test
    public void testScanRecordId() throws Exception {
        IdGenerator idGenerator = new IdGeneratorImpl();

        RecordScan scan = new RecordScan();
        scan.setStartRecordId(idGenerator.newRecordId());
        scan.setStopRecordId(idGenerator.newRecordId("foo"));

        byte[] data = scanToBytes(scan);
        RecordScan parsedScan = scanFromBytes(data);

        assertEquals(scan.getStartRecordId(), parsedScan.getStartRecordId());
        assertEquals(scan.getStopRecordId(), parsedScan.getStopRecordId());

        // RecordId's should be simply string properties in json
        JsonNode node = new ObjectMapper().readTree(data);
        assertEquals(scan.getStartRecordId().toString(), node.get("startRecordId").getTextValue());
        assertEquals("USER.foo", node.get("stopRecordId").getTextValue());
    }

    @Test
    public void testScanCustomJson() throws Exception {
        // verify the json parser accepts comments and unquoted attributes
        String json = "{ /* a comment */ startRecordId: \"USER.foo\"}";
        scanFromBytes(json.getBytes());
    }

    @Test
    public void testScanRawStartStop() throws Exception {
        RecordScan scan = new RecordScan();
        scan.setRawStartRecordId(Bytes.toBytes("bar"));
        scan.setRawStopRecordId(Bytes.toBytes("foo"));

        byte[] data = scanToBytes(scan);
        RecordScan parsedScan = scanFromBytes(data);

        assertArrayEquals(scan.getRawStartRecordId(), parsedScan.getRawStartRecordId());
        assertArrayEquals(scan.getRawStopRecordId(), parsedScan.getRawStopRecordId());

        // Verify how the bytes are stored in the json
        JsonNode node = new ObjectMapper().readTree(data);
        assertEquals("YmFy", node.get("rawStartRecordId").getTextValue());
        assertEquals("Zm9v", node.get("rawStopRecordId").getTextValue());
    }

    @Test
    public void testScanCaching() throws Exception {
        RecordScan scan = new RecordScan();
        scan.setCacheBlocks(false);
        scan.setCaching(500);

        byte[] data = scanToBytes(scan);
        RecordScan parsedScan = scanFromBytes(data);

        assertEquals(false, parsedScan.getCacheBlocks());
        assertEquals(500, parsedScan.getCaching());
    }

    @Test
    public void testScanRecordTypeFilter() throws Exception {
        QName recordType = new QName("ns", "rt");

        RecordScan scan = new RecordScan();
        scan.setRecordFilter(new RecordTypeFilter(recordType));

        byte[] data = scanToBytes(scan);
        RecordScan parsedScan = scanFromBytes(data);

        assertNotNull(parsedScan.getRecordFilter());
        assertTrue(parsedScan.getRecordFilter() instanceof RecordTypeFilter);
        assertEquals(recordType, ((RecordTypeFilter) parsedScan.getRecordFilter()).getRecordType());
        assertNull(((RecordTypeFilter) parsedScan.getRecordFilter()).getVersion());

        // Check json
        JsonNode node = new ObjectMapper().readTree(data);
        assertEquals("org.lilyproject.repository.api.filter.RecordTypeFilter",
                node.get("recordFilter").get("@class").getTextValue());
        assertEquals("{ns}rt",
                node.get("recordFilter").get("recordType").getTextValue());
    }

    @Test
    public void testScanRecordIdPrefixFilter() throws Exception {
        IdGenerator idGenerator = new IdGeneratorImpl();

        RecordId recordId = idGenerator.newRecordId("foo");

        RecordScan scan = new RecordScan();
        scan.setRecordFilter(new RecordIdPrefixFilter(recordId));

        byte[] data = scanToBytes(scan);
        RecordScan parsedScan = scanFromBytes(data);

        assertNotNull(parsedScan.getRecordFilter());
        assertTrue(parsedScan.getRecordFilter() instanceof RecordIdPrefixFilter);
        assertEquals(recordId, ((RecordIdPrefixFilter) parsedScan.getRecordFilter()).getRecordId());

        // Check json
        JsonNode node = new ObjectMapper().readTree(data);
        assertEquals("org.lilyproject.repository.api.filter.RecordIdPrefixFilter",
                node.get("recordFilter").get("@class").getTextValue());
        assertEquals("USER.foo",
                node.get("recordFilter").get("recordId").getTextValue());
    }

    @Test
    public void testScanFieldValueFilter() throws Exception {
        QName name = new QName("ns", "stringField");
        Object value = "foo";

        RecordScan scan = new RecordScan();
        scan.setRecordFilter(new FieldValueFilter(name, value));

        byte[] data = scanToBytes(scan);
        RecordScan parsedScan = scanFromBytes(data);

        assertNotNull(parsedScan.getRecordFilter());
        assertTrue(parsedScan.getRecordFilter() instanceof FieldValueFilter);
        FieldValueFilter filter = (FieldValueFilter) parsedScan.getRecordFilter();
        assertEquals(name, filter.getField());
        assertEquals(value, filter.getFieldValue());
        assertTrue(filter.getFilterIfMissing());

        // Check json structure
        JsonNode node = new ObjectMapper().readTree(data);
        assertEquals("org.lilyproject.repository.api.filter.FieldValueFilter",
                node.get("recordFilter").get("@class").getTextValue());
        assertEquals("foo", node.get("recordFilter").get("fieldValue").getTextValue());

        // Try different data types as field value
        value = 3L;
        scan.setRecordFilter(new FieldValueFilter(new QName("ns", "longField"), value));
        assertEquals(value, ((FieldValueFilter) scanFromBytes(scanToBytes(scan))
                .getRecordFilter()).getFieldValue());

        value = Lists.newArrayList("foo", "bar");
        scan.setRecordFilter(new FieldValueFilter(new QName("ns", "stringListField"), value));
        assertEquals(value, ((FieldValueFilter) scanFromBytes(scanToBytes(scan))
                .getRecordFilter()).getFieldValue());

        // The following test made more sense when we were using a generic type-detection
        // in the json serialization rather than using TypeManager.
        // Use a list as field value, but with a mixture of datatypes. This should fail,
        // as lists in Lily should contain values of the same type.
        value = Lists.newArrayList("foo", 123L);
        scan.setRecordFilter(new FieldValueFilter(new QName("ns", "stringListField"), value));
        try {
            data = scanToBytes(scan);
            fail("Expected exception with list containing different data types");
        } catch (Exception e) {
            // expected
        }
    }

    @Test
    public void testScanRecordVariantFilter() throws Exception {
        IdGenerator idGenerator = new IdGeneratorImpl();

        final Map<String, String> variantProperties = new HashMap<String, String>();
        variantProperties.put("lang", "en");
        variantProperties.put("branch", null);

        RecordId recordId = idGenerator.newRecordId("foo");

        RecordScan scan = new RecordScan();
        scan.setRecordFilter(new RecordVariantFilter(recordId, variantProperties));

        byte[] data = scanToBytes(scan);
        RecordScan parsedScan = scanFromBytes(data);

        assertNotNull(parsedScan.getRecordFilter());
        assertTrue(parsedScan.getRecordFilter() instanceof RecordVariantFilter);
        assertEquals(recordId.getMaster(), ((RecordVariantFilter) parsedScan.getRecordFilter()).getMasterRecordId());
        assertEquals(variantProperties, ((RecordVariantFilter) parsedScan.getRecordFilter()).getVariantProperties());

        // Check json
        JsonNode node = new ObjectMapper().readTree(data);
        assertEquals("org.lilyproject.repository.api.filter.RecordVariantFilter",
                node.get("recordFilter").get("@class").getTextValue());
        assertEquals("USER.foo", node.get("recordFilter").get("recordId").getTextValue());
        assertEquals("en", node.get("recordFilter").get("variantProperties").get("lang").getTextValue());
        assertEquals(null, node.get("recordFilter").get("variantProperties").get("branch").getTextValue());
    }

    @Test
    public void testScanRecordFilterList() throws Exception {
        IdGenerator idGenerator = new IdGeneratorImpl();

        RecordId recordId = idGenerator.newRecordId("foo");

        RecordScan scan = new RecordScan();
        RecordFilterList filterList = new RecordFilterList(RecordFilterList.Operator.MUST_PASS_ONE);
        filterList.addFilter(new RecordIdPrefixFilter(recordId));
        filterList.addFilter(new RecordTypeFilter(new QName("ns", "stringField")));
        scan.setRecordFilter(filterList);

        byte[] data = scanToBytes(scan);
        RecordScan parsedScan = scanFromBytes(data);

        assertNotNull(parsedScan.getRecordFilter());
        assertTrue(parsedScan.getRecordFilter() instanceof RecordFilterList);
        assertTrue(filterList.getFilters().get(0) instanceof RecordIdPrefixFilter);
        assertTrue(filterList.getFilters().get(1) instanceof RecordTypeFilter);
        assertEquals(RecordFilterList.Operator.MUST_PASS_ONE, filterList.getOperator());

        // Check json
        JsonNode node = new ObjectMapper().readTree(data);
        assertEquals("org.lilyproject.repository.api.filter.RecordFilterList",
                node.get("recordFilter").get("@class").getTextValue());
        assertTrue(node.get("recordFilter").get("filters").isArray());
        assertEquals(2, node.get("recordFilter").get("filters").size());
        assertEquals("org.lilyproject.repository.api.filter.RecordIdPrefixFilter",
                node.get("recordFilter").get("filters").get(0).get("@class").getTextValue());
    }

    @Test
    public void testScanReturnFields() throws Exception {
        RecordScan scan = new RecordScan();
        scan.setReturnFields(ReturnFields.NONE);

        byte[] data = scanToBytes(scan);
        RecordScan parsedScan = scanFromBytes(data);

        assertEquals(ReturnFields.NONE.getType(), parsedScan.getReturnFields().getType());

        // Test with enumeration of fields to return
        scan.setReturnFields(new ReturnFields(new QName("ns", "f1"), new QName("ns", "f2")));
        data = scanToBytes(scan);
        parsedScan = scanFromBytes(data);

        assertEquals(ReturnFields.Type.ENUM, parsedScan.getReturnFields().getType());
        assertEquals(Lists.newArrayList(new QName("ns", "f1"), new QName("ns", "f2")),
                parsedScan.getReturnFields().getFields());
    }

    @Test
    public void testScanNamespaces() throws Exception {
        QName name = new QName("ns", "stringField");
        Object value = "foo";

        RecordScan scan = new RecordScan();
        scan.setRecordFilter(new FieldValueFilter(name, value));

        // Test serialization without namespace prefixes
        byte[] data = scanToBytes(scan);

        JsonNode node = new ObjectMapper().readTree(data);
        assertNull(node.get("namespaces"));
        assertEquals("{ns}stringField", node.get("recordFilter").get("field").getTextValue());

        // Test serialization with namespace prefixes
        byte[] dataWithPrefixes = JsonFormat.serializeAsBytes(
                writer.toJson(scan, new WriteOptions(), repositoryManager));

        JsonNode nodeWithPrefixes = new ObjectMapper().readTree(dataWithPrefixes);
        assertNotNull(nodeWithPrefixes.get("namespaces"));
        assertTrue(nodeWithPrefixes.get("recordFilter").get("field").getTextValue().endsWith("$stringField"));
    }

    @Test
    public void testRecordAttributes() throws Exception {
        Record record = repository.newRecord();
        record.getAttributes().put("one", "onevalue");

        ObjectNode recordNode = RecordWriter.INSTANCE.toJson(record, null, repositoryManager);
        ObjectNode attributes = (ObjectNode) recordNode.get("attributes");
        for (String key : record.getAttributes().keySet()) {
            Assert.assertEquals(record.getAttributes().get(key), attributes.get(key).asText());
        }

        attributes.put("write", "something new");

        record = RecordReader.INSTANCE.fromJson(recordNode, repositoryManager);
        Iterator<Entry<String, JsonNode>> it = attributes.getFields();
        while (it.hasNext()) {
            Entry<String,JsonNode> attr = it.next();
            Assert.assertEquals(attr.getValue().asText(), record.getAttributes().get(attr.getKey()));
        }
    }

    @Test
    public void testMetadata() throws Exception {
        Record record = repository.newRecord();
        record.setMetadata(new QName("ns", "field1"),
                new MetadataBuilder()
                        .value("stringfield", "string")
                        .value("intfield", 55)
                        .value("longfield", 999999999999L)
                        .value("booleanfield", Boolean.TRUE)
                        .value("floatfield", 33.33f)
                        .value("doublefield", 66.66d)
                        .value("binaryfield", new ByteArray("foo".getBytes()))
                        .build());

        record.setMetadata(new QName("ns", "field2"),
                new MetadataBuilder()
                        .value("stringfield", "another_string")
                        .build());

        ObjectNode recordNode = RecordWriter.INSTANCE.toJson(record, null, repositoryManager);

        // go through ser/deser
        String recordJson = JsonFormat.serializeAsString(recordNode);
        recordNode = (ObjectNode)JsonFormat.deserializeNonStd(recordJson);

        // Do some structural validations of the json
        ObjectNode metadataNode = (ObjectNode)recordNode.get("metadata");
        assertNotNull(metadataNode);
        assertNull(recordNode.get("metadataToDelete"));

        String prefix = recordNode.get("namespaces").get("ns").getTextValue();

        assertEquals(2, metadataNode.size());

        assertTrue(metadataNode.get(prefix + "$field1").get("stringfield").isTextual());
        assertEquals("string", metadataNode.get(prefix + "$field1").get("stringfield").getTextValue());

        assertTrue(metadataNode.get(prefix + "$field1").get("intfield").isInt());
        assertEquals(55, metadataNode.get(prefix + "$field1").get("intfield").getIntValue());

        assertTrue(metadataNode.get(prefix + "$field1").get("longfield").isLong());
        assertEquals(999999999999L, metadataNode.get(prefix + "$field1").get("longfield").getLongValue());

        assertTrue(metadataNode.get(prefix + "$field1").get("booleanfield").isBoolean());
        assertEquals(Boolean.TRUE, metadataNode.get(prefix + "$field1").get("booleanfield").getBooleanValue());

        // in JSON, no distinction between floats & doubles
        assertTrue(metadataNode.get(prefix + "$field1").get("floatfield").isFloatingPointNumber());
        assertEquals(33.33d, metadataNode.get(prefix + "$field1").get("floatfield").getDoubleValue(), 0.001);

        assertTrue(metadataNode.get(prefix + "$field1").get("doublefield").isFloatingPointNumber());
        assertEquals(33.33d, metadataNode.get(prefix + "$field1").get("floatfield").getDoubleValue(), 0.001);

        assertTrue(metadataNode.get(prefix + "$field1").get("binaryfield").isObject());
        ObjectNode binaryNode = (ObjectNode)metadataNode.get(prefix + "$field1").get("binaryfield");
        assertEquals("binary", binaryNode.get("type").getTextValue());
        assertArrayEquals("foo".getBytes(), binaryNode.get("value").getBinaryValue());

        assertTrue(metadataNode.get(prefix + "$field2").get("stringfield").isTextual());
        assertEquals("another_string", metadataNode.get(prefix + "$field2").get("stringfield").getTextValue());

        // Now parse json again to API objects
        record = RecordReader.INSTANCE.fromJson(recordNode, repositoryManager);
        assertEquals(2, record.getMetadataMap().size());

        Metadata metadata = record.getMetadata(new QName("ns", "field1"));
        assertEquals("string", metadata.get("stringfield"));
        assertEquals(55, metadata.getInt("intfield", null).intValue());
        assertEquals(999999999999L, metadata.getLong("longfield", null).longValue());
        assertEquals(Boolean.TRUE, metadata.getBoolean("booleanfield", null).booleanValue());
        assertEquals(33.33f, metadata.getFloat("floatfield", null).floatValue(), 0.001);
        assertEquals(66.66d, metadata.getDouble("doublefield", null).doubleValue(), 0.001);
        assertEquals(new ByteArray("foo".getBytes()), metadata.getBytes("binaryfield"));

        metadata = record.getMetadata(new QName("ns", "field2"));
        assertEquals(1, metadata.getMap().size());
        assertEquals(0, metadata.getFieldsToDelete().size());
    }

    @Test
    public void testMetadataToDelete() throws Exception {
        Record record = repository.newRecord();
        record.setMetadata(new QName("ns", "field1"),
                new MetadataBuilder()
                        .value("mfield1", "value1")
                        .delete("mfield2")
                        .delete("mfield3")
                        .build());

        record.setMetadata(new QName("ns", "field2"),
                new MetadataBuilder()
                        .delete("mfield4")
                        .build());

        WriteOptions options = new WriteOptions();
        options.setUseNamespacePrefixes(false);
        ObjectNode recordNode = RecordWriter.INSTANCE.toJson(record, options, repositoryManager);

        // Go through ser/deser
        String recordJson = JsonFormat.serializeAsString(recordNode);
        recordNode = (ObjectNode)JsonFormat.deserializeNonStd(recordJson);

        assertNotNull(recordNode.get("metadataToDelete"));
        assertEquals(2, recordNode.get("metadataToDelete").size());

        assertEquals(2, recordNode.get("metadataToDelete").get("{ns}field1").size());
        assertEquals(1, recordNode.get("metadataToDelete").get("{ns}field2").size());

        // Now parse json again to API objects
        record = RecordReader.INSTANCE.fromJson(recordNode, repositoryManager);
        assertEquals(2, record.getMetadataMap().size());

        Metadata metadata = record.getMetadata(new QName("ns", "field1"));
        assertTrue(metadata.getFieldsToDelete().contains("mfield2"));
        assertTrue(metadata.getFieldsToDelete().contains("mfield3"));
        assertEquals("value1", metadata.get("mfield1"));

        metadata = record.getMetadata(new QName("ns", "field2"));
        assertTrue(metadata.getFieldsToDelete().contains("mfield4"));
        assertEquals(1, metadata.getFieldsToDelete().size());
    }
}
