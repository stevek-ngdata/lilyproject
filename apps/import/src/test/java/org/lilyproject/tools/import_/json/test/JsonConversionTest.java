package org.lilyproject.tools.import_.json.test;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.LocalDate;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.repository.api.*;
import org.lilyproject.repository.api.filter.FieldValueFilter;
import org.lilyproject.repository.api.filter.RecordFilterList;
import org.lilyproject.repository.api.filter.RecordIdPrefixFilter;
import org.lilyproject.repository.api.filter.RecordTypeFilter;
import org.lilyproject.repository.impl.id.IdGeneratorImpl;
import org.lilyproject.repotestfw.RepositorySetup;
import org.lilyproject.tools.import_.cli.JsonImport;
import org.lilyproject.tools.import_.json.RecordScanReader;
import org.lilyproject.tools.import_.json.RecordScanWriter;
import org.lilyproject.util.io.Closer;

import java.math.BigDecimal;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JsonConversionTest {
    private final static RepositorySetup repoSetup = new RepositorySetup();
    private static Repository repository;
    private RecordScanWriter writer;
    private RecordScanReader reader;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        repoSetup.setupCore();
        repoSetup.setupRepository(true);

        repository = repoSetup.getRepository();
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

    @Test
    public void testScanRecordId() throws Exception {
        IdGenerator idGenerator = new IdGeneratorImpl();

        RecordScan scan = new RecordScan();
        scan.setStartRecordId(idGenerator.newRecordId());
        scan.setStopRecordId(idGenerator.newRecordId("foo"));

        byte[] data = writer.toJsonBytes(scan, repository);
        RecordScan parsedScan = reader.fromJsonBytes(data, repository);

        assertEquals(scan.getStartRecordId(), parsedScan.getStartRecordId());
        assertEquals(scan.getStopRecordId(), parsedScan.getStopRecordId());

        // RecordId's should be simply string properties in json
        JsonNode node = new ObjectMapper().readTree(data);
        assertEquals(scan.getStartRecordId().toString(), node.get("startRecordId").getTextValue());
        assertEquals("USER.foo", node.get("stopRecordId").getTextValue());
    }

    @Test
    public void testScanCustomJson() throws Exception {
        IdGenerator idGenerator = new IdGeneratorImpl();

        // verify the json parser accepts comments and unquoted attributes
        String json = "{ /* a comment */ startRecordId: \"USER.foo\"}";
        RecordScan parsedScan = reader.fromJsonBytes(json.getBytes(), repository);
    }

    @Test
    public void testScanRawStartStop() throws Exception {
        IdGenerator idGenerator = new IdGeneratorImpl();

        RecordScan scan = new RecordScan();
        scan.setRawStartRecordId(Bytes.toBytes("bar"));
        scan.setRawStopRecordId(Bytes.toBytes("foo"));

        byte[] data = writer.toJsonBytes(scan, repository);
        RecordScan parsedScan = reader.fromJsonBytes(data, repository);

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

        byte[] data = writer.toJsonBytes(scan, repository);
        RecordScan parsedScan = reader.fromJsonBytes(data, repository);
        
        assertEquals(false, parsedScan.getCacheBlocks());
        assertEquals(500, parsedScan.getCaching());
    }
    
    @Test
    public void testScanRecordTypeFilter() throws Exception {
        IdGenerator idGenerator = new IdGeneratorImpl();

        QName recordType = new QName("namespace1", "name1");

        RecordScan scan = new RecordScan();
        scan.setRecordFilter(new RecordTypeFilter(recordType));

        byte[] data = writer.toJsonBytes(scan, repository);
        RecordScan parsedScan = reader.fromJsonBytes(data, repository);

        assertNotNull(parsedScan.getRecordFilter());
        assertTrue(parsedScan.getRecordFilter() instanceof RecordTypeFilter);
        assertEquals(recordType, ((RecordTypeFilter)parsedScan.getRecordFilter()).getRecordType());
        assertNull(((RecordTypeFilter)parsedScan.getRecordFilter()).getVersion());

        // Check json
        JsonNode node = new ObjectMapper().readTree(data);
        assertEquals("org.lilyproject.repository.api.filter.RecordTypeFilter",
                node.get("recordFilter").get("@class").getTextValue());
        assertEquals("{namespace1}name1",
                node.get("recordFilter").get("recordType").getTextValue());
    }

    @Test
    public void testScanRecordIdPrefixFilter() throws Exception {
        IdGenerator idGenerator = new IdGeneratorImpl();

        RecordId recordId = idGenerator.newRecordId("foo");

        RecordScan scan = new RecordScan();
        scan.setRecordFilter(new RecordIdPrefixFilter(recordId));

        byte[] data = writer.toJsonBytes(scan, repository);
        RecordScan parsedScan = reader.fromJsonBytes(data, repository);

        assertNotNull(parsedScan.getRecordFilter());
        assertTrue(parsedScan.getRecordFilter() instanceof RecordIdPrefixFilter);
        assertEquals(recordId, ((RecordIdPrefixFilter)parsedScan.getRecordFilter()).getRecordId());

        // Check json
        JsonNode node = new ObjectMapper().readTree(data);
        assertEquals("org.lilyproject.repository.api.filter.RecordIdPrefixFilter",
                node.get("recordFilter").get("@class").getTextValue());
        assertEquals("USER.foo",
                node.get("recordFilter").get("recordId").getTextValue());
    }

    @Test
    public void testScanFieldValueFilter() throws Exception {
        IdGenerator idGenerator = new IdGeneratorImpl();

        QName name = new QName("ns", "f1");
        Object value = "foo";

        RecordScan scan = new RecordScan();
        scan.setRecordFilter(new FieldValueFilter(name, value));

        byte[] data = writer.toJsonBytes(scan, repository);
        RecordScan parsedScan = reader.fromJsonBytes(data, repository);

        assertNotNull(parsedScan.getRecordFilter());
        assertTrue(parsedScan.getRecordFilter() instanceof FieldValueFilter);
        FieldValueFilter filter = (FieldValueFilter)parsedScan.getRecordFilter();
        assertEquals(name, filter.getField());
        assertEquals(value, filter.getFieldValue());
        assertTrue(filter.getFilterIfMissing());

        // Check json structure
        JsonNode node = new ObjectMapper().readTree(data);
        assertEquals("org.lilyproject.repository.api.filter.FieldValueFilter",
                node.get("recordFilter").get("@class").getTextValue());
        assertEquals("STRING",
                node.get("recordFilter").get("fieldValue").get("valueType").getTextValue());
        assertEquals("foo",
                node.get("recordFilter").get("fieldValue").get("value").getTextValue());

        // Try different data types as field value
        value = new Long(3);
        scan.setRecordFilter(new FieldValueFilter(name, value));
        assertEquals(value, ((FieldValueFilter)reader.fromJsonBytes(writer.toJsonBytes(scan, repository), repository)
                .getRecordFilter()).getFieldValue());

        value = new BigDecimal(3);
        scan.setRecordFilter(new FieldValueFilter(name, value));
        assertEquals(value, ((FieldValueFilter)reader.fromJsonBytes(writer.toJsonBytes(scan, repository), repository)
                .getRecordFilter()).getFieldValue());

        value = new LocalDate();
        scan.setRecordFilter(new FieldValueFilter(name, value));
        assertEquals(value, ((FieldValueFilter)reader.fromJsonBytes(writer.toJsonBytes(scan, repository), repository)
                .getRecordFilter()).getFieldValue());

        value = Lists.newArrayList("foo", "bar");;
        scan.setRecordFilter(new FieldValueFilter(name, value));
        assertEquals(value, ((FieldValueFilter)reader.fromJsonBytes(writer.toJsonBytes(scan, repository), repository)
                .getRecordFilter()).getFieldValue());

        // Use a list as field value, but with a mixture of datatypes. This should fail,
        // as lists in Lily should contain values of the same type.
        value = Lists.newArrayList("foo", new Long(123));
        scan.setRecordFilter(new FieldValueFilter(name, value));
        try {
            data = writer.toJsonBytes(scan, repository);
            fail("Expected exception with list containing different data types");
        } catch (Exception e) {
            // expected
        }
    }

    @Test
    public void testScanRecordFilterList() throws Exception {
        IdGenerator idGenerator = new IdGeneratorImpl();

        RecordId recordId = idGenerator.newRecordId("foo");

        RecordScan scan = new RecordScan();
        RecordFilterList filterList = new RecordFilterList(RecordFilterList.Operator.MUST_PASS_ONE);
        filterList.addFilter(new RecordIdPrefixFilter(recordId));
        filterList.addFilter(new RecordTypeFilter(new QName("ns", "f")));
        scan.setRecordFilter(filterList);

        byte[] data = writer.toJsonBytes(scan, repository);
        System.out.println(Bytes.toString(data));
        RecordScan parsedScan = reader.fromJsonBytes(data, repository);

        assertNotNull(parsedScan.getRecordFilter());
        assertTrue(parsedScan.getRecordFilter() instanceof RecordFilterList);
        RecordFilterList parsedFilterList = (RecordFilterList)filterList;
        assertTrue(parsedFilterList.getFilters().get(0) instanceof RecordIdPrefixFilter);
        assertTrue(parsedFilterList.getFilters().get(1) instanceof RecordTypeFilter);
        assertEquals(RecordFilterList.Operator.MUST_PASS_ONE, parsedFilterList.getOperator());

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
        IdGenerator idGenerator = new IdGeneratorImpl();

        QName recordType = new QName("namespace1", "name1");

        RecordScan scan = new RecordScan();
        scan.setReturnFields(ReturnFields.NONE);

        byte[] data = writer.toJsonBytes(scan, repository);
        RecordScan parsedScan = reader.fromJsonBytes(data, repository);

        assertEquals(ReturnFields.NONE.getType(), parsedScan.getReturnFields().getType());

        // Test with enumeration of fields to return
        scan.setReturnFields(new ReturnFields(new QName("ns", "f1"), new QName("ns", "f2")));
        data = writer.toJsonBytes(scan, repository);
        System.out.println(Bytes.toString(data));
        parsedScan = reader.fromJsonBytes(data, repository);

        assertEquals(ReturnFields.Type.ENUM, parsedScan.getReturnFields().getType());
        assertEquals(Lists.newArrayList(new QName("ns", "f1"), new QName("ns", "f2")),
                parsedScan.getReturnFields().getFields());
    }
}
