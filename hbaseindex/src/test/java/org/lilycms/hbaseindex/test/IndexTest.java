package org.lilycms.hbaseindex.test;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.lilycms.hbaseindex.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;

public class IndexTest {
    private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
      TEST_UTIL.startMiniCluster(1);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
      TEST_UTIL.shutdownMiniCluster();
    }

    @Test
    public void testSingleStringFieldIndex() throws Exception {
        final String INDEX_NAME = "singlestringfield";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME);
        indexDef.addStringField("field1");

        indexManager.createIndex(indexDef);

        Index index = indexManager.getIndex(INDEX_NAME);

        // Create a few index entries, inserting them in non-sorted order
        String[] values = {"d", "a", "c", "e", "b"};

        for (int i = 0; i < values.length; i++) {
            IndexEntry entry = new IndexEntry();
            entry.addField("field1", values[i]);
            index.addEntry(entry, Bytes.toBytes("targetkey" + i));            
        }

        Query query = new Query();
        query.setRangeCondition("field1", "b", "d");
        QueryResult result = index.performQuery(query);

        assertEquals("targetkey4", Bytes.toString(result.next()));
        assertEquals("targetkey2", Bytes.toString(result.next()));
        assertEquals("targetkey0", Bytes.toString(result.next()));
        assertNull(result.next());
    }

    @Test
    public void testSingleIntFieldIndex() throws Exception {
        final String INDEX_NAME = "singleintfield";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME);
        indexDef.addIntegerField("field1");

        indexManager.createIndex(indexDef);

        Index index = indexManager.getIndex(INDEX_NAME);

        final int COUNT = 1000;
        final int MAXVALUE = Integer.MAX_VALUE;
        int[] values = new int[COUNT];

        for (int i = 0; i < COUNT; i++) {
            values[i] = (int)(Math.random() * MAXVALUE);
        }

        for (int value : values) {
            IndexEntry entry = new IndexEntry();
            entry.addField("field1", value);
            index.addEntry(entry, Bytes.toBytes("targetkey" + value));
        }

        Query query = new Query();
        query.setRangeCondition("field1", new Integer(0), new Integer(MAXVALUE));
        QueryResult result = index.performQuery(query);

        Arrays.sort(values);

        for (int value : values) {
            assertEquals("targetkey" + value, Bytes.toString(result.next()));
        }

        assertNull(result.next());
    }

    @Test
    public void testSingleFloatFieldIndex() throws Exception {
        final String INDEX_NAME = "singlefloatfield";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME);
        indexDef.addFloatField("field1");

        indexManager.createIndex(indexDef);

        Index index = indexManager.getIndex(INDEX_NAME);

        float[] values = {55.45f, 63.88f, 55.46f, 55.47f, -0.3f};

        for (int i = 0; i < values.length; i++) {
            IndexEntry entry = new IndexEntry();
            entry.addField("field1", values[i]);
            index.addEntry(entry, Bytes.toBytes("targetkey" + i));
        }

        Query query = new Query();
        query.setRangeCondition("field1", new Float(55.44f), new Float(55.48f));
        QueryResult result = index.performQuery(query);

        assertEquals("targetkey0", Bytes.toString(result.next()));
        assertEquals("targetkey2", Bytes.toString(result.next()));
        assertEquals("targetkey3", Bytes.toString(result.next()));
        assertNull(result.next());
    }

    @Test
    public void testSingleDateTimeFieldIndex() throws Exception {
        final String INDEX_NAME = "singledatetimefield";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME);
        DateTimeIndexFieldDefinition fieldDef = indexDef.addDateTimeField("field1");
        fieldDef.setPrecision(DateTimeIndexFieldDefinition.Precision.DATETIME_NOMILLIS);
        indexManager.createIndex(indexDef);

        Index index = indexManager.getIndex(INDEX_NAME);

        DateTimeFormatter formatter = ISODateTimeFormat.basicDateTimeNoMillis();
        Date[] values = {
                formatter.parseDateTime("20100215T140500Z").toDate(),
                formatter.parseDateTime("20100215T140501Z").toDate(),
                formatter.parseDateTime("20100216T100000Z").toDate(),
                formatter.parseDateTime("20100217T100000Z").toDate()
        };

        for (int i = 0; i < values.length; i++) {
            IndexEntry entry = new IndexEntry();
            entry.addField("field1", values[i]);
            index.addEntry(entry, Bytes.toBytes("targetkey" + i));
        }

        Query query = new Query();
        query.setRangeCondition("field1", formatter.parseDateTime("20100215T140500Z").toDate(),
                formatter.parseDateTime("20100215T140501Z").toDate());
        QueryResult result = index.performQuery(query);

        assertEquals("targetkey0", Bytes.toString(result.next()));
        assertEquals("targetkey1", Bytes.toString(result.next()));
        assertNull(result.next());
    }

    @Test
    public void testDuplicateValuesIndex() throws Exception {
        final String INDEX_NAME = "duplicatevalues";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME);
        indexDef.addStringField("field1");

        indexManager.createIndex(indexDef);

        Index index = indexManager.getIndex(INDEX_NAME);

        // Create a few index entries, inserting them in non-sorted order
        String[] values = {"a", "a", "a", "a", "b", "c", "d"};

        for (int i = 0; i < values.length; i++) {
            IndexEntry entry = new IndexEntry();
            entry.addField("field1", values[i]);
            index.addEntry(entry, Bytes.toBytes("targetkey" + i));
        }

        Query query = new Query();
        query.addEqualsCondition("field1", "a");
        QueryResult result = index.performQuery(query);

        assertResultSize(4, result);
    }

    private void assertResultSize(int expectedCount, QueryResult result) throws IOException {
        int matchCount = 0;
        while (result.next() != null) {
            matchCount++;
        }
        assertEquals(expectedCount, matchCount);
    }

    @Test
    public void testMultiFieldIndex() throws Exception {
        final String INDEX_NAME = "multifield";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME);
        indexDef.addIntegerField("field1");
        indexDef.addStringField("field2");

        indexManager.createIndex(indexDef);

        Index index = indexManager.getIndex(INDEX_NAME);

        IndexEntry entry = new IndexEntry();
        entry.addField("field1", 10);
        entry.addField("field2", "a");
        index.addEntry(entry, Bytes.toBytes("targetkey1"));
        index.addEntry(entry, Bytes.toBytes("targetkey2"));
        index.addEntry(entry, Bytes.toBytes("targetkey3"));

        entry = new IndexEntry();
        entry.addField("field1", 11);
        entry.addField("field2", "a");
        index.addEntry(entry, Bytes.toBytes("targetkey4"));

        entry = new IndexEntry();
        entry.addField("field1", 10);
        entry.addField("field2", "b");
        index.addEntry(entry, Bytes.toBytes("targetkey5"));

        Query query = new Query();
        query.addEqualsCondition("field1", 10);
        query.addEqualsCondition("field2", "a");
        QueryResult result = index.performQuery(query);

        assertResultSize(3, result);
    }
}
