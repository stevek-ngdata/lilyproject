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
package org.lilyproject.repository.impl.test;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.net.URI;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;

import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.bytes.api.ByteArray;
import org.lilyproject.bytes.api.DataInput;
import org.lilyproject.bytes.api.DataOutput;
import org.lilyproject.bytes.impl.DataInputImpl;
import org.lilyproject.bytes.impl.DataOutputImpl;
import org.lilyproject.hadooptestfw.TestHelper;
import org.lilyproject.repository.api.Blob;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.HierarchyPath;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.IdentityRecordStack;
import org.lilyproject.repository.api.Link;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordException;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.RecordTypeBuilder;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TypeException;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.api.UnknownValueTypeEncodingException;
import org.lilyproject.repository.api.ValueType;
import org.lilyproject.repository.api.ValueTypeFactory;
import org.lilyproject.repository.impl.valuetype.AbstractValueType;
import org.lilyproject.repotestfw.RepositorySetup;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ValueTypeTest {

    private static final RepositorySetup repoSetup = new RepositorySetup();

    private static TypeManager typeManager;
    private static Repository repository;
    private static IdGenerator idGenerator;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging();
        // TODO this test relies on all blobs being inline blobs, since it reuses blob values
        repoSetup.setBlobLimits(Long.MAX_VALUE, -1);
        repoSetup.setupCore();
        repoSetup.setupRepository();

        typeManager = repoSetup.getTypeManager();
        repository = (Repository)repoSetup.getRepositoryManager().getDefaultRepository().getDefaultTable();
        idGenerator = repoSetup.getIdGenerator();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        repoSetup.stop();
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testFooType() throws Exception {
        try {
            runValueTypeTests("fooRecordTypeId", "FOO", "foo", "bar", "pub");
            fail("Excpeting TypeException");
        } catch (TypeException expected) {
        }
    }

    @Test
    public void testStringType() throws Exception {
        runValueTypeTests("stringRecordTypeId", "STRING", "foo", "bar", "pub");
    }

    @Test
    public void testIntegerType() throws Exception {
        runValueTypeTests("integerRecordTypeId", "INTEGER", Integer.MIN_VALUE, 0, Integer.MAX_VALUE);
    }

    @Test
    public void testLongType() throws Exception {
        runValueTypeTests("longRecordTypeId", "LONG", Long.MIN_VALUE, Long.valueOf(0), Long.MAX_VALUE);
    }

    @Test
    public void testDoubleType() throws Exception {
        runValueTypeTests("doubleRecordTypeId", "DOUBLE", Double.MIN_VALUE, Double.valueOf(0), Double.MAX_VALUE);
    }

    @Test
    public void testDecimalType() throws Exception {
        runValueTypeTests("decimalRecordTypeId", "DECIMAL", BigDecimal.valueOf(Double.MIN_EXPONENT), BigDecimal.ZERO, BigDecimal.valueOf(Long.MAX_VALUE));
    }

    @Test
    public void testBooleanType() throws Exception {
        runValueTypeTests("booleanRecordTypeId", "BOOLEAN", true, false, true);
    }

    @Test
    public void testDateTimeType() throws Exception {
        runValueTypeTests("dateTimeRecordTypeId", "DATETIME", new DateTime(DateTimeZone.UTC), new DateTime(Long.MAX_VALUE, DateTimeZone.UTC), new DateTime(Long.MIN_VALUE, DateTimeZone.UTC));
    }

    @Test
    public void testDateType() throws Exception {
        runValueTypeTests("dateRecordTypeId", "DATE", new LocalDate(DateTimeZone.UTC), new LocalDate(2900, 10, 14), new LocalDate(1300, 5, 4));
    }

    @Test
    public void testUriType() throws Exception {
        runValueTypeTests("uriRecordTypeId", "URI", URI.create("http://foo.com/bar"), URI.create("file://foo/com/bar.txt"), URI.create("https://site/index.html"));
    }

    @Test
    public void testByteArrayType() throws Exception {
        runValueTypeTests("byteArrayRecordTypeId", "BYTEARRAY", new ByteArray(Bytes.toBytes("bytes1")), new ByteArray(
                Bytes.toBytes("bytes2")), new ByteArray(Bytes.toBytes("bbb")));
    }

    @Test
    public void testBlobType() throws Exception {
        Blob blob1 = new Blob("text/html", Long.MAX_VALUE, null);
        writeBlob(blob1, Bytes.toBytes("aKey"));
        Blob blob2 = new Blob("image/jpeg", Long.MIN_VALUE, "images/image.jpg");
        writeBlob(blob2, Bytes.toBytes("anotherKey"));
        Blob blob3 = new Blob("text/plain", Long.valueOf(0), null);
        writeBlob(blob3, Bytes.toBytes("thirdKey"));

        runValueTypeTests("blobTypeId", "BLOB", blob1, blob2, blob3);
    }

    private void writeBlob(Blob blob, byte[] bytes) throws RepositoryException, InterruptedException, IOException {
        OutputStream bos = repository.getOutputStream(blob);
        bos.write(bytes);
        bos.close();
    }

    @Test
    public void testRecordType() throws Exception {
        FieldType fieldType1 = typeManager.createFieldType(typeManager.newFieldType(typeManager.getValueType("STRING"), new QName("testRecordType", "field1"), Scope.NON_VERSIONED));
        FieldType fieldType2 = typeManager.createFieldType(typeManager.newFieldType(typeManager.getValueType("INTEGER"), new QName("testRecordType", "field2"), Scope.NON_VERSIONED));
        RecordTypeBuilder rtBuilder = typeManager.recordTypeBuilder();
        RecordType valueTypeRT = rtBuilder.name(new QName("testRecordType", "recordValueTypeRecordType"))
            .field(fieldType1.getId(), false)
            .field(fieldType2.getId(), true)
            .create();

        Record recordField1 = repository.recordBuilder()
            .recordType(valueTypeRT.getName(), 1L)
            .field(fieldType1.getName(), "abc")
            .field(fieldType2.getName(), 123)
            .build();
        Record recordField2 = repository.recordBuilder()
            .recordType(valueTypeRT.getName(), 1L)
            .field(fieldType1.getName(), "def")
            .field(fieldType2.getName(), 456)
            .build();
        Record recordField3 = repository.recordBuilder()
            .recordType(valueTypeRT.getName(), 1L)
            .field(fieldType1.getName(), "xyz")
            .field(fieldType2.getName(), 888)
            .build();

        testType("recordValueTypeId", "RECORD<{testRecordType}recordValueTypeRecordType>", recordField1);
        testType("recordValueTypeId", "LIST<RECORD<{testRecordType}recordValueTypeRecordType>>", Arrays.asList(recordField1, recordField2));
        testType("recordValueTypeId", "PATH<RECORD<{testRecordType}recordValueTypeRecordType>>", new HierarchyPath(recordField1, recordField2));
        testType("recordValueTypeId", "LIST<PATH<RECORD<{testRecordType}recordValueTypeRecordType>>>", Arrays.asList(new HierarchyPath(recordField1, recordField2), new HierarchyPath(recordField1, recordField3)));
    }

    @Test
    public void testLinkType() throws Exception {
        FieldType fieldType1 = typeManager.createFieldType(typeManager.newFieldType(typeManager.getValueType("STRING"), new QName("testLinkType", "field1"), Scope.NON_VERSIONED));
        RecordTypeBuilder rtBuilder = typeManager.recordTypeBuilder();
        RecordType valueTypeRT = rtBuilder.name(new QName("testLinkType", "linkValueTypeRecordType"))
            .field(fieldType1.getId(), false)
            .create();

        testType("recordValueTypeId", "LINK", new Link(idGenerator.newRecordId()));
        testType("recordValueTypeId", "LINK<{testLinkType}linkValueTypeRecordType>", new Link(idGenerator.newRecordId()));
        testType("recordValueTypeId", "LIST<LINK<{testLinkType}linkValueTypeRecordType>>", Arrays.asList(new Link(idGenerator.newRecordId()), new Link(idGenerator.newRecordId())));
        testType("recordValueTypeId", "LIST<LINK>", Arrays.asList(new Link(idGenerator.newRecordId()), new Link(idGenerator.newRecordId())));
        testType("recordValueTypeId", "PATH<LINK<{testLinkType}linkValueTypeRecordType>>", new HierarchyPath(new Link(idGenerator.newRecordId()), new Link(idGenerator.newRecordId())));
        testType("recordValueTypeId", "PATH<LINK>", new HierarchyPath(new Link(idGenerator.newRecordId()), new Link(idGenerator.newRecordId())));
        testType("recordValueTypeId", "LIST<PATH<LINK<{testLinkType}linkValueTypeRecordType>>>", Arrays.asList(new HierarchyPath(new Link(idGenerator.newRecordId()), new Link(idGenerator.newRecordId())), new HierarchyPath(new Link(idGenerator.newRecordId()), new Link(idGenerator.newRecordId()))));
        testType("recordValueTypeId", "LIST<PATH<LINK>>", Arrays.asList(new HierarchyPath(new Link(idGenerator.newRecordId()), new Link(idGenerator.newRecordId())), new HierarchyPath(new Link(idGenerator.newRecordId()), new Link(idGenerator.newRecordId()))));
    }


    @Test
    public void testNewValueType() throws Exception {
        typeManager.registerValueType(XYValueType.NAME, factory());
        runValueTypeTests("xyRecordTypeId", "XY", new XYCoordinates(-1, 1), new XYCoordinates(Integer.MIN_VALUE, Integer.MAX_VALUE), new XYCoordinates(666, 777));
    }

    @Test
    public void testComparators() throws Exception {
        Comparator comparator = typeManager.getValueType("LONG").getComparator();
        assertTrue(comparator.compare(new Long(2), new Long(5)) < 0);
        assertTrue(comparator.compare(new Long(5), new Long(5)) == 0);

        comparator = typeManager.getValueType("DOUBLE").getComparator();
        assertTrue(comparator.compare(new Double(2.2d), new Double(5.5d)) < 0);

        comparator = typeManager.getValueType("BLOB").getComparator();
        assertNull(comparator);

        comparator = typeManager.getValueType("DATE").getComparator();
        assertTrue(comparator.compare(new LocalDate(2012, 5, 5), new LocalDate(2013, 5, 5)) < 0);
        assertTrue(comparator.compare(new LocalDate(2012, 5, 5), new LocalDate(2012, 5, 5)) == 0);

        comparator = typeManager.getValueType("URI").getComparator();
        assertNull(comparator);

        comparator = typeManager.getValueType("DECIMAL").getComparator();
        assertTrue(comparator.compare(new BigDecimal("2"), new BigDecimal("5")) < 0);
        assertTrue(comparator.compare(new BigDecimal("5"), new BigDecimal("5")) == 0);

        comparator = typeManager.getValueType("BOOLEAN").getComparator();
        assertTrue(comparator.compare(Boolean.FALSE, Boolean.TRUE) < 0);
        assertTrue(comparator.compare(Boolean.TRUE, Boolean.TRUE) == 0);

        comparator = typeManager.getValueType("DATETIME").getComparator();
        assertTrue(comparator.compare(new DateTime(2012, 5, 5, 6, 7, 8, 9), new DateTime(2012, 5, 5, 6, 7, 8, 10)) < 0);
        assertTrue(comparator.compare(new DateTime(2012, 5, 5, 6, 7, 8, 9), new DateTime(2012, 5, 5, 6, 7, 8, 9)) == 0);

        comparator = typeManager.getValueType("STRING").getComparator();
        assertTrue(comparator.compare("a", "b") < 0);
        assertTrue(comparator.compare("a", "a") == 0);

        comparator = typeManager.getValueType("INTEGER").getComparator();
        assertTrue(comparator.compare(new Integer(2), new Integer(5)) < 0);
        assertTrue(comparator.compare(new Integer(5), new Integer(5)) == 0);

        comparator = typeManager.getValueType("LINK").getComparator();
        assertNull(comparator);

        comparator = typeManager.getValueType("LIST<STRING>").getComparator();
        assertNull(comparator);

        comparator = typeManager.getValueType("PATH<STRING>").getComparator();
        assertNull(comparator);

        comparator = typeManager.getValueType("RECORD<{testRecordType}recordValueTypeRecordType>").getComparator();
        assertNull(comparator);
    }

    private void runValueTypeTests(String name, String valueType, Object value1, Object value2, Object value3) throws Exception {
        testType(name, valueType, value1);
        testType(name, "LIST<"+valueType+">", Arrays.asList(value1, value2));
        testType(name, "PATH<"+valueType+">", new HierarchyPath(value1, value2));
        testType(name, "LIST<PATH<"+valueType+">>", Arrays.asList(new HierarchyPath(value1, value2), new HierarchyPath(value1, value3)));
    }

    private void testType(String name, String valueType,
                    Object fieldValue) throws Exception {
        String testName = name+valueType;
        QName fieldTypeName = new QName("valueTypeTest", testName+"FieldId");
        FieldType fieldType = typeManager.createFieldType(typeManager.newFieldType(typeManager.getValueType(
                        valueType), fieldTypeName, Scope.VERSIONED));
        RecordType recordType = typeManager.newRecordType(new QName("valueTypeTest", testName+"RecordTypeId"));
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType.getId(), true));
        recordType = typeManager.createRecordType(recordType);

        Record record = repository.newRecord(idGenerator.newRecordId());
        record.setRecordType(recordType.getName(), recordType.getVersion());
        record.setField(fieldType.getName(), fieldValue);
        repository.create(record);

        Record actualRecord = repository.read(record.getId());
        assertEquals(fieldValue, actualRecord.getField(fieldType.getName()));
    }

    public class XYValueType extends AbstractValueType implements ValueType {
        private Random random = new Random();

        public static final String NAME = "XY";

        @Override
        public String getBaseName() {
            return NAME;
        }

        @Override
        public ValueType getDeepestValueType() {
            return this;
        }

        @Override
        public Object read(DataInput dataInput) throws UnknownValueTypeEncodingException {
            byte encodingVersion = dataInput.readByte();
            int x;
            int y;
            if ((byte)1 == encodingVersion) {
                x = dataInput.readInt();
                y = dataInput.readInt();
            } else if ((byte)2 == encodingVersion) {
                y = dataInput.readInt();
                x = dataInput.readInt();
            } else {
                throw new UnknownValueTypeEncodingException(NAME, encodingVersion);
            }
            return new XYCoordinates(x, y);
        }

        @Override
        public void write(Object value, DataOutput dataOutput, IdentityRecordStack parentRecords) {
            if (random.nextBoolean()) {
                dataOutput.writeByte((byte)1); // encoding version 1
                dataOutput.writeInt(((XYCoordinates) value).getX());
                dataOutput.writeInt(((XYCoordinates) value).getY());
            } else {
                dataOutput.writeByte((byte)2); // encoding version 2
                dataOutput.writeInt(((XYCoordinates) value).getY());
                dataOutput.writeInt(((XYCoordinates) value).getX());

            }
        }

        @Override
        public Class getType() {
            return XYCoordinates.class;
        }

        @Override
        public Comparator getComparator() {
            return null;
        }
    }

    //
    // Factory
    //
    public ValueTypeFactory factory() {
        return new XYValueTypeFactory();
    }

    public class XYValueTypeFactory implements ValueTypeFactory {
        @Override
        public ValueType getValueType(String typeParams) {
            return new XYValueType();
        }
    }

    private class XYCoordinates {
        private final int x;
        private final int y;

        XYCoordinates(int x, int y) {
            this.x = x;
            this.y = y;
        }

        public int getX() {
            return x;
        }

        public int getY() {
            return y;
        }

        @Override
        public String toString() {
            return "["+x+","+y+"]";
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + getOuterType().hashCode();
            result = prime * result + x;
            result = prime * result + y;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            XYCoordinates other = (XYCoordinates) obj;
            if (!getOuterType().equals(other.getOuterType())) {
                return false;
            }
            if (x != other.x) {
                return false;
            }
            if (y != other.y) {
                return false;
            }
            return true;
        }

        private ValueTypeTest getOuterType() {
            return ValueTypeTest.this;
        }
    }

    @Test
    public void testRecordVTTypeInVT() throws Exception {
        String ns = "testRecordVTTypeInVT";
        // Create a fieldType to be used as a field in a record type to be used as the type for a RecordValueType
        FieldType fieldType1 = typeManager.createFieldType(typeManager.newFieldType(typeManager.getValueType("STRING"), new QName(ns, "field1"), Scope.NON_VERSIONED));
        // Create a record type to be used as the type for a RecordValueType
        RecordType rt1 = typeManager.recordTypeBuilder()
            .name(new QName(ns, "rt1"))
            .field(fieldType1.getId(), false)
            .create();

        // Make a RecordValueType with the record type specified
        ValueType recordVT1 = typeManager.getValueType("RECORD<{"+ns+"}rt1>");

        // Create a fieldType with as value type a RecordValueType
        FieldType fieldType2 = typeManager.createFieldType(typeManager.newFieldType(recordVT1, new QName(ns, "field2"), Scope.NON_VERSIONED));
        // Create a recordType with a field of this field type
        RecordType rt2 = typeManager.recordTypeBuilder()
            .name(new QName(ns, "rt2"))
            .field(fieldType2.getId(), false)
            .create();

        // Make a record to be used as field value
        Record recordField = repository.recordBuilder().field(new QName(ns, "field1"), "abc").build();
        // Create a record with a record as field
        Record createdRecord = repository.recordBuilder().recordType(new QName(ns, "rt2")).field(new QName(ns,"field2"), recordField).create();

        // Read the record and check the field of the record-field
        Record readRecord = repository.read(createdRecord.getId());
        Record readRecordField = (Record)readRecord.getField(new QName(ns, "field2"));
        assertEquals("abc", recordField.getField(new QName(ns, "field1")));

        assertNull(readRecordField.getId()); // The record field should have no Id
        assertNull(readRecordField.getVersion()); // The record field should have no version
    }

    @Test
    public void testRecordVTNested() throws Exception {
        String ns = "testRecordVTNested";
        // Create a fieldType to be used as a field in a record type to be used as the type for a RecordValueType
        FieldType fieldType1 = typeManager.createFieldType(typeManager.newFieldType(typeManager.getValueType("STRING"), new QName(ns, "field1"), Scope.NON_VERSIONED));
        // Create a record type to be used as the type for a RecordValueType
        RecordType rt1 = typeManager.recordTypeBuilder()
            .name(new QName(ns, "rt1"))
            .field(fieldType1.getId(), false)
            .create();

        // Make a RecordValueType with the record type specified
        ValueType recordVT1 = typeManager.getValueType("RECORD<{"+ns+"}rt1>");

        // Create a fieldType with as value type a RecordValueType
        FieldType fieldType2 = typeManager.createFieldType(typeManager.newFieldType(recordVT1, new QName(ns, "field2"), Scope.NON_VERSIONED));
        // Create a recordType with a field of this field type
        RecordType rt2 = typeManager.recordTypeBuilder()
            .name(new QName(ns, "rt2"))
            .field(fieldType2.getId(), false)
            .create();

        // Create a fieldType with as value type a 'nested' RecordValueType
        ValueType recordVT2 = typeManager.getValueType("RECORD<{"+ns+"}rt2>");
        FieldType fieldType3 = typeManager.createFieldType(typeManager.newFieldType(recordVT2, new QName(ns, "field3"), Scope.NON_VERSIONED));
        RecordType rt3 = typeManager.recordTypeBuilder()
            .name(new QName(ns, "rt3"))
            .field(fieldType3.getId(), false)
            .create();

        // Make nested records
        Record recordField1 = repository.recordBuilder().field(new QName(ns, "field1"), "abc").build();

        Record recordField2 = repository.recordBuilder().field(new QName(ns, "field2"), recordField1).build();

        // Create a record with nested records
        Record createdRecord = repository.recordBuilder().recordType(new QName(ns, "rt3")).field(new QName(ns,"field3"), recordField2).create();

        // Read the record and check the field of the record-field
        Record readRecord = repository.read(createdRecord.getId());
        Record nestedRecord1 = readRecord.getField(new QName(ns, "field3"));
        Record nestedRecord2 = nestedRecord1.getField(new QName(ns, "field2"));
        assertEquals("abc", nestedRecord2.getField(new QName(ns, "field1")));
    }

    @Test
    public void testRecordVTTypeInRecord() throws Exception {
        String ns = "testRecordVTTypeInRecord";
        // Create a fieldType to be used as a field in a record type to be used as the type for a RecordValueType
        FieldType fieldType1 = typeManager.createFieldType(typeManager.newFieldType(typeManager.getValueType("STRING"), new QName(ns, "field1"), Scope.NON_VERSIONED));
        // Create a record type to be used as the type for a RecordValueType
        RecordType rt1 = typeManager.recordTypeBuilder().name(new QName(ns, "rt1")).field(fieldType1.getId(), false).create();

        // Create record types to be used as versioned and versioned-mutable record types, which should be ignored
        RecordType vrt = typeManager.recordTypeBuilder().name(new QName(ns, "vrt")).field(fieldType1.getId(), false).create();
        RecordType vmrt = typeManager.recordTypeBuilder().name(new QName(ns, "vmrt")).field(fieldType1.getId(), false).create();

        // Make a RecordValueType without the record type specified
        ValueType recordVT1 = typeManager.getValueType("RECORD");

        // Create a fieldType with as value type a RecordValueType
        FieldType fieldType2 = typeManager.createFieldType(typeManager.newFieldType(recordVT1, new QName(ns, "field2"), Scope.NON_VERSIONED));
        // Create a recordType with a field of this field type
        RecordType rt2 = typeManager.recordTypeBuilder()
            .name(new QName(ns, "rt2"))
            .field(fieldType2.getId(), false)
            .create();

        // Make a record to be used as field value, specify the record type here
        Record recordField = repository.recordBuilder().recordType(new QName(ns, "rt1")).field(new QName(ns, "field1"), "abc").build();
        recordField.setRecordType(Scope.VERSIONED, new QName(ns, "vrt"), null); // This record type should be ignored
        recordField.setRecordType(Scope.VERSIONED_MUTABLE, new QName(ns, "vmrt"), null); // This record type should be ignored

        // Create a record with a record as field
        Record createdRecord = repository.recordBuilder().recordType(new QName(ns, "rt2")).field(new QName(ns,"field2"), recordField).create();

        // Read the record and check the field of the record-field
        Record readRecord = repository.read(createdRecord.getId());
        Record readRecordField = (Record)readRecord.getField(new QName(ns, "field2"));
        assertEquals("abc", readRecordField.getField(new QName(ns, "field1")));
        assertEquals(new QName(ns, "rt1"), readRecordField.getRecordTypeName(Scope.NON_VERSIONED));
        assertNull(readRecordField.getRecordTypeName(Scope.VERSIONED));
        assertNull(readRecordField.getRecordTypeName(Scope.VERSIONED_MUTABLE));
    }

    @Test
    public void testRecordVTNoTypeDefined() throws Exception {
        String ns = "testRecordVTNoTypeDefined";
        // Create a fieldType to be used as a field in a record type to be used as the type for a RecordValueType
        FieldType fieldType1 = typeManager.createFieldType(typeManager.newFieldType(typeManager.getValueType("STRING"), new QName(ns, "field1"), Scope.NON_VERSIONED));
        // Create a record type to be used as the type for a RecordValueType
        RecordType rt1 = typeManager.recordTypeBuilder()
            .name(new QName(ns, "rt1"))
            .field(fieldType1.getId(), false)
            .create();

        // Make a RecordValueType without the record type specified
        ValueType recordVT1 = typeManager.getValueType("RECORD");

        // Create a fieldType with as value type a RecordValueType
        FieldType fieldType2 = typeManager.createFieldType(typeManager.newFieldType(recordVT1, new QName(ns, "field2"), Scope.NON_VERSIONED));

        // Create a recordType with a field of this field type
        RecordType rt2 = typeManager.recordTypeBuilder()
            .name(new QName(ns, "rt2"))
            .field(fieldType2.getId(), false)
            .create();

        // Make a record to be used as field value, don't specify the record type here either
        Record recordField = repository.recordBuilder().field(new QName(ns, "field1"), "abc").build();

        // Create a record with a record as field
        try {
            Record createdRecord = repository.recordBuilder().recordType(new QName(ns, "rt2")).field(new QName(ns,"field2"), recordField).create();
            Assert.fail();
        } catch (RecordException expected) {
        }
    }

    @Test
    public void testRecordVTUndefinedField() throws Exception {
        String ns = "testRecordVTUndefinedField";
        // Create a fieldType to be used as a field in a record type to be used as the type for a RecordValueType
        FieldType fieldType1 = typeManager.createFieldType(typeManager.newFieldType(typeManager.getValueType("STRING"), new QName(ns, "field1"), Scope.NON_VERSIONED));
        FieldType fieldType1b = typeManager.createFieldType(typeManager.newFieldType(typeManager.getValueType("STRING"), new QName(ns, "field1b"), Scope.NON_VERSIONED));
        // Create a record type to be used as the type for a RecordValueType
        RecordType rt1 = typeManager.recordTypeBuilder()
            .name(new QName(ns, "rt1"))
            .field(fieldType1.getId(), false)
            .field(fieldType1b.getId(), false)
            .create();

        // Make a RecordValueType without the record type specified
        ValueType recordVT1 = typeManager.getValueType("RECORD");

        // Create a fieldType with as value type a RecordValueType
        FieldType fieldType2 = typeManager.createFieldType(typeManager.newFieldType(recordVT1, new QName(ns, "field2"), Scope.NON_VERSIONED));
        // Create a recordType with a field of this field type
        RecordType rt2 = typeManager.recordTypeBuilder()
            .name(new QName(ns, "rt2"))
            .field(fieldType2.getId(), false)
            .create();

        // Make a record to be used as field value, specify the record type here
        // Only fill in field1, not field1b
        Record recordField = repository.recordBuilder().recordType(new QName(ns, "rt1")).field(new QName(ns, "field1"), "abc").build();

        // Create a record with a record as field
        Record createdRecord = repository.recordBuilder().recordType(new QName(ns, "rt2")).field(new QName(ns,"field2"), recordField).create();

        // Read the record and check the field of the record-field
        Record readRecord = repository.read(createdRecord.getId());
        Record readRecordInRecord = (Record)readRecord.getField(new QName(ns, "field2"));
        assertEquals("abc", readRecordInRecord.getField(new QName(ns, "field1")));
        Assert.assertFalse(readRecordInRecord.hasField(new QName(ns, "field1b")));

    }

    @Test
    public void testRecordVTUnallowedField() throws Exception {
        String ns = "testRecordVTUnallowedField";
        // Create a fieldType to be used as a field in a record type to be used as the type for a RecordValueType
        FieldType fieldType1 = typeManager.createFieldType(typeManager.newFieldType(typeManager.getValueType("STRING"), new QName(ns, "field1"), Scope.NON_VERSIONED));
        FieldType fieldType1b = typeManager.createFieldType(typeManager.newFieldType(typeManager.getValueType("STRING"), new QName(ns, "field1b"), Scope.NON_VERSIONED));
        // Create a record type to be used as the type for a RecordValueType
        // Do not add fieldType1b to the record type
        RecordType rt1 = typeManager.recordTypeBuilder()
            .name(new QName(ns, "rt1"))
            .field(fieldType1.getId(), false)
            .create();

        // Make a RecordValueType without the record type specified
        ValueType recordVT1 = typeManager.getValueType("RECORD");

        // Create a fieldType with as value type a RecordValueType
        FieldType fieldType2 = typeManager.createFieldType(typeManager.newFieldType(recordVT1, new QName(ns, "field2"), Scope.NON_VERSIONED));
        // Create a recordType with a field of this field type
        RecordType rt2 = typeManager.recordTypeBuilder()
            .name(new QName(ns, "rt2"))
            .field(fieldType2.getId(), false)
            .create();

        // Make a record to be used as field value, specify the record type here
        Record recordField = repository.recordBuilder()
            .recordType(new QName(ns, "rt1"))
            .field(new QName(ns, "field1"), "abc")
            .field(new QName(ns, "field1b"), "def") // Also fill in field1b, although the recordType does not specify it
            .build();

        // Create a record with a record as field
        try {
            repository.recordBuilder().recordType(new QName(ns, "rt2")).field(new QName(ns,"field2"), recordField).create();
            Assert.fail();
        } catch (RecordException expected) {

        }
    }

    @Test
    public void testRecordVTReadWrite() throws Exception {
        String ns = "testRecordVTWrite";
     // Create a fieldType to be used as a field in a record type to be used as the type for a RecordValueType
        FieldType fieldType1 = typeManager.createFieldType(typeManager.newFieldType(typeManager.getValueType("STRING"), new QName(ns, "field1"), Scope.NON_VERSIONED));
        // Create a record type to be used as the type for a RecordValueType
        RecordType rt1 = typeManager.recordTypeBuilder().name(new QName(ns, "rt1")).field(fieldType1.getId(), false).create();

        // Make a RecordValueType without the record type specified
        ValueType recordVT = typeManager.getValueType("RECORD");


        RecordType rt2 = typeManager.recordTypeBuilder()
            .name(new QName(ns, "rt2"))
            .supertype().id(rt1.getId()).add()
            .create();



        // Create a record with a record as field
        Record createdRecord = repository.recordBuilder()
                .recordType(new QName(ns, "rt2"))
                .field(new QName(ns,"field1"), "def")
                .create();


        DataOutput dataOutput = new DataOutputImpl();

        // Do a write and lets not get exceptions
        recordVT.write(createdRecord, dataOutput, new IdentityRecordStack());

        DataInput dataInput = new DataInputImpl(dataOutput.toByteArray());

        // Do a read and lets not get exceptions
        Record readRecord = recordVT.read(dataInput);

        assertEquals(createdRecord.getFields(), readRecord.getFields());
    }

    @Test
    public void testInvalidValueTypeSyntax() throws Exception {
        try {
            typeManager.getValueType("LIST<");
            fail("expected exception");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            typeManager.getValueType("LIST<>");
            fail("expected exception");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            typeManager.getValueType("LIST<!");
            fail("expected exception");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            typeManager.getValueType("LIST<STRING<>");
            fail("expected exception");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            typeManager.getValueType("RECORD<!namespace}name>");
            fail("expected exception");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }
}
