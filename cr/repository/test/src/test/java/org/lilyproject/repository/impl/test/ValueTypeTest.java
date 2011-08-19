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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.net.URI;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;

import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.junit.*;
import org.lilyproject.bytes.api.DataInput;
import org.lilyproject.bytes.api.DataOutput;
import org.lilyproject.hadooptestfw.TestHelper;
import org.lilyproject.repository.api.*;
import org.lilyproject.repository.impl.primitivevaluetype.AbstractValueType;
import org.lilyproject.repotestfw.RepositorySetup;

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
        repoSetup.setupRepository(true);

        typeManager = repoSetup.getTypeManager();
        repository = repoSetup.getRepository();
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
        runValueTypeTests("dateTimeRecordTypeId", "DATETIME", new DateTime(), new DateTime(Long.MAX_VALUE), new DateTime(Long.MIN_VALUE));
    }

    @Test
    public void testDateType() throws Exception {
        runValueTypeTests("dateRecordTypeId", "DATE", new LocalDate(), new LocalDate(2900, 10, 14), new LocalDate(1300, 5, 4));
    }

    @Test
    public void testLinkType() throws Exception {
        runValueTypeTests("linkRecordTypeId", "LINK", new Link(idGenerator.newRecordId()), new Link(idGenerator.newRecordId()), new Link(idGenerator.newRecordId()));
    }

    @Test
    public void testUriType() throws Exception {
        runValueTypeTests("uriRecordTypeId", "URI", URI.create("http://foo.com/bar"), URI.create("file://foo/com/bar.txt"), URI.create("https://site/index.html"));
    }
    
    @Test
    public void testBlobType() throws Exception {
        Blob blob1 = new Blob(Bytes.toBytes("aKey"), "text/html", Long.MAX_VALUE, null);
        Blob blob2 = new Blob(Bytes.toBytes("anotherKey"), "image/jpeg", Long.MIN_VALUE, "images/image.jpg");
        Blob blob3 = new Blob("text/plain", Long.valueOf(0), null);
        
        runValueTypeTests("blobTypeId", "BLOB", blob1, blob2, blob3);
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

        comparator = typeManager.getValueType("LIST", "STRING").getComparator();
        assertNull(comparator);
        
        comparator = typeManager.getValueType("PATH", "STRING").getComparator();
        assertNull(comparator); 
    }

    private void runValueTypeTests(String name, String valueType, Object value1, Object value2, Object value3) throws Exception {
        testType(name, valueType, null, value1);
        testType(name, "LIST", valueType, Arrays.asList(value1, value2));
        testType(name, "PATH", valueType, new HierarchyPath(value1, value2));
        testType(name, "LIST", "PATH<"+valueType+">", Arrays.asList(new HierarchyPath(value1, value2), new HierarchyPath(value1, value3)));
    }
    
    private void testType(String name, String valueTypeString, String typeParams,
                    Object fieldValue) throws Exception {
        String testName = name+valueTypeString+typeParams;
        QName fieldTypeName = new QName("valueTypeTest", testName+"FieldId");
        FieldType fieldType = typeManager.createFieldType(typeManager.newFieldType(typeManager.getValueType(
                        valueTypeString, typeParams), fieldTypeName, Scope.VERSIONED));
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

        public String getName() {
            return NAME;
        }
        
        public ValueType getBaseValueType() {
            return this;
        }
        
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

        public void write(Object value, DataOutput dataOutput) {
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

        public XYCoordinates(int x, int y) {
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
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            XYCoordinates other = (XYCoordinates) obj;
            if (!getOuterType().equals(other.getOuterType()))
                return false;
            if (x != other.x)
                return false;
            if (y != other.y)
                return false;
            return true;
        }

        private ValueTypeTest getOuterType() {
            return ValueTypeTest.this;
        }
    }
}
