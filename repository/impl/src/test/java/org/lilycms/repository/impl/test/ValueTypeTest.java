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
package org.lilycms.repository.impl.test;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.apache.commons.net.nntp.NewGroupsOrNewsQuery;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilycms.repository.api.FieldDescriptor;
import org.lilycms.repository.api.FieldNotFoundException;
import org.lilycms.repository.api.InvalidRecordException;
import org.lilycms.repository.api.PrimitiveValueType;
import org.lilycms.repository.api.Record;
import org.lilycms.repository.api.RecordExistsException;
import org.lilycms.repository.api.RecordNotFoundException;
import org.lilycms.repository.api.RecordType;
import org.lilycms.repository.api.RepositoryException;
import org.lilycms.repository.impl.FieldDescriptorImpl;
import org.lilycms.repository.impl.HBaseRepository;
import org.lilycms.repository.impl.HBaseTypeManager;
import org.lilycms.repository.impl.HierarchyPathImpl;
import org.lilycms.repository.impl.IdGeneratorImpl;
import org.lilycms.repository.impl.RecordImpl;
import org.lilycms.repository.impl.RecordTypeImpl;
import org.lilycms.testfw.TestHelper;

/**
 *
 */
public class ValueTypeTest {

    private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

    private HBaseTypeManager typeManager;
    private HBaseRepository repository;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging();
        TEST_UTIL.startMiniCluster(1);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        TEST_UTIL.shutdownMiniCluster();
    }

    @Before
    public void setUp() throws Exception {
        typeManager = new HBaseTypeManager(RecordTypeImpl.class, FieldDescriptorImpl.class, TEST_UTIL
                        .getConfiguration());
        repository = new HBaseRepository(typeManager, new IdGeneratorImpl(), RecordImpl.class, TEST_UTIL
                        .getConfiguration());
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testStringType() throws Exception {
        testType("stringRecordTypeId", "STRING", false, false, "aStringValue");
        testType("stringRecordTypeId", "STRING", true, false, Arrays.asList(new String[] { "value1", "value2" }));
        testType("stringRecordTypeId", "STRING", false, true, new HierarchyPathImpl(new String[] { "foo", "bar" }));
        testType("stringRecordTypeId", "STRING", true, true, Arrays.asList(new HierarchyPathImpl[] {
                new HierarchyPathImpl(new String[] { "foo", "bar" }),
                new HierarchyPathImpl(new String[] { "foo", "pub" }) }));
    }

    @Test
    public void testIntegerType() throws Exception {
        testType("integerRecordTypeId", "INTEGER", false, false, new Integer(123));
        testType("integerRecordTypeId", "INTEGER", true, false, Arrays.asList(new Integer[] { Integer.valueOf(666),
                        Integer.valueOf(777) }));
        testType("integerRecordTypeId", "INTEGER", false, true, new HierarchyPathImpl(new Integer[] { Integer.valueOf(1),
                        Integer.valueOf(2) }));
        testType("integerRecordTypeId", "INTEGER", true, true, Arrays.asList(new HierarchyPathImpl[] {
                new HierarchyPathImpl(new Integer[] { Integer.valueOf(5), Integer.valueOf(6) }),
                new HierarchyPathImpl(new Integer[] { Integer.valueOf(5), Integer.valueOf(7) }) }));
    }

    @Test
    public void testNewPrimitiveType() throws Exception {
        typeManager.registerPrimitiveValueType(new XYPrimitiveValueType());
        testType("xyRecordTypeId", "XY", false, false, new XYCoordinates(5, 6));
        testType("xyRecordTypeId", "XY", true, false, Arrays.asList(new XYCoordinates[] { new XYCoordinates(5, 6),
                        new XYCoordinates(30, 40) }));
        testType("xyRecordTypeId", "XY", false, true, new HierarchyPathImpl(new XYCoordinates[] { new XYCoordinates(90, 91), new XYCoordinates(92, 93) }));
        testType("xyRecordTypeId", "XY", true, true, Arrays.asList(new HierarchyPathImpl[] {
                new HierarchyPathImpl(new XYCoordinates[] { new XYCoordinates(100, 100), new XYCoordinates(101, 101) }),
                new HierarchyPathImpl(new XYCoordinates[] { new XYCoordinates(100, 100), new XYCoordinates(102, 102) }) }));
    }

    private void testType(String recordId, String valueTypeString, boolean multivalue, boolean hierarchical,
                    Object fieldValue) throws RepositoryException, RecordExistsException, RecordNotFoundException,
                    InvalidRecordException, FieldNotFoundException {
        RecordType recordType = typeManager.newRecordType(recordId);
        FieldDescriptor fieldDescriptor = typeManager.newFieldDescriptor("aFieldId", typeManager.getValueType(
                        valueTypeString, multivalue, hierarchical), true, true);
        recordType.addFieldDescriptor(fieldDescriptor);
        typeManager.createRecordType(recordType);

        Record record = repository.newRecord();
        record.setRecordType(recordType.getId(), recordType.getVersion());
        record.setField("aFieldId", fieldValue);
        repository.create(record);

        Record actualRecord = repository.read(record.getId());
        assertEquals(fieldValue, actualRecord.getField("aFieldId"));
    }

    private class XYPrimitiveValueType implements PrimitiveValueType {

        private final String NAME = "XY";

        public String getName() {
            return NAME;
        }

        public Object fromBytes(byte[] value) {
            int x = Bytes.toInt(value, 0, Bytes.SIZEOF_INT);
            int y = Bytes.toInt(value, Bytes.SIZEOF_INT, Bytes.SIZEOF_INT);
            return new XYCoordinates(x, y);
        }

        public byte[] toBytes(Object value) {
            byte[] result = new byte[0];
            result = Bytes.add(result, Bytes.toBytes(((XYCoordinates) value).getX()));
            result = Bytes.add(result, Bytes.toBytes(((XYCoordinates) value).getY()));
            return result;
        }

        public Class getType() {
            return XYCoordinates.class;
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
