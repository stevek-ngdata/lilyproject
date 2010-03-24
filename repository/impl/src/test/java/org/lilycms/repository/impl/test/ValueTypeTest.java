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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilycms.repository.api.FieldDescriptor;
import org.lilycms.repository.api.PrimitiveValueType;
import org.lilycms.repository.api.Record;
import org.lilycms.repository.api.RecordType;
import org.lilycms.repository.api.ValueType;
import org.lilycms.repository.impl.FieldDescriptorImpl;
import org.lilycms.repository.impl.HBaseRepository;
import org.lilycms.repository.impl.HBaseTypeManager;
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
        typeManager = new HBaseTypeManager(RecordTypeImpl.class, FieldDescriptorImpl.class, TEST_UTIL.getConfiguration());
        repository = new HBaseRepository(typeManager, new IdGeneratorImpl(), RecordImpl.class, TEST_UTIL.getConfiguration());
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testStringType() throws Exception {
        RecordType recordType = typeManager.newRecordType("stringRecordTypeId");
        FieldDescriptor fieldDescriptor = typeManager.newFieldDescriptor("aFieldId", typeManager.getValueType("STRING", false), true, true);
        recordType.addFieldDescriptor(fieldDescriptor);
        typeManager.createRecordType(recordType);
        
        Record record = repository.newRecord();
        record.setRecordType(recordType.getId(), recordType.getVersion());
        record.setField("aFieldId", "aStringValue");
        repository.create(record);
        
        Record actualRecord = repository.read(record.getId());
        String actualField = (String)actualRecord.getField("aFieldId");
        assertEquals("aStringValue", actualField);
    }
    
    @Test
    public void testIntegerType() throws Exception {
        RecordType recordType = typeManager.newRecordType("integerRecordTypeId");
        FieldDescriptor fieldDescriptor = typeManager.newFieldDescriptor("aFieldId", typeManager.getValueType("INTEGER", false), true, true);
        recordType.addFieldDescriptor(fieldDescriptor);
        typeManager.createRecordType(recordType);
        
        Record record = repository.newRecord();
        record.setRecordType(recordType.getId(), recordType.getVersion());
        record.setField("aFieldId", 123);
        repository.create(record);
        
        Record actualRecord = repository.read(record.getId());
        Integer actualField = (Integer)actualRecord.getField("aFieldId");
        assertEquals(Integer.valueOf(123), actualField);
    }
    
    @Test
    public void testMultiValueType() throws Exception {
        RecordType recordType = typeManager.newRecordType("multivalueRecordTypeId");
        ValueType multiValueType = typeManager.getValueType("STRING", true);
        FieldDescriptor fieldDescriptor = typeManager.newFieldDescriptor("aFieldId", multiValueType, false, false);
        recordType.addFieldDescriptor(fieldDescriptor);
        typeManager.createRecordType(recordType);
        
        Record record = repository.newRecord();
        record.setRecordType(recordType.getId(), recordType.getVersion());
        List<String> items = new ArrayList<String>();
        items.add("value1");
        items.add("value2");
        record.setField("aFieldId", items);
        repository.create(record);
        
        Record actualRecord = repository.read(record.getId());
        List<String> actualField = (List<String>)actualRecord.getField("aFieldId");
        assertEquals(items, actualField);
    }
    
    @Test
    public void testNewPrimitiveType() throws Exception {
        typeManager.registerPrimitiveValueType(new XYPrimitiveValueType());
        
        RecordType recordType = typeManager.newRecordType("xyRecordTypeId");
        FieldDescriptor fieldDescriptor = typeManager.newFieldDescriptor("aFieldId", typeManager.getValueType("XY", false), true, true);
        recordType.addFieldDescriptor(fieldDescriptor);
        typeManager.createRecordType(recordType);
        
        Record record = repository.newRecord();
        record.setRecordType(recordType.getId(), recordType.getVersion());
        record.setField("aFieldId", new XYCoordinates(5, 6));
        repository.create(record);
        
        Record actualRecord = repository.read(record.getId());
        XYCoordinates actualField = (XYCoordinates)actualRecord.getField("aFieldId");
        assertEquals(5, actualField.getX());
        assertEquals(6, actualField.getY());
    }
    
    @Test
    public void testNewPrimitiveTypeAsMultiValueType() throws Exception {
        typeManager.registerPrimitiveValueType(new XYPrimitiveValueType());
        
        RecordType recordType = typeManager.newRecordType("multivalueXYRecordTypeId");
        ValueType multiValueType = typeManager.getValueType("XY", true);
        FieldDescriptor fieldDescriptor = typeManager.newFieldDescriptor("aFieldId", multiValueType, false, false);
        recordType.addFieldDescriptor(fieldDescriptor);
        typeManager.createRecordType(recordType);
        
        Record record = repository.newRecord();
        record.setRecordType(recordType.getId(), recordType.getVersion());
        List<XYCoordinates> items = new ArrayList<XYCoordinates>();
        items.add(new XYCoordinates(1, 2));
        items.add(new XYCoordinates(666, 777));
        record.setField("aFieldId", items);
        repository.create(record);
        
        Record actualRecord = repository.read(record.getId());
        List<XYCoordinates> actualField = (List<XYCoordinates>)actualRecord.getField("aFieldId");
        assertEquals(items, actualField);
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
            result = Bytes.add(result, Bytes.toBytes(((XYCoordinates)value).getX()));
            result = Bytes.add(result, Bytes.toBytes(((XYCoordinates)value).getY()));
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
