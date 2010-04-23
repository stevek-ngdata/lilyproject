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
import java.util.Date;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilycms.repository.api.Blob;
import org.lilycms.repository.api.BlobStoreAccessFactory;
import org.lilycms.repository.api.FieldType;
import org.lilycms.repository.api.HierarchyPath;
import org.lilycms.repository.api.PrimitiveValueType;
import org.lilycms.repository.api.QName;
import org.lilycms.repository.api.Record;
import org.lilycms.repository.api.RecordType;
import org.lilycms.repository.api.Scope;
import org.lilycms.repository.impl.DFSBlobStoreAccess;
import org.lilycms.repository.impl.HBaseRepository;
import org.lilycms.repository.impl.HBaseTypeManager;
import org.lilycms.repository.impl.IdGeneratorImpl;
import org.lilycms.repository.impl.SizeBasedBlobStoreAccessFactory;
import org.lilycms.testfw.TestHelper;

public class ValueTypeTest {

    private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

    private HBaseTypeManager typeManager;
    private HBaseRepository repository;

    private IdGeneratorImpl idGenerator;

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
        idGenerator = new IdGeneratorImpl();
        typeManager = new HBaseTypeManager(idGenerator, TEST_UTIL.getConfiguration());
        DFSBlobStoreAccess dfsBlobStoreAccess = new DFSBlobStoreAccess(TEST_UTIL.getDFSCluster().getFileSystem());
        BlobStoreAccessFactory blobStoreOutputStreamFactory = new SizeBasedBlobStoreAccessFactory(dfsBlobStoreAccess);
        repository = new HBaseRepository(typeManager, idGenerator, blobStoreOutputStreamFactory , TEST_UTIL.getConfiguration());
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
    public void testBooleanType() throws Exception {
        runValueTypeTests("booleanRecordTypeId", "BOOLEAN", true, false, true);
    }
    
    @Test
    public void testDateType() throws Exception {
        runValueTypeTests("dateRecordTypeId", "DATE", new Date(), new Date(Long.MAX_VALUE), new Date(Long.MIN_VALUE));
    }

    @Test
    public void testLinkType() throws Exception {
        runValueTypeTests("linkRecordTypeId", "LINK", idGenerator.newRecordId(), idGenerator.newRecordId(), idGenerator.newRecordId());
    }
    
    @Test
    public void testBlobType() throws Exception {
        Blob blob1 = new Blob(Bytes.toBytes("aKey"), "text/html", Long.MAX_VALUE, null);
        Blob blob2 = new Blob(Bytes.toBytes("anotherKey"), "image/jpeg", Long.MIN_VALUE, "images/image.jpg");
        Blob blob3 = new Blob("text/plain", Long.valueOf(0), null);
        runValueTypeTests("blobTypeId", "BLOB", blob1, blob2, blob3);
    }
    
    @Test
    public void testNewPrimitiveType() throws Exception {
        typeManager.registerPrimitiveValueType(new XYPrimitiveValueType());
        runValueTypeTests("xyRecordTypeId", "XY", new XYCoordinates(-1, 1), new XYCoordinates(Integer.MIN_VALUE, Integer.MAX_VALUE), new XYCoordinates(666, 777));
    }

    private void runValueTypeTests(String recordTypeId, String primitiveValueType, Object value1, Object value2, Object value3) throws Exception {
        testType(recordTypeId, primitiveValueType, false, false, value1);
        testType(recordTypeId, primitiveValueType, true, false, Arrays.asList(new Object[] { value1,
                        value2 }));
        testType(recordTypeId, primitiveValueType, false, true, new HierarchyPath(new Object[] { value1,
                        value2 }));
        testType(recordTypeId, primitiveValueType, true, true, Arrays.asList(new HierarchyPath[] {
                new HierarchyPath(new Object[] { value1, value2 }),
                new HierarchyPath(new Object[] { value1, value3 }) }));
    }
    
    private void testType(String recordTypeId, String valueTypeString, boolean multivalue, boolean hierarchical,
                    Object fieldValue) throws Exception {
        QName name = new QName(null, valueTypeString+"FieldId"+multivalue+hierarchical);
        FieldType fieldType = typeManager.createFieldType(typeManager.newFieldType(typeManager.getValueType(
                        valueTypeString, multivalue, hierarchical), name, Scope.VERSIONED));
        RecordType recordType = typeManager.newRecordType(recordTypeId+"RecordTypeId"+multivalue+hierarchical);
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType.getId(), true));
        typeManager.createRecordType(recordType);

        Record record = repository.newRecord(idGenerator.newRecordId());
        record.setRecordType(recordType.getId(), recordType.getVersion());
        record.setField(fieldType.getName(), fieldValue);
        repository.create(record);

        Record actualRecord = repository.read(record.getId());
        assertEquals(fieldValue, actualRecord.getField(fieldType.getName()));
    }

    private class XYPrimitiveValueType implements PrimitiveValueType {

        private final String NAME = "XY";

        public String getName() {
            return NAME;
        }

        public Object fromBytes(byte[] bytes) {
            int x = Bytes.toInt(bytes, 0, Bytes.SIZEOF_INT);
            int y = Bytes.toInt(bytes, Bytes.SIZEOF_INT, Bytes.SIZEOF_INT);
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
