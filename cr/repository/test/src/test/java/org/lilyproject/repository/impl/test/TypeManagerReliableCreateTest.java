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


import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.repository.api.*;
import org.lilyproject.repository.impl.HBaseTypeManager;
import org.lilyproject.repository.impl.IdGeneratorImpl;
import org.lilyproject.repository.impl.SchemaIdImpl;
import org.lilyproject.hadooptestfw.HBaseProxy;
import org.lilyproject.hadooptestfw.TestHelper;
import org.lilyproject.util.hbase.*;
import org.lilyproject.util.hbase.LilyHBaseSchema.TypeCf;
import org.lilyproject.util.hbase.LilyHBaseSchema.TypeColumn;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.zookeeper.ZkUtil;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

public class TypeManagerReliableCreateTest {

    private static HBaseProxy HBASE_PROXY;
    private static final byte[] DATA_COLUMN_FAMILY = Bytes.toBytes("data");
    private static final byte[] CONCURRENT_COUNTER_COLUMN_NAME = Bytes.toBytes("cc");
    private static ValueType valueType;
    private static TypeManager typeManager;
    private static ZooKeeperItf zooKeeper;
    private static HBaseTableFactory hbaseTableFactory;
    private static HTableInterface typeTable;


    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging();
        HBASE_PROXY = new HBaseProxy();
        HBASE_PROXY.start();
        zooKeeper = ZkUtil.connect(HBASE_PROXY.getZkConnectString(), 10000);
        hbaseTableFactory = new HBaseTableFactoryImpl(HBASE_PROXY.getConf());
        typeTable = LilyHBaseSchema.getTypeTable(hbaseTableFactory);
        typeManager = new HBaseTypeManager(new IdGeneratorImpl(), HBASE_PROXY.getConf(), zooKeeper, hbaseTableFactory);
        valueType = typeManager.getValueType("LONG");
    }
    
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        Closer.close(typeManager);
        Closer.close(zooKeeper);
        HBASE_PROXY.stop();
    }


    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
        HBASE_PROXY.cleanTables();
    }

    @Test
    public void testConcurrentRecordCreate() throws Exception {
        QName qName = new QName("NS", "testConcurrentRecordCreate");
        
        fakeConcurrentUpdate(qName);
        
        try {
            typeManager.createRecordType(typeManager.newRecordType(qName));
            fail();
        } catch (RecordTypeExistsException expected) {
            // This will be thrown when the cache of the typeManager was updated as a consequence of the update on basicTypeManager
            // Through ZooKeeper the cache will have been marked as invalidated
        } catch (TypeException expected) {
        }
    }

    @Test
    public void testConcurrentRecordUpdate() throws Exception {
        QName qName1 = new QName("NS", "testConcurrentRecordUpdate1");
        QName qName2 = new QName("NS", "testConcurrentRecordUpdate2");
        
        RecordType recordType = typeManager.createRecordType(typeManager.newRecordType(qName1));
        
        // Fake concurrent update
        fakeConcurrentUpdate(qName2);
        
        recordType.setName(qName2);
        try {
            typeManager.updateRecordType(recordType);
            fail();
        } catch (TypeException expected) {
        }
    }


    
    @Test
    public void testConcurrentFieldCreate() throws Exception {
        QName qName = new QName("NS", "testConcurrentFieldCreate");
        
        fakeConcurrentUpdate(qName);
        
        try {
            typeManager.createFieldType(typeManager.newFieldType(valueType, qName, Scope.VERSIONED));
            fail();
        } catch (FieldTypeExistsException expected) {
            // This will be thrown when the cache of the typeManager was updated as a consequence of the update on basicTypeManager
            // Through ZooKeeper the cache will have been marked as invalidated
        } catch (TypeException expected) {
        }
    }
    
    
    @Test
    public void testConcurrentFieldUpdate() throws Exception {
        QName qName1 = new QName("NS", "testConcurrentFieldUpdate1");
        QName qName2 = new QName("NS", "testConcurrentFieldUpdate2");
        
        FieldType createdFieldType = typeManager.createFieldType(typeManager.newFieldType(valueType, qName1, Scope.VERSIONED));
        
        fakeConcurrentUpdate(qName2);
        
        createdFieldType.setName(qName2);
        try {
            typeManager.updateFieldType(createdFieldType);
            fail();
        } catch (TypeException expected) {
        }
    }

    private void fakeConcurrentUpdate(QName qName) throws IOException {
        byte[] nameKey = HBaseTypeManager.encodeName(qName);
        long now = System.currentTimeMillis();
        Put put = new Put(nameKey);
        put.add(TypeCf.DATA.bytes, TypeColumn.CONCURRENT_TIMESTAMP.bytes, Bytes.toBytes(now + 6000));
        typeTable.put(put);
    }
    
    @Test
    public void testGetTypeIgnoresConcurrentCounterRows() throws Exception {
        SchemaId id = new SchemaIdImpl(UUID.randomUUID());
        byte[] rowId = id.getBytes();
        
        typeTable.incrementColumnValue(rowId, DATA_COLUMN_FAMILY, CONCURRENT_COUNTER_COLUMN_NAME, 1);
        try {
            typeManager.getFieldTypeById(id);
            fail();
        } catch (FieldTypeNotFoundException expected) {
        }
        try {
            typeManager.getRecordTypeById(id, null);
            fail();
        } catch (RecordTypeNotFoundException expected) {
        }
        typeManager.close();
    }
}
