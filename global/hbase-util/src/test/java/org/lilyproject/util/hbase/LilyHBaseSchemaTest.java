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
package org.lilyproject.util.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

public class LilyHBaseSchemaTest  {

    private static final String RECORD_TABLE_NAME = "myrecordtable";
    private static final String NON_RECORD_TABLE_NAME = "notarecordtable";

    private HBaseTableFactory tableFactory;

    private HTableInterface recordTable;
    private HTableInterface nonRecordTable;

    @Before
    public void setUp() throws IOException {
        tableFactory = mock(HBaseTableFactory.class);

        recordTable = mock(HTableInterface.class);
        HTableDescriptor recordTableDescriptor = new HTableDescriptor(Bytes.toBytes(RECORD_TABLE_NAME));
        recordTableDescriptor.setValue(LilyHBaseSchema.IS_RECORD_TABLE_PROPERTY, LilyHBaseSchema.IS_RECORD_TABLE_VALUE);
        when(recordTable.getTableDescriptor()).thenReturn(recordTableDescriptor);

        nonRecordTable = mock(HTableInterface.class);
        HTableDescriptor nonRecordTableDescriptor = new HTableDescriptor(Bytes.toBytes(NON_RECORD_TABLE_NAME));
        when(nonRecordTable.getTableDescriptor()).thenReturn(nonRecordTableDescriptor);


    }

    @Test
    public void testIsRecordTableDescriptor_True() {
        HTableDescriptor tableDescriptor = new HTableDescriptor(Bytes.toBytes("myrecordtable"));
        tableDescriptor.setValue(LilyHBaseSchema.IS_RECORD_TABLE_PROPERTY, LilyHBaseSchema.IS_RECORD_TABLE_VALUE);
        assertTrue(LilyHBaseSchema.isRecordTableDescriptor(tableDescriptor));
    }

    @Test
    public void testIsRecordTableDescriptor_False_WrongValue() {
        HTableDescriptor tableDescriptor = new HTableDescriptor(Bytes.toBytes("myrecordtable"));
        tableDescriptor.setValue(LilyHBaseSchema.IS_RECORD_TABLE_PROPERTY, Bytes.toBytes("no"));
        assertFalse(LilyHBaseSchema.isRecordTableDescriptor(tableDescriptor));
    }

    @Test
    public void testIsRecordTableDescriptor_False_NoPropertySet() {
        HTableDescriptor tableDescriptor = new HTableDescriptor(Bytes.toBytes("myrecordtable"));
        assertFalse(LilyHBaseSchema.isRecordTableDescriptor(tableDescriptor));
    }

    @Test
    public void testGetRecordTable() throws IOException, InterruptedException {
        when(tableFactory.getTable(any(HTableDescriptor.class))).thenReturn(recordTable);

        HTableInterface recordTable = LilyHBaseSchema.getRecordTable(tableFactory, RECORD_TABLE_NAME);

        assertNotNull(recordTable);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testGetRecordTable_NotRecordTable() throws IOException, InterruptedException {
        when(tableFactory.getTable(any(HTableDescriptor.class))).thenReturn(nonRecordTable);
        LilyHBaseSchema.getRecordTable(tableFactory, NON_RECORD_TABLE_NAME);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testGetRecordTable_WithSplits_NotRecordTable() throws IOException, InterruptedException {
        when(tableFactory.getTable(any(HTableDescriptor.class), any(byte[][].class))).thenReturn(nonRecordTable);
        LilyHBaseSchema.getRecordTable(tableFactory, NON_RECORD_TABLE_NAME, new byte[][]{});
    }

    @Test(expected=IllegalArgumentException.class)
    public void testGetRecordTable_ClientMode_NotRecordTable() throws IOException, InterruptedException {
        when(tableFactory.getTable(any(HTableDescriptor.class), anyBoolean())).thenReturn(nonRecordTable);
        LilyHBaseSchema.getRecordTable(tableFactory, NON_RECORD_TABLE_NAME, true);
    }

    @Test
    public void testCreateRecordTableDescriptor() {
        HTableDescriptor descriptor = LilyHBaseSchema.createRecordTableDescriptor("myrecordtable");
        assertEquals(1, descriptor.getColumnFamilies().length);
        assertEquals("myrecordtable", descriptor.getNameAsString());
        assertTrue(LilyHBaseSchema.isRecordTableDescriptor(descriptor));

    }

    @Test(expected=IllegalArgumentException.class)
    public void testCreateRecordTableDescriptor_WithDotInName() {
        // A dot isn't allowed in a repository table name because it can get in the
        // way of parsing absolute link fields that include a table name
        LilyHBaseSchema.createRecordTableDescriptor("my.recordtable");
    }

    @Test(expected=IllegalArgumentException.class)
    public void testCreateRecordTableDescriptor_WithColonInName() {
        // A colon isn't allowed in a repository table name because it can get in the
        // way of parsing absolute link fields that include a table name
        LilyHBaseSchema.createRecordTableDescriptor("my:recordtable");
    }

}
