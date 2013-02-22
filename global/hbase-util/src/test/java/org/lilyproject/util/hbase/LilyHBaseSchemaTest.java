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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class LilyHBaseSchemaTest  {
    
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

}
