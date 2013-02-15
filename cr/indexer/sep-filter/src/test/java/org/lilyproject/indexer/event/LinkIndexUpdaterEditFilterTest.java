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
package org.lilyproject.indexer.event;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.lilyproject.util.hbase.LilyHBaseSchema.RecordCf;
import org.lilyproject.util.hbase.LilyHBaseSchema.RecordColumn;

public class LinkIndexUpdaterEditFilterTest {
    
    private LinkIndexUpdaterEditFilter editFilter;
    
    @Before
    public void setUp() {
        editFilter = new LinkIndexUpdaterEditFilter();
    }

    @Test
    public void testApply_NoPayload() {
        WALEdit walEdit = new WALEdit();
        walEdit.add(new KeyValue(Bytes.toBytes("row"), RecordCf.DATA.bytes, RecordColumn.OCC.bytes, Bytes.toBytes(123)));
        
        editFilter.apply(walEdit);
        
        assertEquals(0, walEdit.getKeyValues().size());
    }
    
    @Test
    public void testApply_WithPayload() {

        WALEdit walEdit = new WALEdit();
        walEdit.add(new KeyValue(Bytes.toBytes("row"), RecordCf.DATA.bytes, RecordColumn.PAYLOAD.bytes, Bytes.toBytes("payload")));
        
        editFilter.apply(walEdit);
        
        assertEquals(1, walEdit.getKeyValues().size());
    }

}
