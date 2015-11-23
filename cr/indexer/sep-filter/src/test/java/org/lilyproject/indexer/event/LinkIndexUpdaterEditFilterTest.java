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

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.lilyproject.util.hbase.LilyHBaseSchema.RecordCf;
import org.lilyproject.util.hbase.LilyHBaseSchema.RecordColumn;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LinkIndexUpdaterEditFilterTest {

    private LinkIndexUpdaterEditFilter editFilter;

    private static final long SUBSCRIPTION_TIMESTAMP = 100000;

    private static final byte[] TABLE_NAME = Bytes.toBytes("test_table");
    private static final byte[] encodedRegionName = Bytes.toBytes("1028785192");
    private static final List<UUID> clusterUUIDs = new ArrayList<UUID>();


    private HLog.Entry createHlogEntry(byte[] tableName, KeyValue... keyValues) {
        return createHlogEntry(tableName, SUBSCRIPTION_TIMESTAMP + 1, keyValues);
    }

    private HLog.Entry createHlogEntry(byte[] tableName, long writeTime, KeyValue... keyValues) {
        HLog.Entry entry = mock(HLog.Entry.class, Mockito.RETURNS_DEEP_STUBS);
        when(entry.getEdit().getKeyValues()).thenReturn(Lists.newArrayList(keyValues));
        when(entry.getKey().getTablename()).thenReturn(TableName.valueOf(tableName));
        when(entry.getKey().getWriteTime()).thenReturn(writeTime);
        when(entry.getKey().getEncodedRegionName()).thenReturn(encodedRegionName);
        when(entry.getKey().getClusterIds()).thenReturn(clusterUUIDs);
        return entry;
    }

    @Before
    public void setUp() {
        editFilter = new LinkIndexUpdaterEditFilter();
    }

    @Test
    public void testApply_NoPayload() {
        HLog.Entry hLogEntry = createHlogEntry(TABLE_NAME, new KeyValue(Bytes.toBytes("row"), RecordCf.DATA.bytes, RecordColumn.OCC.bytes, Bytes.toBytes(123)));

        editFilter.apply(hLogEntry);

        assertEquals(0, hLogEntry.getEdit().getKeyValues().size());
    }

    @Test
    public void testApply_WithPayload() {
        HLog.Entry hLogEntry = createHlogEntry(TABLE_NAME, new KeyValue(Bytes.toBytes("row"), RecordCf.DATA.bytes, RecordColumn.PAYLOAD.bytes, Bytes.toBytes("payload")));

        editFilter.apply(hLogEntry);

        assertEquals(1, hLogEntry.getEdit().getKeyValues().size());
    }

}
