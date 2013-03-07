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

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.lilyproject.util.hbase.LilyHBaseSchema.RecordCf;
import org.lilyproject.util.hbase.LilyHBaseSchema.RecordColumn;
import org.lilyproject.util.repo.RecordEvent;
import org.lilyproject.util.repo.RecordEvent.IndexRecordFilterData;

public class IndexerEditFilterTest {

    private static final String INDEX_NAME = "IndexName";

    private IndexerEditFilter editFilter;

    @Before
    public void setUp() {
        editFilter = new IndexerEditFilter(INDEX_NAME);
    }

    @Test
    public void testApply_NoPayload() {
        WALEdit walEdit = new WALEdit();
        walEdit.add(new KeyValue(Bytes.toBytes("row"), RecordCf.DATA.bytes, Bytes.toBytes("not payload"),
                Bytes.toBytes("value")));

        editFilter.apply(walEdit);

        assertEquals(0, walEdit.size());
    }

    @Test
    public void testApply_Payload_NotApplicableIndex() {
        RecordEvent recordEvent = new RecordEvent();
        IndexRecordFilterData filterData = new IndexRecordFilterData();
        filterData.setSubscriptionInclusions(ImmutableSet.of("SomeOtherIndexName"));
        recordEvent.setIndexRecordFilterData(filterData);

        WALEdit walEdit = new WALEdit();
        walEdit.add(new KeyValue(Bytes.toBytes("row"), RecordCf.DATA.bytes, RecordColumn.PAYLOAD.bytes,
                recordEvent.toJsonBytes()));

        editFilter.apply(walEdit);

        assertEquals(0, walEdit.size());
    }

    @Test
    public void testApply_Payload_ApplicableIndex() {
        RecordEvent recordEvent = new RecordEvent();
        IndexRecordFilterData filterData = new IndexRecordFilterData();
        filterData.setSubscriptionInclusions(ImmutableSet.of(INDEX_NAME));
        recordEvent.setIndexRecordFilterData(filterData);

        WALEdit walEdit = new WALEdit();
        walEdit.add(new KeyValue(Bytes.toBytes("row"), RecordCf.DATA.bytes, RecordColumn.PAYLOAD.bytes,
                recordEvent.toJsonBytes()));

        editFilter.apply(walEdit);

        assertEquals(1, walEdit.size());
    }

    @Test
    public void testApply_Payload_ApplicableIndexButNoIndexFlagIsSet() {
        RecordEvent recordEvent = new RecordEvent();
        IndexRecordFilterData filterData = new IndexRecordFilterData();
        filterData.setSubscriptionInclusions(ImmutableSet.of(INDEX_NAME));
        recordEvent.setIndexRecordFilterData(filterData);
        recordEvent.getAttributes().put(IndexerEditFilter.NO_INDEX_FLAG, "false");

        WALEdit walEdit = new WALEdit();
        walEdit.add(new KeyValue(Bytes.toBytes("row"), RecordCf.DATA.bytes, RecordColumn.PAYLOAD.bytes,
                recordEvent.toJsonBytes()));

        editFilter.apply(walEdit);

        assertEquals(0, walEdit.size());
    }

    @Test
    public void testApply_NonJsonPayload() {

        WALEdit walEdit = new WALEdit();
        walEdit.add(new KeyValue(Bytes.toBytes("row"), RecordCf.DATA.bytes, RecordColumn.PAYLOAD.bytes,
                Bytes.toBytes("non-json payload")));

        editFilter.apply(walEdit);

        // If we can't parse the RecordEvent, we don't want to have the IndexUpdater dealing with it either
        assertEquals(0, walEdit.size());
    }

}
