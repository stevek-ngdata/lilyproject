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
package org.lilyproject.util.repo.test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.impl.id.IdGeneratorImpl;
import org.lilyproject.repository.impl.id.SchemaIdImpl;
import org.lilyproject.util.repo.RecordEvent;
import org.lilyproject.util.repo.RecordEvent.FieldChange;
import org.lilyproject.util.repo.RecordEvent.IndexRecordFilterData;

public class RecordEventTest {

    private IdGenerator idGenerator;

    @Before
    public void setUp() {
        this.idGenerator = new IdGeneratorImpl();
    }

    @Test
    public void testRecordEvent_JsonRoundTrip() throws Exception {

        RecordEvent event = new RecordEvent();
        byte[] json = event.toJsonBytes();
        event = new RecordEvent(json, idGenerator);

        assertNull(event.getIndexRecordFilterData());

        SchemaId oldRtId = idGenerator.getSchemaId(UUID.randomUUID());
        SchemaId newRtId = idGenerator.getSchemaId(UUID.randomUUID());

        RecordEvent.IndexRecordFilterData idxSel = new RecordEvent.IndexRecordFilterData();
        event.setIndexRecordFilterData(idxSel);
        idxSel.setOldRecordType(oldRtId);
        idxSel.setNewRecordType(newRtId);

        json = event.toJsonBytes();
        event = new RecordEvent(json, idGenerator);

        assertNotNull(event.getIndexRecordFilterData());
        assertEquals(oldRtId, event.getIndexRecordFilterData().getOldRecordType());
        assertEquals(newRtId, event.getIndexRecordFilterData().getNewRecordType());
        assertNull(event.getIndexRecordFilterData().getFieldChanges());

        SchemaId field1Id = idGenerator.getSchemaId(UUID.randomUUID());
        SchemaId field2Id = idGenerator.getSchemaId(UUID.randomUUID());
        SchemaId field3Id = idGenerator.getSchemaId(UUID.randomUUID());
        SchemaId field4Id = idGenerator.getSchemaId(UUID.randomUUID());

        event = new RecordEvent();
        idxSel = new RecordEvent.IndexRecordFilterData();
        event.setIndexRecordFilterData(idxSel);
        idxSel.addChangedField(field1Id, null, null);
        idxSel.addChangedField(field2Id, Bytes.toBytes("foo1"), Bytes.toBytes("foo2"));
        idxSel.addChangedField(field3Id, Bytes.toBytes("foo3"), null);
        idxSel.addChangedField(field4Id, null, Bytes.toBytes("foo4"));

        json = event.toJsonBytes();
        event = new RecordEvent(json, idGenerator);

        List<RecordEvent.FieldChange> fieldChanges = event.getIndexRecordFilterData().getFieldChanges();
        assertEquals(4, fieldChanges.size());

        assertEquals(field1Id, fieldChanges.get(0).getId());
        assertNull(fieldChanges.get(0).getOldValue());
        assertNull(fieldChanges.get(0).getNewValue());

        assertEquals(field2Id, fieldChanges.get(1).getId());
        assertArrayEquals(Bytes.toBytes("foo1"), fieldChanges.get(1).getOldValue());
        assertArrayEquals(Bytes.toBytes("foo2"), fieldChanges.get(1).getNewValue());

        assertEquals(field3Id, fieldChanges.get(2).getId());
        assertArrayEquals(Bytes.toBytes("foo3"), fieldChanges.get(2).getOldValue());
        assertNull(null, fieldChanges.get(2).getNewValue());

        assertEquals(field4Id, fieldChanges.get(3).getId());
        assertNull(fieldChanges.get(3).getOldValue());
        assertArrayEquals(Bytes.toBytes("foo4"), fieldChanges.get(3).getNewValue());
    }

    @Test
    public void testRecordEvent_JsonRoundtrip_TableName() throws IOException {
        final String tableName = "_table_name_";
        RecordEvent recordEvent = new RecordEvent();
        recordEvent.setTableName(tableName);

        byte[] jsonBytes = recordEvent.toJsonBytes();

        RecordEvent deserialized = new RecordEvent(jsonBytes, idGenerator);

        assertEquals(tableName, deserialized.getTableName());
    }

    @Test
    public void testIndexRecordFilterData_JsonRoundtrip() {
        IndexRecordFilterData recordFilterData = new IndexRecordFilterData();
        SchemaId newTypeSchemaId = new SchemaIdImpl("newtype".getBytes());
        SchemaId oldTypeSchemaId = new SchemaIdImpl("oldtype".getBytes());
        SchemaId changedFieldId = new SchemaIdImpl("changedfield".getBytes());
        byte[] oldFieldValue = new byte[] { 1 };
        byte[] newFieldValue = new byte[] { 2 };

        recordFilterData.setNewRecordExists(true);
        recordFilterData.setOldRecordExists(true);
        recordFilterData.setNewRecordType(newTypeSchemaId);
        recordFilterData.setOldRecordType(oldTypeSchemaId);
        recordFilterData.addChangedField(changedFieldId, oldFieldValue, newFieldValue);

        IndexRecordFilterData deserialized = doJsonRoundtrip(recordFilterData);

        assertTrue(deserialized.getNewRecordExists());
        assertTrue(deserialized.getOldRecordExists());
        assertEquals(newTypeSchemaId, deserialized.getNewRecordType());
        assertEquals(oldTypeSchemaId, deserialized.getOldRecordType());
        List<FieldChange> fieldChanges = deserialized.getFieldChanges();

        assertEquals(1, fieldChanges.size());
        FieldChange fieldChange = fieldChanges.get(0);
        assertEquals(changedFieldId, fieldChange.getId());
        assertArrayEquals(oldFieldValue, fieldChange.getOldValue());
        assertArrayEquals(newFieldValue, fieldChange.getNewValue());

        assertEquals(recordFilterData, deserialized);
    }

    @Test
    public void testIndexRecordFilterData_JsonRoundtrip_IncludeIndexes() {
        IndexRecordFilterData filterData = new IndexRecordFilterData();
        filterData.setSubscriptionInclusions(Sets.newHashSet("indexA", "indexB"));

        assertEquals(filterData, doJsonRoundtrip(filterData));
    }

    @Test
    public void testIndexRecordFilterData_JsonRoundtrip_ExcludeIndexes() {
        IndexRecordFilterData filterData = new IndexRecordFilterData();
        filterData.setSubscriptionExclusions(Sets.newHashSet("indexA", "indexB"));

        assertEquals(filterData, doJsonRoundtrip(filterData));
    }

    @Test
    public void testAppliesToSubscription_DefaultCase() {
        IndexRecordFilterData filterData = new IndexRecordFilterData();

        assertTrue(filterData.appliesToSubscription("indexname"));
    }

    @Test
    public void testAppliesToSubscription_AllInclusive() {
        IndexRecordFilterData filterData = new IndexRecordFilterData();
        filterData.setSubscriptionInclusions(IndexRecordFilterData.ALL_INDEX_SUBSCRIPTIONS);

        assertTrue(filterData.appliesToSubscription("indexname"));
    }

    @Test
    public void testAppliesToSubscription_AllExclusive() {
        IndexRecordFilterData filterData = new IndexRecordFilterData();
        filterData.setSubscriptionExclusions(IndexRecordFilterData.ALL_INDEX_SUBSCRIPTIONS);

        assertFalse(filterData.appliesToSubscription("indexname"));
    }

    @Test
    public void testAppliesToSubscription_Included() {
        IndexRecordFilterData filterData = new IndexRecordFilterData();
        filterData.setSubscriptionInclusions(ImmutableSet.of("to_include"));

        assertTrue(filterData.appliesToSubscription("to_include"));
    }

    @Test
    public void testAppliesToSubscription_Excluded() {
        IndexRecordFilterData filterData = new IndexRecordFilterData();
        filterData.setSubscriptionExclusions(ImmutableSet.of("to_exclude"));

        assertFalse(filterData.appliesToSubscription("to_exclude"));
    }

    @Test
    public void testAppliesToSubcription_NotExcluded() {
        IndexRecordFilterData filterData = new IndexRecordFilterData();
        filterData.setSubscriptionExclusions(ImmutableSet.of("to_exclude"));

        assertTrue(filterData.appliesToSubscription("not_excluded"));
    }

    @Test
    public void testAppliesToSubscription_NotIncluded() {
        IndexRecordFilterData filterData = new IndexRecordFilterData();
        filterData.setSubscriptionInclusions(ImmutableSet.of("to_include"));

        assertFalse(filterData.appliesToSubscription("not_included"));
    }

    private IndexRecordFilterData doJsonRoundtrip(IndexRecordFilterData recordFilterData) {
        RecordEvent recordEvent = new RecordEvent();
        recordEvent.setIndexRecordFilterData(recordFilterData);
        RecordEvent deserializedEvent;
        try {
            deserializedEvent = new RecordEvent(recordEvent.toJsonBytes(), idGenerator);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return deserializedEvent.getIndexRecordFilterData();
    }

}
