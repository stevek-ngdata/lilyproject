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

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.impl.id.IdGeneratorImpl;
import org.lilyproject.util.repo.RecordEvent;

import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class RecordEventTest {
    @Test
    public void testIndexSelectionJson() throws Exception {
        IdGenerator idGenerator = new IdGeneratorImpl();

        RecordEvent event = new RecordEvent();
        byte[] json = event.toJsonBytes();
        event = new RecordEvent(json, idGenerator);

        assertNull(event.getIndexSelection());

        SchemaId oldRtId = idGenerator.getSchemaId(UUID.randomUUID());
        SchemaId newRtId = idGenerator.getSchemaId(UUID.randomUUID());

        RecordEvent.IndexSelection idxSel = new RecordEvent.IndexSelection();
        event.setIndexSelection(idxSel);
        idxSel.setOldRecordType(oldRtId);
        idxSel.setNewRecordType(newRtId);

        json = event.toJsonBytes();
        event = new RecordEvent(json, idGenerator);

        assertNotNull(event.getIndexSelection());
        assertEquals(oldRtId, event.getIndexSelection().getOldRecordType());
        assertEquals(newRtId, event.getIndexSelection().getNewRecordType());
        assertNull(event.getIndexSelection().getFieldChanges());

        SchemaId field1Id = idGenerator.getSchemaId(UUID.randomUUID());
        SchemaId field2Id = idGenerator.getSchemaId(UUID.randomUUID());
        SchemaId field3Id = idGenerator.getSchemaId(UUID.randomUUID());
        SchemaId field4Id = idGenerator.getSchemaId(UUID.randomUUID());

        event = new RecordEvent();
        idxSel = new RecordEvent.IndexSelection();
        event.setIndexSelection(idxSel);
        idxSel.addChangedField(field1Id, null, null);
        idxSel.addChangedField(field2Id, Bytes.toBytes("foo1"), Bytes.toBytes("foo2"));
        idxSel.addChangedField(field3Id, Bytes.toBytes("foo3"), null);
        idxSel.addChangedField(field4Id, null, Bytes.toBytes("foo4"));

        json = event.toJsonBytes();
        event = new RecordEvent(json, idGenerator);

        List<RecordEvent.FieldChange> fieldChanges = event.getIndexSelection().getFieldChanges();
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
}
