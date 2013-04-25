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
package org.lilyproject.repository.remote;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.junit.Before;
import org.junit.Test;
import org.lilyproject.avro.AvroConverter;
import org.lilyproject.avro.AvroLily;
import org.lilyproject.avro.AvroMutationCondition;
import org.lilyproject.repository.api.BlobManager;
import org.lilyproject.repository.api.MutationCondition;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.impl.TenantTableKey;
import org.lilyproject.util.hbase.LilyHBaseSchema.Table;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RemoteRepositoryTest {

    private AvroLily avroLily;
    private AvroLilyTransceiver avroLilyTransceiver;
    private AvroConverter avroConverter;
    private HTableInterface recordTable;
    private RemoteRepository remoteRepository;
    private static final String tenantId = "public";

    @Before
    public void setUp() throws IOException, InterruptedException {
        avroLily = mock(AvroLily.class);
        avroLilyTransceiver = mock(AvroLilyTransceiver.class);
        when(avroLilyTransceiver.getLilyProxy()).thenReturn(avroLily);
        avroConverter = mock(AvroConverter.class);
        recordTable = mock(HTableInterface.class);

        remoteRepository = new RemoteRepository(new TenantTableKey(tenantId, Table.RECORD.name), avroLilyTransceiver,
                avroConverter, mock(RemoteRepositoryManager.class), mock(BlobManager.class), recordTable);
    }

    @Test
    public void testDelete_RecordById() throws Exception {
        RecordId recordId = mock(RecordId.class);
        remoteRepository.delete(recordId);
        ByteBuffer encodedRecordId = mock(ByteBuffer.class);
        when(avroConverter.convert(recordId)).thenReturn(encodedRecordId);

        remoteRepository.delete(recordId);

        verify(avroLily).delete(encodedRecordId, tenantId, Table.RECORD.name, null, null);
    }

    @Test
    public void testDelete_RecordById_WithMutations() throws Exception {
        RecordId recordId = mock(RecordId.class);
        List<MutationCondition> mutationConditions = Lists.newArrayList(mock(MutationCondition.class));

        ByteBuffer encodedRecordId = mock(ByteBuffer.class);
        List<AvroMutationCondition> encodedMutationConditions = Lists.newArrayList(mock(AvroMutationCondition.class));

        when(avroConverter.convert(recordId)).thenReturn(encodedRecordId);
        when(avroConverter.convert(null, mutationConditions)).thenReturn(encodedMutationConditions);

        remoteRepository.delete(recordId, mutationConditions);

        verify(avroLily).delete(encodedRecordId, tenantId, Table.RECORD.name, encodedMutationConditions, null);
    }

    @Test
    public void testDelete_FullRecord() throws Exception {
        RecordId recordId = mock(RecordId.class);
        Record record = mock(Record.class);
        Map<String, String> attributes = Maps.newHashMap();
        attributes.put("key", "value");

        when(record.getId()).thenReturn(recordId);
        when(record.getAttributes()).thenReturn(attributes);

        ByteBuffer encodedRecordId = mock(ByteBuffer.class);
        when(avroConverter.convert(recordId)).thenReturn(encodedRecordId);

        remoteRepository.delete(record);

        verify(avroLily).delete(encodedRecordId, tenantId, Table.RECORD.name, null, attributes);
    }

}
