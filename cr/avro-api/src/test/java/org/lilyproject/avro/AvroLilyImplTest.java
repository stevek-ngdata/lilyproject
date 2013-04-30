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
package org.lilyproject.avro;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Before;
import org.junit.Test;
import org.lilyproject.indexer.Indexer;
import org.lilyproject.repository.api.LRepository;
import org.lilyproject.repository.api.LTable;
import org.lilyproject.repository.api.MutationCondition;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.util.hbase.LilyHBaseSchema.Table;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AvroLilyImplTest {

    private LRepository repository;
    private LTable table;
    private AvroConverter avroConverter;
    private AvroLilyImpl avroLilyImpl;
    private final static String tenantName = "public";

    @Before
    public void setUp() throws IOException, InterruptedException, RepositoryException {
        RepositoryManager repositoryManager = mock(RepositoryManager.class);
        table = mock(LTable.class);
        Repository repository = mock(Repository.class);
        this.repository = repository;
        when(repository.getTable(Table.RECORD.name)).thenReturn(table);
        when(repositoryManager.getRepository(tenantName)).thenReturn(repository);
        TypeManager typeManager = mock(TypeManager.class);
        Indexer indexer = mock(Indexer.class);
        avroConverter = mock(AvroConverter.class);
        avroLilyImpl = new AvroLilyImpl(repositoryManager, typeManager, indexer);
        avroLilyImpl.setAvroConverter(avroConverter);
    }

    @Test
    public void testDelete_WithAttributes() throws Exception {
        ByteBuffer recordIdBytes = mock(ByteBuffer.class);
        Map<String, String> attributes = Maps.newHashMap();
        attributes.put("atrKey", "atrValue");

        RecordId recordId = mock(RecordId.class);
        when(avroConverter.convertAvroRecordId(recordIdBytes, repository)).thenReturn(recordId);

        Record toDelete = mock(Record.class);
        when(table.newRecord(recordId)).thenReturn(toDelete);

        avroLilyImpl.delete(recordIdBytes, tenantName, Table.RECORD.name, null, attributes);

        verify(toDelete).setAttributes(attributes);
        verify(table).delete(toDelete);
    }

    @Test
    public void testDelete_NoAttributes() throws Exception {
        ByteBuffer recordIdBytes = mock(ByteBuffer.class);
        List<AvroMutationCondition> avroMutationConditions = Lists.newArrayList(mock(AvroMutationCondition.class));

        RecordId recordId = mock(RecordId.class);
        List<MutationCondition> mutationConditions = Lists.newArrayList(mock(MutationCondition.class));

        when(avroConverter.convertAvroRecordId(recordIdBytes, repository)).thenReturn(recordId);
        when(avroConverter.convertFromAvro(avroMutationConditions, repository)).thenReturn(mutationConditions);

        avroLilyImpl.delete(recordIdBytes, tenantName, Table.RECORD.name, avroMutationConditions, null);

        verify(table).delete(recordId, mutationConditions);
    }

    @Test(expected = IllegalStateException.class)
    public void testDelete_MutationConditionsAndAttributesSupplied() throws Exception {
        ByteBuffer recordIdBytes = mock(ByteBuffer.class);
        List<AvroMutationCondition> avroMutationConditions = Lists.newArrayList(mock(AvroMutationCondition.class));
        Map<String, String> attributes = Maps.newHashMap();
        attributes.put("atrKey", "atrValue");

        RecordId recordId = mock(RecordId.class);
        List<MutationCondition> mutationConditions = Lists.newArrayList(mock(MutationCondition.class));

        when(avroConverter.convertAvroRecordId(recordIdBytes, repository)).thenReturn(recordId);
        when(avroConverter.convertFromAvro(avroMutationConditions, repository)).thenReturn(mutationConditions);

        avroLilyImpl.delete(recordIdBytes, tenantName, Table.RECORD.name, avroMutationConditions, attributes);
    }

}
