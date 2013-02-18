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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Before;
import org.junit.Test;
import org.lilyproject.indexer.Indexer;
import org.lilyproject.repository.api.MutationCondition;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.util.hbase.LilyHBaseSchema.Table;

public class AvroLilyImplTest {

    private RepositoryManager repositoryManager;
    private Repository repository;
    private TypeManager typeManager;
    private Indexer indexer;
    private AvroConverter avroConverter;
    private AvroLilyImpl avroLilyImpl;

    @Before
    public void setUp() throws IOException, InterruptedException {
        repositoryManager = mock(RepositoryManager.class);
        repository = mock(Repository.class);
        when(repositoryManager.getRepository(Table.RECORD.name)).thenReturn(repository);
        typeManager = mock(TypeManager.class);
        indexer = mock(Indexer.class);
        avroConverter = mock(AvroConverter.class);
        avroLilyImpl = new AvroLilyImpl(repositoryManager, typeManager, indexer);
        avroLilyImpl.setAvroConverter(avroConverter);
    }
    
// TODO repository re-enable
//
//    @Test
//    public void testDelete_WithAttributes() throws Exception {
//        ByteBuffer recordIdBytes = mock(ByteBuffer.class);
//        Map<String, String> attributes = Maps.newHashMap();
//        attributes.put("atrKey", "atrValue");
//
//        RecordId recordId = mock(RecordId.class);
//        when(avroConverter.convertAvroRecordId(recordIdBytes)).thenReturn(recordId);
//
//        Record toDelete = mock(Record.class);
//        when(repository.newRecord(recordId)).thenReturn(toDelete);
//
//        avroLilyImpl.delete(recordIdBytes, ByteBuffer.wrap(Table.RECORD.bytes), null, attributes);
//
//        verify(toDelete).setAttributes(attributes);
//        verify(repository).delete(toDelete);
//    }
//
//    @Test
//    public void testDelete_NoAttributes() throws Exception {
//        ByteBuffer recordIdBytes = mock(ByteBuffer.class);
//        List<AvroMutationCondition> avroMutationConditions = Lists.newArrayList(mock(AvroMutationCondition.class));
//
//        RecordId recordId = mock(RecordId.class);
//        List<MutationCondition> mutationConditions = Lists.newArrayList(mock(MutationCondition.class));
//
//        when(avroConverter.convertAvroRecordId(recordIdBytes)).thenReturn(recordId);
//        when(avroConverter.convertFromAvro(avroMutationConditions)).thenReturn(mutationConditions);
//
//        avroLilyImpl.delete(recordIdBytes, ByteBuffer.wrap(Table.RECORD.bytes), avroMutationConditions, null);
//
//        verify(repository).delete(recordId, mutationConditions);
//    }
//
//    @Test(expected = IllegalStateException.class)
//    public void testDelete_MutationConditionsAndAttributesSupplied() throws Exception {
//        ByteBuffer recordIdBytes = mock(ByteBuffer.class);
//        List<AvroMutationCondition> avroMutationConditions = Lists.newArrayList(mock(AvroMutationCondition.class));
//        Map<String, String> attributes = Maps.newHashMap();
//        attributes.put("atrKey", "atrValue");
//
//        RecordId recordId = mock(RecordId.class);
//        List<MutationCondition> mutationConditions = Lists.newArrayList(mock(MutationCondition.class));
//
//        when(avroConverter.convertAvroRecordId(recordIdBytes)).thenReturn(recordId);
//        when(avroConverter.convertFromAvro(avroMutationConditions)).thenReturn(mutationConditions);
//
//        avroLilyImpl.delete(recordIdBytes, ByteBuffer.wrap(Table.RECORD.bytes), avroMutationConditions, attributes);
//    }

}
