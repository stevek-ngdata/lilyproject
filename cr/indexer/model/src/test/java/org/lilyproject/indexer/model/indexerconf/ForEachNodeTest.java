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
package org.lilyproject.indexer.model.indexerconf;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.util.repo.SystemFields;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ForEachNodeTest {

    private static final QName USERDEFINED_FIELDTYPE_NAME = new QName("_namespace_", "_fieldtypename_");
    private static final QName SYSTEM_FIELDTYPE_NAME = new QName("_system_", "_systemfieldtypename_");
    private static final SchemaId USERDEFINED_FIELDTYPE_ID = mock(SchemaId.class);
    private static final SchemaId SYSTEM_FIELDTYPE_ID = mock(SchemaId.class);

    private SystemFields systemFields;
    private IndexUpdateBuilder indexUpdateBuilder;
    private RecordContext recordContext;
    private Record recordContextRecord;
    private FieldType systemFieldType;
    private FieldType userDefinedFieldType;

    @Before
    public void setUp() {
        systemFields = mock(SystemFields.class);
        when(systemFields.isSystemField(SYSTEM_FIELDTYPE_NAME)).thenReturn(true);
        when(systemFields.isSystemField(USERDEFINED_FIELDTYPE_NAME)).thenReturn(false);

        indexUpdateBuilder = mock(IndexUpdateBuilder.class);
        recordContextRecord = mock(Record.class);
        recordContext = new RecordContext(recordContextRecord, mock(Dep.class));
        when(indexUpdateBuilder.getRecordContext()).thenReturn(recordContext);

        userDefinedFieldType = mock(FieldType.class);
        when(userDefinedFieldType.getName()).thenReturn(USERDEFINED_FIELDTYPE_NAME);
        when(userDefinedFieldType.getId()).thenReturn(USERDEFINED_FIELDTYPE_ID);

        systemFieldType = mock(FieldType.class);
        when(systemFieldType.getName()).thenReturn(SYSTEM_FIELDTYPE_NAME);
        when(systemFieldType.getId()).thenReturn(SYSTEM_FIELDTYPE_ID);
    }

    @Test
    public void testCollectIndexUpdate_LinkedRecordFieldType() throws InterruptedException, RepositoryException, IOException {

        when(recordContextRecord.hasField(USERDEFINED_FIELDTYPE_NAME)).thenReturn(true);

        LinkFieldFollow linkFieldFollow = mock(LinkFieldFollow.class);
        when(linkFieldFollow.getFieldType()).thenReturn(userDefinedFieldType);
        ForEachNode forEachNode = new ForEachNode(systemFields, linkFieldFollow);

        forEachNode.collectIndexUpdate(indexUpdateBuilder);

        verify(indexUpdateBuilder).addDependency(USERDEFINED_FIELDTYPE_ID);
        verify(linkFieldFollow).follow(eq(indexUpdateBuilder), any(FollowCallback.class));
    }

    @Test
    public void testCollectIndexUpdate_LinkedRecordFieldType_FieldIsNotWithinRecordContextRecord()
            throws InterruptedException, RepositoryException, IOException {

        when(recordContextRecord.hasField(USERDEFINED_FIELDTYPE_NAME)).thenReturn(false);

        LinkFieldFollow linkFieldFollow = mock(LinkFieldFollow.class);
        when(linkFieldFollow.getFieldType()).thenReturn(userDefinedFieldType);
        ForEachNode forEachNode = new ForEachNode(systemFields, linkFieldFollow);

        forEachNode.collectIndexUpdate(indexUpdateBuilder);

        verify(indexUpdateBuilder).addDependency(USERDEFINED_FIELDTYPE_ID);
        verify(linkFieldFollow, never()).follow(eq(indexUpdateBuilder), any(FollowCallback.class));
    }

    @Test
    public void testCollectIndexUpdate_EmbeddedRecordFieldType() throws InterruptedException, RepositoryException, IOException {

        when(recordContextRecord.hasField(USERDEFINED_FIELDTYPE_NAME)).thenReturn(true);

        RecordFieldFollow recordFieldFollow = mock(RecordFieldFollow.class);
        when(recordFieldFollow.getFieldType()).thenReturn(userDefinedFieldType);
        ForEachNode forEachNode = new ForEachNode(systemFields, recordFieldFollow);

        forEachNode.collectIndexUpdate(indexUpdateBuilder);

        verify(indexUpdateBuilder).addDependency(USERDEFINED_FIELDTYPE_ID);
        verify(recordFieldFollow).follow(eq(indexUpdateBuilder), any(FollowCallback.class));
    }

    @Test
    public void testCollectIndexUpdate_EmbeddedRecordFieldType_FieldIsNotWithinRecordContextRecord()
            throws InterruptedException, RepositoryException, IOException {

        when(recordContextRecord.hasField(USERDEFINED_FIELDTYPE_NAME)).thenReturn(false);

        RecordFieldFollow recordFieldFollow = mock(RecordFieldFollow.class);
        when(recordFieldFollow.getFieldType()).thenReturn(userDefinedFieldType);
        ForEachNode forEachNode = new ForEachNode(systemFields, recordFieldFollow);

        forEachNode.collectIndexUpdate(indexUpdateBuilder);

        verify(indexUpdateBuilder).addDependency(USERDEFINED_FIELDTYPE_ID);
        verify(recordFieldFollow, never()).follow(eq(indexUpdateBuilder), any(FollowCallback.class));
    }

}
