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
package org.lilyproject.indexer.integration;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.lilyproject.indexer.model.indexerconf.IndexCase;
import org.lilyproject.indexer.model.indexerconf.IndexRecordFilter;
import org.lilyproject.indexer.model.util.IndexInfo;
import org.lilyproject.indexer.model.util.IndexesInfo;
import org.lilyproject.repository.api.FieldTypes;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.util.hbase.LilyHBaseSchema.Table;
import org.lilyproject.util.hbase.RepoAndTableUtil;
import org.lilyproject.util.repo.RecordEvent;
import org.lilyproject.util.repo.RecordEvent.IndexRecordFilterData;
import org.lilyproject.util.repo.RecordEvent.Type;
import org.mockito.Mockito;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IndexRecordFilterHookTest {

    private Record oldRecord;
    private Record newRecord;
    private Repository repository;
    private FieldTypes fieldTypes;

    private IndexesInfo indexesInfo;
    private IndexRecordFilterHook indexFilterHook;


    @Before
    public void setUp() {
        oldRecord = mock(Record.class);
        newRecord = mock(Record.class);

        repository = mock(Repository.class);
        fieldTypes = mock(FieldTypes.class);

        indexesInfo = mock(IndexesInfo.class);
        indexFilterHook = spy(new IndexRecordFilterHook(indexesInfo));
        when(repository.getRepositoryName()).thenReturn(RepoAndTableUtil.DEFAULT_REPOSITORY);
    }

    @Test
    public void testBeforeUpdate() throws RepositoryException, InterruptedException {
        IndexInfo inclusion = createMockIndexInfo("include", true);
        when(indexesInfo.getIndexInfos()).thenReturn(Lists.newArrayList(inclusion));

        RecordEvent recordEvent = new RecordEvent();
        recordEvent.setType(Type.UPDATE);
        recordEvent.setTableName(Table.RECORD.name);

        indexFilterHook.beforeUpdate(newRecord, oldRecord, repository, fieldTypes, recordEvent);

        IndexRecordFilterData idxFilterData = recordEvent.getIndexRecordFilterData();
        assertTrue(idxFilterData.getOldRecordExists());
        assertTrue(idxFilterData.getNewRecordExists());
        verify(indexFilterHook).calculateIndexInclusion(RepoAndTableUtil.DEFAULT_REPOSITORY,
                Table.RECORD.name, oldRecord, newRecord, idxFilterData);
    }

    @Test
    public void testBeforeCreate() throws RepositoryException, InterruptedException {
        IndexInfo inclusion = createMockIndexInfo("include", true);
        when(indexesInfo.getIndexInfos()).thenReturn(Lists.newArrayList(inclusion));

        RecordEvent recordEvent = new RecordEvent();
        recordEvent.setType(Type.CREATE);
        recordEvent.setTableName(Table.RECORD.name);

        indexFilterHook.beforeCreate(newRecord, repository, fieldTypes, recordEvent);

        IndexRecordFilterData idxFilterData = recordEvent.getIndexRecordFilterData();
        assertFalse(idxFilterData.getOldRecordExists());
        assertTrue(idxFilterData.getNewRecordExists());
        verify(indexFilterHook).calculateIndexInclusion(RepoAndTableUtil.DEFAULT_REPOSITORY,
                Table.RECORD.name, null, newRecord, idxFilterData);
    }

    @Test
    public void testBeforeDelete() throws RepositoryException, InterruptedException {
        IndexInfo inclusion = createMockIndexInfo("include", true);
        when(indexesInfo.getIndexInfos()).thenReturn(Lists.newArrayList(inclusion));

        RecordEvent recordEvent = new RecordEvent();
        recordEvent.setType(Type.DELETE);
        recordEvent.setTableName(Table.RECORD.name);

        indexFilterHook.beforeDelete(oldRecord, repository, fieldTypes, recordEvent);

        IndexRecordFilterData idxFilterData = recordEvent.getIndexRecordFilterData();
        assertTrue(idxFilterData.getOldRecordExists());
        assertFalse(idxFilterData.getNewRecordExists());
        verify(indexFilterHook).calculateIndexInclusion(RepoAndTableUtil.DEFAULT_REPOSITORY,
                Table.RECORD.name, oldRecord, null, idxFilterData);
    }

    @Test
    public void testCalculateIndexInclusion_MoreInclusionsThanExclusions() {
        IndexRecordFilterData indexFilterData = mock(IndexRecordFilterData.class);
        IndexInfo inclusionA = createMockIndexInfo("includeA", true);
        IndexInfo inclusionB = createMockIndexInfo("includeB", true);
        IndexInfo exclusion = createMockIndexInfo("exclude", false);

        when(indexesInfo.getIndexInfos()).thenReturn(Lists.newArrayList(inclusionA, inclusionB, exclusion));

        indexFilterHook.calculateIndexInclusion(RepoAndTableUtil.DEFAULT_REPOSITORY,
                Table.RECORD.name, oldRecord, newRecord, indexFilterData);

        verify(indexFilterData).setSubscriptionInclusions(ImmutableSet.of("includeA", "includeB"));
    }

    @Test
    public void testCalculateIndexInclusion_MoreExclusionsThanInclusions() {
        IndexRecordFilterData indexFilterData = mock(IndexRecordFilterData.class);
        IndexInfo inclusion = createMockIndexInfo("include", true);
        IndexInfo exclusionA = createMockIndexInfo("excludeA", false);
        IndexInfo exclusionB = createMockIndexInfo("excludeB", false);

        when(this.indexesInfo.getIndexInfos()).thenReturn(Lists.newArrayList(inclusion, exclusionA, exclusionB));

        indexFilterHook.calculateIndexInclusion(RepoAndTableUtil.DEFAULT_REPOSITORY,
                Table.RECORD.name, oldRecord, newRecord, indexFilterData);

        verify(indexFilterData).setSubscriptionExclusions(ImmutableSet.of("excludeA", "excludeB"));
    }

    @Test
    public void testCalculateIndexInclusion_AllIndexesIncluded() {
        IndexRecordFilterData indexFilterData = mock(IndexRecordFilterData.class);
        IndexInfo inclusion = createMockIndexInfo("include", true);

        when(indexesInfo.getIndexInfos()).thenReturn(Lists.newArrayList(inclusion));

        indexFilterHook.calculateIndexInclusion(RepoAndTableUtil.DEFAULT_REPOSITORY,
                Table.RECORD.name, oldRecord, newRecord, indexFilterData);

        verify(indexFilterData).setSubscriptionInclusions(IndexRecordFilterData.ALL_INDEX_SUBSCRIPTIONS);
    }

    @Test
    public void testCalculateIndexInclusion_AllIndexesExcluded() {
        IndexRecordFilterData indexFilterData = mock(IndexRecordFilterData.class);
        IndexInfo inclusion = createMockIndexInfo("exclude", false);

        when(indexesInfo.getIndexInfos()).thenReturn(Lists.newArrayList(inclusion));

        indexFilterHook.calculateIndexInclusion(RepoAndTableUtil.DEFAULT_REPOSITORY,
                Table.RECORD.name, oldRecord, newRecord, indexFilterData);

        verify(indexFilterData).setSubscriptionExclusions(IndexRecordFilterData.ALL_INDEX_SUBSCRIPTIONS);
    }

    @Test
    public void testCalculateIndexInclusion_NoIndexSubscriptions() {
        IndexRecordFilterData indexFilterData = mock(IndexRecordFilterData.class);
        when(indexesInfo.getIndexInfos()).thenReturn(Lists.<IndexInfo>newArrayList());

        indexFilterHook.calculateIndexInclusion(RepoAndTableUtil.DEFAULT_REPOSITORY,
                Table.RECORD.name, oldRecord, newRecord, indexFilterData);

        verify(indexFilterData).setSubscriptionExclusions(IndexRecordFilterData.ALL_INDEX_SUBSCRIPTIONS);
    }

    private IndexInfo createMockIndexInfo(String queueSubscriptionId, boolean include) {
        IndexInfo indexInfo = mock(IndexInfo.class, Mockito.RETURNS_DEEP_STUBS);
        IndexRecordFilter indexRecordFilter = mock(IndexRecordFilter.class);

        when(indexInfo.getIndexerConf().getRecordFilter()).thenReturn(indexRecordFilter);
        doReturn(include).when(indexFilterHook).indexIsApplicable(indexRecordFilter, Table.RECORD.name, oldRecord, newRecord);
        when(indexInfo.getIndexDefinition().getQueueSubscriptionId()).thenReturn(queueSubscriptionId);

        return indexInfo;
    }

    @Test
    public void testIndexIsApplicable_TrueForOldRecord() {
        IndexRecordFilter indexRecordFilter = mock(IndexRecordFilter.class);
        Record oldRecord = mock(Record.class);
        when(indexRecordFilter.getIndexCase(Table.RECORD.name, oldRecord)).thenReturn(mock(IndexCase.class));

        assertTrue(indexFilterHook.indexIsApplicable(indexRecordFilter, Table.RECORD.name, oldRecord, null));
    }

    @Test
    public void testIndexIsApplicable_FalseForOldRecord() {
        IndexRecordFilter indexRecordFilter = mock(IndexRecordFilter.class);
        Record oldRecord = mock(Record.class);
        when(indexRecordFilter.getIndexCase(Table.RECORD.name, oldRecord)).thenReturn(null);

        assertFalse(indexFilterHook.indexIsApplicable(indexRecordFilter, Table.RECORD.name, oldRecord, null));
    }

    @Test
    public void testIndexIsApplicable_TrueForNewRecord() {
        IndexRecordFilter indexRecordFilter = mock(IndexRecordFilter.class);
        Record newRecord = mock(Record.class);
        when(indexRecordFilter.getIndexCase(Table.RECORD.name, newRecord)).thenReturn(mock(IndexCase.class));

        assertTrue(indexFilterHook.indexIsApplicable(indexRecordFilter, Table.RECORD.name, null, newRecord));
    }

    @Test
    public void testIndexIsApplicable_FalseForNewRecord() {
        IndexRecordFilter indexRecordFilter = mock(IndexRecordFilter.class);
        Record newRecord = mock(Record.class);
        when(indexRecordFilter.getIndexCase(Table.RECORD.name, newRecord)).thenReturn(null);

        assertFalse(indexFilterHook.indexIsApplicable(indexRecordFilter, Table.RECORD.name,  null, newRecord));
    }

}
