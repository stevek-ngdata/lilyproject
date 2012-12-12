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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.lilyproject.repository.api.RepositoryException;

import org.lilyproject.util.repo.RecordEvent.Type;

import org.lilyproject.util.repo.RecordEvent;

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
import org.lilyproject.util.repo.RecordEvent.IndexRecordFilterData;
import org.mockito.Mockito;

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
    }

    @Test
    public void testBeforeUpdate() throws RepositoryException, InterruptedException {
        IndexInfo inclusion = createMockIndexInfo("include", true);
        when(indexesInfo.getIndexInfos()).thenReturn(Lists.newArrayList(inclusion));

        RecordEvent recordEvent = new RecordEvent();
        recordEvent.setType(Type.UPDATE);

        indexFilterHook.beforeUpdate(newRecord, oldRecord, repository, fieldTypes, recordEvent);

        IndexRecordFilterData idxFilterData = recordEvent.getIndexRecordFilterData();
        assertTrue(idxFilterData.getOldRecordExists());
        assertTrue(idxFilterData.getNewRecordExists());
        verify(indexFilterHook).calculateIndexInclusion(oldRecord, newRecord, idxFilterData);
    }

    @Test
    public void testBeforeCreate() throws RepositoryException, InterruptedException {
        IndexInfo inclusion = createMockIndexInfo("include", true);
        when(indexesInfo.getIndexInfos()).thenReturn(Lists.newArrayList(inclusion));

        RecordEvent recordEvent = new RecordEvent();
        recordEvent.setType(Type.CREATE);

        indexFilterHook.beforeCreate(newRecord, repository, fieldTypes, recordEvent);

        IndexRecordFilterData idxFilterData = recordEvent.getIndexRecordFilterData();
        assertFalse(idxFilterData.getOldRecordExists());
        assertTrue(idxFilterData.getNewRecordExists());
        verify(indexFilterHook).calculateIndexInclusion(null, newRecord, idxFilterData);
    }

    @Test
    public void testBeforeDelete() throws RepositoryException, InterruptedException {
        IndexInfo inclusion = createMockIndexInfo("include", true);
        when(indexesInfo.getIndexInfos()).thenReturn(Lists.newArrayList(inclusion));

        RecordEvent recordEvent = new RecordEvent();
        recordEvent.setType(Type.DELETE);

        indexFilterHook.beforeDelete(oldRecord, repository, fieldTypes, recordEvent);

        IndexRecordFilterData idxFilterData = recordEvent.getIndexRecordFilterData();
        assertTrue(idxFilterData.getOldRecordExists());
        assertFalse(idxFilterData.getNewRecordExists());
        verify(indexFilterHook).calculateIndexInclusion(oldRecord, null, idxFilterData);
    }

    @Test
    public void testCalculateIndexInclusion_MoreInclusionsThanExclusions() {
        IndexRecordFilterData indexFilterData = mock(IndexRecordFilterData.class);
        IndexInfo inclusionA = createMockIndexInfo("includeA", true);
        IndexInfo inclusionB = createMockIndexInfo("includeB", true);
        IndexInfo exclusion = createMockIndexInfo("exclude", false);

        when(indexesInfo.getIndexInfos()).thenReturn(Lists.newArrayList(inclusionA, inclusionB, exclusion));

        indexFilterHook.calculateIndexInclusion(oldRecord, newRecord, indexFilterData);

        verify(indexFilterData).setSubscriptionInclusions(ImmutableSet.of("includeA", "includeB"));
    }

    @Test
    public void testCalculateIndexInclusion_MoreExclusionsThanInclusions() {
        IndexRecordFilterData indexFilterData = mock(IndexRecordFilterData.class);
        IndexInfo inclusion = createMockIndexInfo("include", true);
        IndexInfo exclusionA = createMockIndexInfo("excludeA", false);
        IndexInfo exclusionB = createMockIndexInfo("excludeB", false);

        when(this.indexesInfo.getIndexInfos()).thenReturn(Lists.newArrayList(inclusion, exclusionA, exclusionB));

        indexFilterHook.calculateIndexInclusion(oldRecord, newRecord, indexFilterData);

        verify(indexFilterData).setSubscriptionExclusions(ImmutableSet.of("excludeA", "excludeB"));
    }

    @Test
    public void testCalculateIndexInclusion_AllIndexesIncluded() {
        IndexRecordFilterData indexFilterData = mock(IndexRecordFilterData.class);
        IndexInfo inclusion = createMockIndexInfo("include", true);

        when(indexesInfo.getIndexInfos()).thenReturn(Lists.newArrayList(inclusion));

        indexFilterHook.calculateIndexInclusion(oldRecord, newRecord, indexFilterData);

        verify(indexFilterData).setSubscriptionInclusions(IndexRecordFilterData.ALL_INDEX_SUBSCRIPTIONS);
    }

    @Test
    public void testCalculateIndexInclusion_AllIndexesExcluded() {
        IndexRecordFilterData indexFilterData = mock(IndexRecordFilterData.class);
        IndexInfo inclusion = createMockIndexInfo("exclude", false);

        when(indexesInfo.getIndexInfos()).thenReturn(Lists.newArrayList(inclusion));

        indexFilterHook.calculateIndexInclusion(oldRecord, newRecord, indexFilterData);

        verify(indexFilterData).setSubscriptionExclusions(IndexRecordFilterData.ALL_INDEX_SUBSCRIPTIONS);
    }

    @Test
    public void testCalculationInclusion_NoIndexSubscriptions() {
        IndexRecordFilterData indexFilterData = mock(IndexRecordFilterData.class);
        when(indexesInfo.getIndexInfos()).thenReturn(Lists.<IndexInfo>newArrayList());

        indexFilterHook.calculateIndexInclusion(oldRecord, newRecord, indexFilterData);

        verify(indexFilterData).setSubscriptionExclusions(IndexRecordFilterData.ALL_INDEX_SUBSCRIPTIONS);
    }

    private IndexInfo createMockIndexInfo(String queueSubscriptionId, boolean include) {
        IndexInfo indexInfo = mock(IndexInfo.class, Mockito.RETURNS_DEEP_STUBS);
        IndexRecordFilter indexRecordFilter = mock(IndexRecordFilter.class);

        when(indexInfo.getIndexerConf().getRecordFilter()).thenReturn(indexRecordFilter);
        doReturn(include).when(indexFilterHook).indexIsApplicable(indexRecordFilter, oldRecord, newRecord);
        when(indexInfo.getIndexDefinition().getQueueSubscriptionId()).thenReturn(queueSubscriptionId);

        return indexInfo;
    }

    @Test
    public void testIndexIsApplicable_TrueForOldRecord() {
        IndexRecordFilter indexRecordFilter = mock(IndexRecordFilter.class);
        Record oldRecord = mock(Record.class);
        when(indexRecordFilter.getIndexCase(oldRecord)).thenReturn(mock(IndexCase.class));

        assertTrue(indexFilterHook.indexIsApplicable(indexRecordFilter, oldRecord, null));
    }

    @Test
    public void testIndexIsApplicable_FalseForOldRecord() {
        IndexRecordFilter indexRecordFilter = mock(IndexRecordFilter.class);
        Record oldRecord = mock(Record.class);
        when(indexRecordFilter.getIndexCase(oldRecord)).thenReturn(null);

        assertFalse(indexFilterHook.indexIsApplicable(indexRecordFilter, oldRecord, null));
    }

    @Test
    public void testIndexIsApplicable_TrueForNewRecord() {
        IndexRecordFilter indexRecordFilter = mock(IndexRecordFilter.class);
        Record newRecord = mock(Record.class);
        when(indexRecordFilter.getIndexCase(newRecord)).thenReturn(mock(IndexCase.class));

        assertTrue(indexFilterHook.indexIsApplicable(indexRecordFilter, null, newRecord));
    }

    @Test
    public void testIndexIsApplicable_FalseForNewRecord() {
        IndexRecordFilter indexRecordFilter = mock(IndexRecordFilter.class);
        Record newRecord = mock(Record.class);
        when(indexRecordFilter.getIndexCase(newRecord)).thenReturn(null);

        assertFalse(indexFilterHook.indexIsApplicable(indexRecordFilter, null, newRecord));
    }

}
