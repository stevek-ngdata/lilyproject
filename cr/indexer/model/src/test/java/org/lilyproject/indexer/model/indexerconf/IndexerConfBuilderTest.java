/*
 * Copyright 2013 NGDATA nv
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

import java.io.ByteArrayInputStream;
import java.util.List;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.impl.id.IdGeneratorImpl;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndexerConfBuilderTest {

    private TypeManager typeManager;
    private IdGenerator idGenerator;
    private RepositoryManager repositoryManager;

    @Before
    public void setUp() {
        typeManager = mock(TypeManager.class, Mockito.RETURNS_DEEP_STUBS);
        idGenerator = new IdGeneratorImpl();
        repositoryManager = mock(RepositoryManager.class);
        when(repositoryManager.getTypeManager()).thenReturn(typeManager);
        when(repositoryManager.getIdGenerator()).thenReturn(idGenerator);
    }

    private IndexerConf makeIndexerConf(String namespaces, List<String> includes, List<String> excludes) throws IndexerConfException {
        StringBuilder result = new StringBuilder();
        result.append("<indexer ").append(namespaces).append(">\n<recordFilter>\n");

        if (includes.size() > 0) {
            result.append("<includes>\n");
            for (String include : includes) {
                result.append("<include ").append(include).append("/>\n");
            }
            result.append("</includes>\n");
        }

        if (includes.size() > 0) {
            result.append("<excludes>\n");
            for (String exclude : excludes) {
                result.append("<exclude ").append(exclude).append("/>\n");
            }
            result.append("</excludes>\n");
        }

        result.append("</recordFilter>\n</indexer>\n");

        return IndexerConfBuilder.build(new ByteArrayInputStream(result.toString().getBytes()), repositoryManager);
    }

    @Test
    public void testExtractTableNames_SingleTable() {
        assertEquals(Lists.newArrayList("mytable"), IndexerConfBuilder.extractTableNames("mytable"));
    }

    @Test
    public void testExtractTableNames_MultipleTables() {
        assertEquals(
                Lists.newArrayList("table1", "table2", "table3"),
                IndexerConfBuilder.extractTableNames(" table1 , table2, table3"));

    }

    @Test
    public void testExtractTableNames_Null() {
        assertNull(IndexerConfBuilder.extractTableNames(null));
    }

    @Test
    public void testExtractTableNames_NoneGiven() {
        assertNull(IndexerConfBuilder.extractTableNames("  "));
    }

}
