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
package org.lilyproject.repository.impl.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.lilyproject.repository.api.TableManager;
import org.lilyproject.repository.impl.TableManagerImpl;
import org.lilyproject.util.hbase.HBaseTableFactory;
import org.lilyproject.util.hbase.LilyHBaseSchema.Table;

import static org.mockito.Mockito.mock;

public class RepositoryTableManagerImplTest {

    private Configuration configuration;
    private HBaseTableFactory tableFactory;
    private TableManager tableManager;

    @Before
    public void setUp() {
        configuration = new Configuration();
        tableFactory = mock(HBaseTableFactory.class);
        tableManager = new TableManagerImpl(/* TODO multiple repositories */ "default", configuration, tableFactory);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testDeleteTable_RecordTable() throws InterruptedException, IOException {
        tableManager.dropTable(Table.RECORD.name);
    }

}
