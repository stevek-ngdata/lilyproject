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
package org.lilyproject.repository.impl;

import java.io.IOException;

import org.lilyproject.repository.api.BlobManager;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.RecordFactory;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.util.hbase.HBaseTableFactory;

public class HBaseRepositoryManager extends AbstractRepositoryManager {

    private HBaseTableFactory hbaseTableFactory;
    private BlobManager blobManager;
    
    public HBaseRepositoryManager(TypeManager typeManager, IdGenerator idGenerator, RecordFactory recordFactory, HBaseTableFactory hbaseTableFactory, BlobManager blobManager) {
        super(typeManager, idGenerator, recordFactory);
        this.hbaseTableFactory = hbaseTableFactory;
        this.blobManager = blobManager;
    }
    
    @Override
    protected Repository createRepository(String tableName) throws IOException, InterruptedException {
        return new HBaseRepository(this, hbaseTableFactory, blobManager);
    }

}
