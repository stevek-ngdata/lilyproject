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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.lilyproject.repository.api.BlobManager;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.RecordFactory;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.TableManager;
import org.lilyproject.repository.api.TableNotFoundException;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.tenant.model.api.RepositoryModel;
import org.lilyproject.util.hbase.HBaseTableFactory;
import org.lilyproject.util.hbase.LilyHBaseSchema;

public class HBaseRepositoryManager extends AbstractRepositoryManager {

    private HBaseTableFactory hbaseTableFactory;
    private BlobManager blobManager;
    private Configuration hbaseConf;

    public HBaseRepositoryManager(TypeManager typeManager, IdGenerator idGenerator, RecordFactory recordFactory,
            HBaseTableFactory hbaseTableFactory, BlobManager blobManager, Configuration hbaseConf,
            RepositoryModel repositoryModel) {
        super(typeManager, idGenerator, recordFactory, repositoryModel);
        this.hbaseTableFactory = hbaseTableFactory;
        this.blobManager = blobManager;
        this.hbaseConf = hbaseConf;
    }

    @Override
    protected Repository createRepository(RepoTableKey key) throws InterruptedException, RepositoryException {
        TableManager tableManager = new TableManagerImpl(key.getRepositoryName(), hbaseConf, hbaseTableFactory);
        try {
            HTableInterface htable = LilyHBaseSchema.getRecordTable(hbaseTableFactory, key.toHBaseTableName(), true);
            return new HBaseRepository(key, this, htable, blobManager, tableManager, getRecordFactory());
        } catch (org.apache.hadoop.hbase.TableNotFoundException e) {
            throw new TableNotFoundException(key.getRepositoryName(), key.getTableName());
        } catch (IOException e) {
            throw new RepositoryException(e);
        }
    }

}
