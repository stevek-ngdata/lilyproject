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

import org.lilyproject.avro.AvroConverter;
import org.lilyproject.repository.api.BlobManager;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.RecordFactory;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.repository.api.TableManager;
import org.lilyproject.repository.impl.AbstractRepositoryManager;
import org.lilyproject.repository.impl.TenantTableKey;
import org.lilyproject.repository.impl.TracingRepository;
import org.lilyproject.tenant.model.api.TenantModel;
import org.lilyproject.util.hbase.HBaseTableFactory;
import org.lilyproject.util.hbase.LilyHBaseSchema;

public class RemoteRepositoryManager extends AbstractRepositoryManager implements RepositoryManager {

    private AvroLilyTransceiver transceiver;
    private AvroConverter avroConverter;
    private BlobManager blobManager;
    private HBaseTableFactory tableFactory;

    public RemoteRepositoryManager(RemoteTypeManager typeManager, IdGenerator idGenerator, RecordFactory recordFactory,
            AvroLilyTransceiver transceiver, AvroConverter avroConverter, BlobManager blobManager,
            HBaseTableFactory tableFactory, TenantModel tenantModel) {
        super(typeManager, idGenerator, recordFactory, tenantModel);
        this.transceiver = transceiver;
        this.avroConverter = avroConverter;
        this.blobManager = blobManager;
        this.tableFactory = tableFactory;
    }

    @Override
    protected Repository createRepository(TenantTableKey key) throws IOException, InterruptedException {
        String hbaseTableName = key.toHBaseTableName();
        TableManager tableManager = new RemoteTableManager(key.getTenantName(), transceiver, avroConverter);
        Repository repo = new RemoteRepository(key, transceiver, avroConverter, this, blobManager,
                LilyHBaseSchema.getRecordTable(tableFactory, hbaseTableName, true), tableManager, getRecordFactory());
        if ("true".equals(System.getProperty("lilyclient.trace"))) {
            repo = TracingRepository.wrap(repo);
        }
        return repo;
    }

}
