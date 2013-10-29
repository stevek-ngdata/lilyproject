/*
 * Copyright 2010 Outerthought bvba
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
import java.lang.reflect.UndeclaredThrowableException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.Transceiver;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.lilyproject.avro.AvroAuthzContext;
import org.lilyproject.avro.AvroConverter;
import org.lilyproject.avro.AvroGenericException;
import org.lilyproject.avro.AvroLily;
import org.lilyproject.avro.AvroRepositoryException;
import org.lilyproject.repository.api.BlobManager;
import org.lilyproject.repository.api.IORecordException;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.MutationCondition;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordBuilder;
import org.lilyproject.repository.api.RecordException;
import org.lilyproject.repository.api.RecordFactory;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.TableManager;
import org.lilyproject.repository.impl.AbstractRepositoryManager;
import org.lilyproject.repository.impl.BaseRepository;
import org.lilyproject.repository.impl.RecordBuilderImpl;
import org.lilyproject.repository.impl.RepoTableKey;
import org.lilyproject.repository.spi.AuthorizationContextHolder;
import org.lilyproject.util.io.Closer;

// ATTENTION: when adding new methods, do not forget to add handling for UndeclaredThrowableException! This is
//            necessary because, at the time of this writing, Avro did not include IOException in its generated
//            interfaces.

public class RemoteRepository extends BaseRepository {
    private AvroLily lilyProxy;
    private final AvroConverter converter;
    private Transceiver client;
    private String repositoryName;
    private String tableName;

    public RemoteRepository(RepoTableKey repoTableKey, AvroLilyTransceiver lilyTransceiver, AvroConverter converter,
            AbstractRepositoryManager repositoryManager, BlobManager blobManager, HTableInterface recordTable,
            HTableInterface nonAuthRecordTable, TableManager tableManager, RecordFactory recordFactory)
            throws IOException, InterruptedException {
        super(repoTableKey, repositoryManager, blobManager, recordTable, nonAuthRecordTable, null, tableManager,
                recordFactory);
        this.converter = converter;
        client = lilyTransceiver.getTransceiver();
        lilyProxy = lilyTransceiver.getLilyProxy();
        this.repositoryName = repoTableKey.getRepositoryName();
        this.tableName = repoTableKey.getTableName();
    }

    @Override
    public void close() throws IOException {
        Closer.close(client);
    }

    @Override
    public IdGenerator getIdGenerator() {
        return idGenerator;
    }

    private AvroAuthzContext getAuthzContext() {
        return converter.convert(AuthorizationContextHolder.getCurrentContext());
    }

    @Override
    public Record create(Record record) throws RepositoryException, InterruptedException {
        try {
            return converter.convertRecord(lilyProxy.create(getAuthzContext(), converter.convert(record, this),
                    repositoryName, tableName), this);
        } catch (AvroRepositoryException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw handleAvroRemoteException(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredRecordThrowable(e);
        }
    }

    @Override
    public Record delete(RecordId recordId, List<MutationCondition> conditions)
            throws RepositoryException, InterruptedException {
        try {
            ByteBuffer record = lilyProxy.delete(getAuthzContext(), converter.convert(recordId), repositoryName,
                    tableName, converter.convert(null, conditions, this), null);
            return record == null ? null : converter.convertRecord(record, this);
        } catch (AvroRepositoryException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw handleAvroRemoteException(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredRecordThrowable(e);
        }
    }

    @Override
    public void delete(RecordId recordId) throws RepositoryException, InterruptedException {
        try {
            lilyProxy.delete(getAuthzContext(), converter.convert(recordId), repositoryName, tableName, null, null);
        } catch (AvroRepositoryException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw handleAvroRemoteException(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredRecordThrowable(e);
        }
    }

    @Override
    public void delete(Record record) throws RepositoryException, InterruptedException {
        try {
            lilyProxy.delete(getAuthzContext(), converter.convert(record.getId()), repositoryName, tableName, null,
                    record.getAttributes());
        } catch (AvroRepositoryException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw handleAvroRemoteException(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredRecordThrowable(e);
        }
    }

    @Override
    public Record update(Record record) throws RepositoryException, InterruptedException {
        return update(record, false, true);
    }

    @Override
    public Record update(Record record, List<MutationCondition> conditions)
            throws RepositoryException, InterruptedException {
        return update(record, false, true, conditions);
    }

    @Override
    public Record update(Record record, boolean updateVersion, boolean useLatestRecordType)
            throws RepositoryException, InterruptedException {
        return update(record, updateVersion, useLatestRecordType, null);
    }

    @Override
    public Record update(Record record, boolean updateVersion, boolean useLatestRecordType,
                         List<MutationCondition> conditions) throws RepositoryException, InterruptedException {
        try {
            return converter
                    .convertRecord(lilyProxy.update(getAuthzContext(), converter.convert(record, this), repositoryName,
                            tableName, updateVersion, useLatestRecordType, converter.convert(record, conditions, this)),
                            this);
        } catch (AvroRepositoryException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw handleAvroRemoteException(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredRecordThrowable(e);
        }
    }

    @Override
    public Record createOrUpdate(Record record) throws RepositoryException, InterruptedException {
        return createOrUpdate(record, true);
    }

    @Override
    public Record createOrUpdate(Record record, boolean useLatestRecordType)
            throws RepositoryException, InterruptedException {
        try {
            return converter.convertRecord(lilyProxy.createOrUpdate(getAuthzContext(), converter.convert(record, this),
                    repositoryName, tableName, useLatestRecordType), this);
        } catch (AvroRepositoryException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw handleAvroRemoteException(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredRecordThrowable(e);
        }
    }

    @Override
    public Set<RecordId> getVariants(RecordId recordId) throws RepositoryException, InterruptedException {
        try {
            return converter.convertAvroRecordIds(lilyProxy.getVariants(getAuthzContext(), converter.convert(recordId),
                    repositoryName, tableName), this);
        } catch (AvroRepositoryException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw handleAvroRemoteException(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredRecordThrowable(e);
        }
    }

    private RuntimeException handleUndeclaredRecordThrowable(UndeclaredThrowableException e) throws RecordException {
        if (e.getCause() instanceof IOException) {
            throw new IORecordException(e.getCause());
        } else {
            throw e;
        }
    }

    private RuntimeException handleAvroRemoteException(AvroRemoteException e) throws RecordException {
        // AvroRemoteException's are exceptions which are not declared in the avro protocol and
        // which are not RuntimeException's.
        if (e.getCause() instanceof IOException) {
            throw new IORecordException(e.getCause());
        } else {
            throw converter.convert(e);
        }
    }

    @Override
    public RecordBuilder recordBuilder() throws RecordException {
        return new RecordBuilderImpl(this, getIdGenerator());
    }
}

