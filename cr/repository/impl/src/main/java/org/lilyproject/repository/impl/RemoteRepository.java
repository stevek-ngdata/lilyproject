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
package org.lilyproject.repository.impl;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.lilyproject.repository.api.*;
import org.lilyproject.repository.avro.*;
import org.lilyproject.util.ArgumentValidator;
import org.lilyproject.util.io.Closer;

// ATTENTION: when adding new methods, do not forget to add handling for UndeclaredThrowableException! This is
//            necessary because, at the time of this writing, Avro did not include IOException in its generated
//            interfaces.

public class RemoteRepository extends BaseRepository {
    private AvroLily lilyProxy;
    private final AvroConverter converter;
    private Transceiver client;

    public RemoteRepository(InetSocketAddress address, AvroConverter converter, RemoteTypeManager typeManager,
            IdGenerator idGenerator, BlobManager blobManager) throws IOException {
        super(typeManager, blobManager, idGenerator);

        this.converter = converter;

        //client = new HttpTransceiver(new URL("http://" + address.getHostName() + ":" + address.getPort() + "/"));
        client = NettyTransceiverFactory.create(address);

        lilyProxy = SpecificRequestor.getClient(AvroLily.class, client);
    }

    public void close() throws IOException {
        Closer.close(typeManager);
        Closer.close(client);
    }

    public IdGenerator getIdGenerator() {
        return idGenerator;
    }
    
    public Record create(Record record) throws RepositoryException, InterruptedException {
        try {
            return converter.convert(lilyProxy.create(converter.convert(record)));
        } catch (AvroRepositoryException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredRecordThrowable(e);
        }
    }

    @Override
    public Record delete(RecordId recordId, List<MutationCondition> conditions)
            throws RepositoryException, InterruptedException {
        try {
            AvroRecord record = lilyProxy.delete(converter.convert(recordId), converter.convert(null, conditions));
            return record == null ? null : converter.convert(record);
        } catch (AvroRepositoryException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredRecordThrowable(e);
        }
    }

    public void delete(RecordId recordId) throws RepositoryException, InterruptedException {
        try {
            lilyProxy.delete(converter.convert(recordId), null);
        } catch (AvroRepositoryException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredRecordThrowable(e);
        }
    }

    public Record read(RecordId recordId, List<QName> fieldNames) throws RepositoryException, InterruptedException {
        return read(recordId, fieldNames == null ? null : fieldNames.toArray(new QName[fieldNames.size()]));
    }
    
    public Record read(RecordId recordId, QName... fieldNames) throws RepositoryException, InterruptedException {
        return read(recordId, null, fieldNames);
    }
    
    public List<Record> read(List<RecordId> recordIds, List<QName> fieldNames) throws RepositoryException, InterruptedException {
        return read(recordIds, fieldNames == null ? null : fieldNames.toArray(new QName[fieldNames.size()]));
    }
    
    public List<Record> read(List<RecordId> recordIds, QName... fieldNames) throws RepositoryException, InterruptedException {
        ArgumentValidator.notNull(recordIds, "recordIds");
        if (recordIds == null)
            return new ArrayList<Record>();
        try {
            List<ByteBuffer> avroRecordIds = new ArrayList<ByteBuffer>();
            for (RecordId recordId : recordIds) {
                avroRecordIds.add(converter.convert(recordId));
            }
            List<AvroQName> avroFieldNames = null;
            if (fieldNames != null) {
                avroFieldNames = new ArrayList<AvroQName>(fieldNames.length);
                for (QName fieldName : fieldNames) {
                    avroFieldNames.add(converter.convert(fieldName));
                }
            }
            return converter.convertAvroRecords(lilyProxy.readRecords(avroRecordIds, avroFieldNames));
        } catch (AvroRepositoryException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredRecordThrowable(e);
        }
    }

    public Record read(RecordId recordId, Long version, List<QName> fieldNames) throws RepositoryException, InterruptedException {
        return read(recordId, version, fieldNames == null ? null : fieldNames.toArray(new QName[fieldNames.size()]));
    }
    
    public Record read(RecordId recordId, Long version, QName... fieldNames) throws RepositoryException, InterruptedException {
        try {
            List<AvroQName> avroFieldNames = null;
            if (fieldNames != null) {
                avroFieldNames = new ArrayList<AvroQName>(fieldNames.length);
                for (QName fieldName : fieldNames) {
                    avroFieldNames.add(converter.convert(fieldName));
                }
            }
            return converter.convert(lilyProxy.read(converter.convert(recordId), converter.convertVersion(version), avroFieldNames));
        } catch (AvroRepositoryException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredRecordThrowable(e);
        }
    }

    public List<Record> readVersions(RecordId recordId, Long fromVersion, Long toVersion, List<QName> fieldNames) throws RepositoryException, InterruptedException {
        return readVersions(recordId, fromVersion, toVersion, fieldNames == null ? null : fieldNames.toArray(new QName[fieldNames.size()]));
    }
    
    public List<Record> readVersions(RecordId recordId, Long fromVersion, Long toVersion, QName... fieldNames) throws RepositoryException, InterruptedException {
        try {
            List<AvroQName> avroFieldNames = null;
            if (fieldNames != null) {
                avroFieldNames = new ArrayList<AvroQName>(fieldNames.length);
                for (QName fieldName : fieldNames) {
                    avroFieldNames.add(converter.convert(fieldName));
                }
            }
            return converter.convertAvroRecords(lilyProxy.readVersions(converter.convert(recordId), converter.convertVersion(fromVersion), converter.convertVersion(toVersion), avroFieldNames));
        } catch (AvroRepositoryException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredRecordThrowable(e);
        }
    }

    public List<Record> readVersions(RecordId recordId, List<Long> versions, List<QName> fieldNames)
    throws RepositoryException, InterruptedException {
        return readVersions(recordId, versions, fieldNames == null ? null : fieldNames.toArray(new QName[fieldNames.size()]));
    }
        
    public List<Record> readVersions(RecordId recordId, List<Long> versions, QName... fieldNames)
    throws RepositoryException, InterruptedException {
        try {
            List<AvroQName> avroFieldNames = null;
            if (fieldNames != null) {
                avroFieldNames = new ArrayList<AvroQName>(fieldNames.length);
                for (QName fieldName : fieldNames) {
                    avroFieldNames.add(converter.convert(fieldName));
                }
            }
            return converter.convertAvroRecords(lilyProxy.readSpecificVersions(converter.convert(recordId), versions, avroFieldNames));
        } catch (AvroRepositoryException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredRecordThrowable(e);
        }
    }
    
    public Record update(Record record) throws RepositoryException, InterruptedException {
        return update(record, false, true);
    }

    @Override
    public Record update(Record record, List<MutationCondition> conditions)
            throws RepositoryException, InterruptedException {
        return update(record, false, true, conditions);
    }

    public Record update(Record record, boolean updateVersion, boolean useLatestRecordType)
            throws RepositoryException, InterruptedException {
        return update(record, updateVersion, useLatestRecordType, null);
    }

    public Record update(Record record, boolean updateVersion, boolean useLatestRecordType,
            List<MutationCondition> conditions) throws RepositoryException, InterruptedException {
        try {
            return converter.convert(lilyProxy.update(converter.convert(record), updateVersion, useLatestRecordType,
                    converter.convert(record, conditions)));
        } catch (AvroRepositoryException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredRecordThrowable(e);
        }
    }

    public Record createOrUpdate(Record record) throws RepositoryException, InterruptedException {
        return createOrUpdate(record, true);
    }

    public Record createOrUpdate(Record record, boolean useLatestRecordType) throws RepositoryException, InterruptedException {
        try {
            return converter.convert(lilyProxy.createOrUpdate(converter.convert(record), useLatestRecordType));
        } catch (AvroRepositoryException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredRecordThrowable(e);
        }
    }

    public Set<RecordId> getVariants(RecordId recordId) throws RepositoryException, InterruptedException {
        try {
            return converter.convertAvroRecordIds(lilyProxy.getVariants(converter.convert(recordId)));
        } catch (AvroRepositoryException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredRecordThrowable(e);
        }
    }
    
    public IdRecord readWithIds(RecordId recordId, Long version, List<SchemaId> fieldIds) throws RepositoryException, InterruptedException {
        try {
            List<AvroSchemaId> avroFieldIds = null;
            if (fieldIds != null) {
                avroFieldIds = new ArrayList<AvroSchemaId>(fieldIds.size());
                for (SchemaId fieldId : fieldIds) {
                    avroFieldIds.add(converter.convert(fieldId));
                }
            }
            return converter.convert(lilyProxy.readWithIds(converter.convert(recordId), converter.convertVersion(version), avroFieldIds));
        } catch (AvroRepositoryException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
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
    
    public RecordBuilder recordBuilder() throws RecordException {
        return new RecordBuilderImpl(this);
    }
}

