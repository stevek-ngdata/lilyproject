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
import java.io.OutputStream;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.lilyproject.repository.api.Blob;
import org.lilyproject.repository.api.BlobException;
import org.lilyproject.repository.api.BlobInputStream;
import org.lilyproject.repository.api.BlobManager;
import org.lilyproject.repository.api.BlobNotFoundException;
import org.lilyproject.repository.api.BlobStoreAccess;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.FieldTypeNotFoundException;
import org.lilyproject.repository.api.IORecordException;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.IdRecord;
import org.lilyproject.repository.api.InvalidRecordException;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordException;
import org.lilyproject.repository.api.RecordExistsException;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.RecordLockedException;
import org.lilyproject.repository.api.RecordNotFoundException;
import org.lilyproject.repository.api.RecordTypeNotFoundException;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.TypeException;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.api.VersionNotFoundException;
import org.lilyproject.repository.avro.AvroConverter;
import org.lilyproject.repository.avro.AvroFieldTypeNotFoundException;
import org.lilyproject.repository.avro.AvroGenericException;
import org.lilyproject.repository.avro.AvroInvalidRecordException;
import org.lilyproject.repository.avro.AvroLily;
import org.lilyproject.repository.avro.AvroQName;
import org.lilyproject.repository.avro.AvroRecordException;
import org.lilyproject.repository.avro.AvroRecordExistsException;
import org.lilyproject.repository.avro.AvroRecordLockedException;
import org.lilyproject.repository.avro.AvroRecordNotFoundException;
import org.lilyproject.repository.avro.AvroRecordTypeNotFoundException;
import org.lilyproject.repository.avro.AvroRepositoryException;
import org.lilyproject.repository.avro.AvroTypeException;
import org.lilyproject.repository.avro.AvroVersionNotFoundException;
import org.lilyproject.util.ArgumentValidator;
import org.lilyproject.util.io.Closer;

// ATTENTION: when adding new methods, do not forget to add handling for UndeclaredThrowableException! This is
//            necessary because, at the time of this writing, Avro did not include IOException in its generated
//            interfaces.

public class RemoteRepository implements Repository {
    private AvroLily lilyProxy;
    private final AvroConverter converter;
    private IdGenerator idGenerator;
    private final TypeManager typeManager;
    private Transceiver client;
    private final BlobManager blobManager;

    public RemoteRepository(InetSocketAddress address, AvroConverter converter, RemoteTypeManager typeManager,
            IdGenerator idGenerator, BlobManager blobManager) throws IOException {        
        this.converter = converter;
        this.typeManager = typeManager;
        this.idGenerator = idGenerator;
        this.blobManager = blobManager;
        
        //client = new HttpTransceiver(new URL("http://" + address.getHostName() + ":" + address.getPort() + "/"));
        client = new NettyTransceiver(address);

        lilyProxy = SpecificRequestor.getClient(AvroLily.class, client);
    }

    public void close() throws IOException {
        Closer.close(typeManager);
        Closer.close(client);
    }

    public TypeManager getTypeManager() {
        return typeManager;
    }
    
    public Record newRecord() {
        return new RecordImpl();
    }

    public Record newRecord(RecordId recordId) {
        ArgumentValidator.notNull(recordId, "recordId");
        return new RecordImpl(recordId);
    }
    
    public IdGenerator getIdGenerator() {
        return idGenerator;
    }
    
    public Record create(Record record) throws RecordExistsException, InvalidRecordException,
            RecordTypeNotFoundException, FieldTypeNotFoundException, RecordLockedException, RecordException,
            TypeException {
        try {
            return converter.convert(lilyProxy.create(converter.convert(record)));
        } catch (AvroRecordExistsException e) {
            throw converter.convert(e);
        } catch (AvroInvalidRecordException e) {
            throw converter.convert(e);
        } catch (AvroRecordTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroFieldTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroRecordLockedException e) {
            throw converter.convert(e);
        } catch (AvroRecordException e) {
            throw converter.convert(e);
        } catch (AvroTypeException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredRecordThrowable(e);
        }
    }

    public void delete(RecordId recordId) throws RecordException, RecordNotFoundException, RecordLockedException {
        try {
            lilyProxy.delete(converter.convert(recordId));
        } catch (AvroRecordNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroRecordLockedException e) {
            throw converter.convert(e);
        } catch (AvroRecordException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredRecordThrowable(e);
        }
    }

    public Record read(RecordId recordId) throws RecordNotFoundException, RecordTypeNotFoundException,
            FieldTypeNotFoundException, VersionNotFoundException, RecordException, TypeException {
        return read(recordId, null, null);
    }

    public Record read(RecordId recordId, List<QName> fieldNames) throws RecordNotFoundException,
            RecordTypeNotFoundException, FieldTypeNotFoundException, VersionNotFoundException, RecordException,
            TypeException {
        return read(recordId, null, fieldNames);
    }

    public Record read(RecordId recordId, Long version) throws RecordNotFoundException, RecordTypeNotFoundException,
            FieldTypeNotFoundException, VersionNotFoundException, RecordException, TypeException {
        return read(recordId, version, null);
    }

    public Record read(RecordId recordId, Long version, List<QName> fieldNames) throws RecordNotFoundException,
            RecordTypeNotFoundException, FieldTypeNotFoundException, VersionNotFoundException, RecordException,
            TypeException {
        try {
            List<AvroQName> avroFieldNames = null;
            if (fieldNames != null) {
                avroFieldNames = new ArrayList<AvroQName>(fieldNames.size());
                for (QName fieldName : fieldNames) {
                    avroFieldNames.add(converter.convert(fieldName));
                }
            }
            return converter.convert(lilyProxy.read(converter.convert(recordId), converter.convertVersion(version), avroFieldNames));
        } catch (AvroRecordNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroVersionNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroRecordTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroFieldTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroRecordException e) {
            throw converter.convert(e);
        } catch (AvroTypeException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredRecordThrowable(e);
        }
    }
    
    public List<Record> readVersions(RecordId recordId, Long fromVersion, Long toVersion, List<QName> fieldNames)
            throws RecordNotFoundException, RecordTypeNotFoundException, FieldTypeNotFoundException, RecordException,
            VersionNotFoundException, TypeException {
        try {
            List<AvroQName> avroFieldNames = null;
            if (fieldNames != null) {
                avroFieldNames = new ArrayList<AvroQName>(fieldNames.size());
                for (QName fieldName : fieldNames) {
                    avroFieldNames.add(converter.convert(fieldName));
                }
            }
            return converter.convertAvroRecords(lilyProxy.readVersions(converter.convert(recordId), converter.convertVersion(fromVersion), converter.convertVersion(toVersion), avroFieldNames));
        } catch (AvroRecordNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroVersionNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroRecordTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroFieldTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroRecordException e) {
            throw converter.convert(e);
        } catch (AvroTypeException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredRecordThrowable(e);
        }
    }
    
    public Record update(Record record) throws RecordNotFoundException, InvalidRecordException,
            RecordTypeNotFoundException, FieldTypeNotFoundException, RecordLockedException, RecordException,
            TypeException, VersionNotFoundException {
        return update(record, false, true);
    }

    public Record update(Record record, boolean updateVersion, boolean useLatestRecordType) throws
            RecordNotFoundException, InvalidRecordException, RecordTypeNotFoundException, FieldTypeNotFoundException,
            RecordLockedException, VersionNotFoundException, RecordException, TypeException {
        try {
            return converter.convert(lilyProxy.update(converter.convert(record), updateVersion, useLatestRecordType));
        } catch (AvroRecordNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroInvalidRecordException e) {
            throw converter.convert(e);
        } catch (AvroRecordTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroFieldTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroRecordLockedException e) {
            throw converter.convert(e);
        } catch (AvroVersionNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroRecordException e) {
            throw converter.convert(e);
        } catch (AvroTypeException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredRecordThrowable(e);
        }
    }

    public Record createOrUpdate(Record record) throws FieldTypeNotFoundException, RecordException,
            RecordTypeNotFoundException, RecordLockedException, InvalidRecordException, TypeException,
            VersionNotFoundException {
        return createOrUpdate(record, true);
    }

    public Record createOrUpdate(Record record, boolean useLatestRecordType) throws FieldTypeNotFoundException,
            RecordException, RecordTypeNotFoundException, RecordLockedException, InvalidRecordException, TypeException,
            VersionNotFoundException {
        try {
            return converter.convert(lilyProxy.createOrUpdate(converter.convert(record), useLatestRecordType));
        } catch (AvroInvalidRecordException e) {
            throw converter.convert(e);
        } catch (AvroRecordTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroFieldTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroRecordLockedException e) {
            throw converter.convert(e);
        } catch (AvroVersionNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroRecordException e) {
            throw converter.convert(e);
        } catch (AvroTypeException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredRecordThrowable(e);
        }
    }

    public Set<RecordId> getVariants(RecordId recordId) throws RepositoryException {
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
    
    public IdRecord readWithIds(RecordId recordId, Long version, List<String> fieldIds) throws RecordNotFoundException,
            VersionNotFoundException, RecordTypeNotFoundException, FieldTypeNotFoundException, RecordException,
            TypeException {
        try {
            List<CharSequence> avroFieldIds = null;
            if (fieldIds != null) {
                avroFieldIds = new ArrayList<CharSequence>(fieldIds.size());
                for (String fieldId : fieldIds) {
                    avroFieldIds.add(fieldId);
                }
            }
            return converter.convert(lilyProxy.readWithIds(converter.convert(recordId), converter.convertVersion(version), avroFieldIds));
        } catch (AvroRecordNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroVersionNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroRecordTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroFieldTypeNotFoundException e) {
            throw converter.convert(e);
        } catch (AvroRecordException e) {
            throw converter.convert(e);
        } catch (AvroTypeException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredRecordThrowable(e);
        }
    }

    public void registerBlobStoreAccess(BlobStoreAccess blobStoreAccess) {
        blobManager.register(blobStoreAccess);
    }
    
    public BlobInputStream getInputStream(RecordId recordId, Long version, QName fieldName, Integer multivalueIndex, Integer hierarchyIndex) throws BlobNotFoundException, BlobException, RecordNotFoundException, RecordTypeNotFoundException, FieldTypeNotFoundException, RecordException, VersionNotFoundException, TypeException, InterruptedException {
        Record record = read(recordId, version, Arrays.asList(new QName[]{fieldName}));
        FieldType fieldType = typeManager.getFieldTypeByName(fieldName);
        return blobManager.getInputStream(record, fieldName, multivalueIndex, hierarchyIndex, fieldType);
    }
    
    public BlobInputStream getInputStream(RecordId recordId, QName fieldName) throws BlobNotFoundException, BlobException, RecordNotFoundException, RecordTypeNotFoundException, FieldTypeNotFoundException, RecordException, VersionNotFoundException, TypeException, InterruptedException {
        return getInputStream(recordId, null, fieldName, null, null);
    }
    
    public OutputStream getOutputStream(Blob blob) throws BlobException {
        return blobManager.getOutputStream(blob);
    }
    
    private RuntimeException handleUndeclaredRecordThrowable(UndeclaredThrowableException e) throws RecordException {
        if (e.getCause() instanceof IOException) {
            throw new IORecordException(e.getCause());
        } else {
            throw e;
        }
    }
}

