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
import java.util.List;

import javax.annotation.PreDestroy;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.lilyproject.repository.api.*;
import org.lilyproject.repository.avro.*;
import org.lilyproject.util.Pair;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

// ATTENTION: when adding new methods, do not forget to add handling for UndeclaredThrowableException! This is
//            necessary because, at the time of this writing, Avro did not include IOException in its generated
//            interfaces.

public class RemoteTypeManager extends AbstractTypeManager implements TypeManager {

    private AvroLily lilyProxy;
    private AvroConverter converter;
    private Transceiver client;

    public RemoteTypeManager(InetSocketAddress address, AvroConverter converter, IdGenerator idGenerator,
            ZooKeeperItf zooKeeper, SchemaCache schemaCache)
            throws IOException {
        super(zooKeeper);
        super.schemaCache = schemaCache;
        log = LogFactory.getLog(getClass());
        this.converter = converter;
        //TODO idGenerator should not be available or used in the remote implementation
        this.idGenerator = idGenerator;
        //client = new HttpTransceiver(new URL("http://" + address.getHostName() + ":" + address.getPort() + "/"));
        client = NettyTransceiverFactory.create(address);

        lilyProxy = SpecificRequestor.getClient(AvroLily.class, client);
        registerDefaultValueTypes();
    }

    /**
     * Start should be called for the RemoteTypeManager after the typemanager
     * has been assigned to the repository, after the repository has been
     * assigned to the AvroConverter and before using the typemanager and
     * repository.
     * 
     * @throws InterruptedException
     * @throws KeeperException
     * @throws RepositoryException
     */
    public void start() throws InterruptedException, KeeperException, RepositoryException {
        // schemaCache.start();
    }

    @Override
    @PreDestroy
    public void close() throws IOException {
        Closer.close(client);
    }

    @Override
    public RecordType createRecordType(RecordType recordType) throws RepositoryException, InterruptedException {

        try {
            RecordType newRecordType = converter.convert(lilyProxy.createRecordType(converter.convert(recordType)));
            updateRecordTypeCache(newRecordType.clone());
            return newRecordType;
        } catch (AvroRepositoryException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredTypeThrowable(e);
        }
    }

    @Override
    public RecordType createOrUpdateRecordType(RecordType recordType) throws RepositoryException, InterruptedException {
        try {
            RecordType newRecordType = converter.convert(lilyProxy.createOrUpdateRecordType(converter.convert(recordType)));
            updateRecordTypeCache(newRecordType.clone());
            return newRecordType;
        } catch (AvroRepositoryException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredTypeThrowable(e);
        }
    }

    @Override
    protected RecordType getRecordTypeByIdWithoutCache(SchemaId id, Long version) throws RepositoryException, InterruptedException {
        try {
            long avroVersion;
            if (version == null) {
                avroVersion = -1;
            } else {
                avroVersion = version;
            }
            return converter.convert(lilyProxy.getRecordTypeById(converter.convert(id), avroVersion));
        } catch (AvroRepositoryException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredTypeThrowable(e);
        }
    }

    @Override
    public RecordType updateRecordType(RecordType recordType) throws RepositoryException, InterruptedException {
        try {
            RecordType newRecordType = converter.convert(lilyProxy.updateRecordType(converter.convert(recordType)));
            updateRecordTypeCache(newRecordType);
            return newRecordType;
        } catch (AvroRepositoryException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredTypeThrowable(e);
        }
    }

    @Override
    public FieldType createFieldType(ValueType valueType, QName name, Scope scope) throws RepositoryException,
            InterruptedException {
        return createFieldType(newFieldType(valueType, name, scope));
    }

    @Override
    public FieldType createFieldType(String valueType, QName name, Scope scope) throws RepositoryException,
            InterruptedException {
        return createFieldType(newFieldType(getValueType(valueType), name, scope));
    }

    @Override
    public FieldType createFieldType(FieldType fieldType) throws RepositoryException, InterruptedException {
        try {
            AvroFieldType avroFieldType = converter.convert(fieldType);
            AvroFieldType createFieldType = lilyProxy.createFieldType(avroFieldType);
            FieldType newFieldType = converter.convert(createFieldType);
            updateFieldTypeCache(newFieldType);
            return newFieldType;
        } catch (AvroRepositoryException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredTypeThrowable(e);
        }
    }

    @Override
    public FieldType createOrUpdateFieldType(FieldType fieldType) throws RepositoryException, InterruptedException {
        try {
            FieldType newFieldType = converter.convert(lilyProxy.createOrUpdateFieldType(converter.convert(fieldType)));
            updateFieldTypeCache(newFieldType);
            return newFieldType;
        } catch (AvroRepositoryException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredTypeThrowable(e);
        }
    }

    @Override
    public FieldType updateFieldType(FieldType fieldType) throws RepositoryException, InterruptedException {

        try {
            FieldType newFieldType = converter.convert(lilyProxy.updateFieldType(converter.convert(fieldType)));
            updateFieldTypeCache(newFieldType);
            return newFieldType;
        } catch (AvroRepositoryException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredTypeThrowable(e);
        }
    }

    @Override
    public List<FieldType> getFieldTypesWithoutCache() throws RepositoryException, InterruptedException {
        try {
            return converter.convertAvroFieldTypes(lilyProxy.getFieldTypesWithoutCache());
        } catch (AvroRepositoryException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredTypeThrowable(e);
        }
    }

    @Override
    public List<RecordType> getRecordTypesWithoutCache() throws RepositoryException, InterruptedException {
        try {
            return converter.convertAvroRecordTypes(lilyProxy.getRecordTypesWithoutCache());
        } catch (AvroRepositoryException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredTypeThrowable(e);
        }
    }

    @Override
    public Pair<List<FieldType>, List<RecordType>> getFieldAndRecordTypesWithoutCache() throws RepositoryException,
            InterruptedException {
        try {
            return converter.convertAvroFieldAndRecordTypes(lilyProxy.getFieldAndRecordTypesWithoutCache());
        } catch (AvroRepositoryException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredTypeThrowable(e);
        }
    }

    @Override
    public void disableSchemaCacheRefresh() throws RepositoryException, InterruptedException {
        try {
            lilyProxy.disableSchemaCacheRefresh();
        } catch (AvroRepositoryException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredTypeThrowable(e);
        }
    }

    @Override
    public void enableSchemaCacheRefresh() throws RepositoryException, InterruptedException {
        try {
            lilyProxy.enableSchemaCacheRefresh();
        } catch (AvroRepositoryException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredTypeThrowable(e);
        }
    }

    @Override
    public boolean isSchemaCacheRefreshEnabled() throws RepositoryException, InterruptedException {
        try {
            return lilyProxy.isSchemaCacheRefreshEnabled();
        } catch (AvroRepositoryException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredTypeThrowable(e);
        }
    }

    @Override
    public void triggerSchemaCacheRefresh() throws RepositoryException, InterruptedException {
        try {
            lilyProxy.triggerSchemaCacheRefresh();
        } catch (AvroRepositoryException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw converter.convert(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredTypeThrowable(e);
        }
    }

    private RuntimeException handleUndeclaredTypeThrowable(UndeclaredThrowableException e) throws TypeException {
        if (e.getCause() instanceof IOException) {
            throw new IOTypeException(e.getCause());
        } else {
            throw e;
        }
    }

}
