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

import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.avro.AvroConverter;
import org.lilyproject.avro.AvroFieldType;
import org.lilyproject.avro.AvroGenericException;
import org.lilyproject.avro.AvroLily;
import org.lilyproject.avro.AvroRepositoryException;
import org.lilyproject.avro.AvroTypeBucket;
import org.lilyproject.avro.NettyTransceiverFactory;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.IOTypeException;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TypeBucket;
import org.lilyproject.repository.api.TypeException;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.api.ValueType;
import org.lilyproject.repository.impl.AbstractTypeManager;
import org.lilyproject.repository.impl.SchemaCache;
import org.lilyproject.util.Pair;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetSocketAddress;
import java.util.List;
import javax.annotation.PreDestroy;

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
        this(address, converter, idGenerator, zooKeeper, schemaCache, false);
    }

    public RemoteTypeManager(InetSocketAddress address, AvroConverter converter, IdGenerator idGenerator,
            ZooKeeperItf zooKeeper, SchemaCache schemaCache, boolean keepAlive)
            throws IOException {
        super(zooKeeper);
        super.schemaCache = schemaCache;
        log = LogFactory.getLog(getClass());
        this.converter = converter;
        //TODO idGenerator should not be available or used in the remote implementation
        this.idGenerator = idGenerator;
        //client = new HttpTransceiver(new URL("http://" + address.getHostName() + ":" + address.getPort() + "/"));
        client = NettyTransceiverFactory.create(address, keepAlive);

        lilyProxy = SpecificRequestor.getClient(AvroLily.class, client);
        registerDefaultValueTypes();
    }

    @Override
    @PreDestroy
    public void close() throws IOException {
        Closer.close(client);
    }

    @Override
    public RecordType createRecordType(RecordType recordType) throws RepositoryException, InterruptedException {

        try {
            RecordType newRecordType = converter.convert(lilyProxy.createRecordType(converter.convert(recordType)), this);
            updateRecordTypeCache(newRecordType.clone());
            return newRecordType;
        } catch (AvroRepositoryException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw handleAvroRemoteException(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredTypeThrowable(e);
        }
    }

    @Override
    public RecordType createOrUpdateRecordType(RecordType recordType) throws RepositoryException, InterruptedException {
        return createOrUpdateRecordType(recordType, false);
    }

    @Override
    public RecordType createOrUpdateRecordType(RecordType recordType, boolean refreshSubtypes)
            throws RepositoryException, InterruptedException {
        try {
            RecordType newRecordType =
                    converter.convert(lilyProxy.createOrUpdateRecordType(converter.convert(recordType), refreshSubtypes), this);
            updateRecordTypeCache(newRecordType.clone());
            return newRecordType;
        } catch (AvroRepositoryException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw handleAvroRemoteException(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredTypeThrowable(e);
        }
    }

    @Override
    public RecordType updateRecordType(RecordType recordType) throws RepositoryException, InterruptedException {
        return updateRecordType(recordType, false);
    }

    @Override
    public RecordType updateRecordType(RecordType recordType, boolean refreshSubtypes)
            throws RepositoryException, InterruptedException {
        try {
            RecordType newRecordType = converter.convert(
                    lilyProxy.updateRecordType(converter.convert(recordType), refreshSubtypes), this);
            updateRecordTypeCache(newRecordType);
            return newRecordType;
        } catch (AvroRepositoryException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw handleAvroRemoteException(e);
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
            FieldType newFieldType = converter.convert(createFieldType, this);
            updateFieldTypeCache(newFieldType);
            return newFieldType;
        } catch (AvroRepositoryException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw handleAvroRemoteException(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredTypeThrowable(e);
        }
    }

    @Override
    public FieldType createOrUpdateFieldType(FieldType fieldType) throws RepositoryException, InterruptedException {
        try {
            FieldType newFieldType = converter.convert(lilyProxy.createOrUpdateFieldType(converter.convert(fieldType)), this);
            updateFieldTypeCache(newFieldType);
            return newFieldType;
        } catch (AvroRepositoryException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw handleAvroRemoteException(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredTypeThrowable(e);
        }
    }

    @Override
    public FieldType updateFieldType(FieldType fieldType) throws RepositoryException, InterruptedException {

        try {
            FieldType newFieldType = converter.convert(lilyProxy.updateFieldType(converter.convert(fieldType)), this);
            updateFieldTypeCache(newFieldType);
            return newFieldType;
        } catch (AvroRepositoryException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw handleAvroRemoteException(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredTypeThrowable(e);
        }
    }

    @Override
    public List<FieldType> getFieldTypesWithoutCache() throws RepositoryException, InterruptedException {
        try {
            return converter.convertAvroFieldTypes(lilyProxy.getFieldTypesWithoutCache(), this);
        } catch (AvroRepositoryException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw handleAvroRemoteException(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredTypeThrowable(e);
        }
    }

    @Override
    public List<RecordType> getRecordTypesWithoutCache() throws RepositoryException, InterruptedException {
        try {
            return converter.convertAvroRecordTypes(lilyProxy.getRecordTypesWithoutCache(), this);
        } catch (AvroRepositoryException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw handleAvroRemoteException(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredTypeThrowable(e);
        }
    }

    @Override
    public Pair<List<FieldType>, List<RecordType>> getTypesWithoutCache() throws RepositoryException,
            InterruptedException {
        try {
            return converter.convertAvroFieldAndRecordTypes(lilyProxy.getTypesWithoutCache(), this);
        } catch (AvroRepositoryException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw handleAvroRemoteException(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredTypeThrowable(e);
        }
    }

    @Override
    public TypeBucket getTypeBucketWithoutCache(String bucketId) throws RepositoryException,
            InterruptedException {
        try {

            AvroTypeBucket avroTypeBucket = lilyProxy.getTypeBucketWithoutCache(bucketId);
            return converter.convertAvroTypeBucket(avroTypeBucket, this);
        } catch (AvroRepositoryException e) {
            throw converter.convert(e);
        } catch (AvroRemoteException e) {
            throw handleAvroRemoteException(e);
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
            throw handleAvroRemoteException(e);
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
            throw handleAvroRemoteException(e);
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
            throw handleAvroRemoteException(e);
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
            throw handleAvroRemoteException(e);
        } catch (UndeclaredThrowableException e) {
            throw handleUndeclaredTypeThrowable(e);
        }
    }

    private RuntimeException handleAvroRemoteException(AvroRemoteException e) throws TypeException {
        // AvroRemoteException's are exceptions which are not declared in the avro protocol and
        // which are not RuntimeException's.
        if (e.getCause() instanceof IOException) {
            throw new IOTypeException(e.getCause());
        } else {
            throw converter.convert(e);
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
