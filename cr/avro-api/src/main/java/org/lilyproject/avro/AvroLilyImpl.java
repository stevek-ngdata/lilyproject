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
package org.lilyproject.avro;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import org.apache.avro.AvroRemoteException;
import org.lilyproject.indexer.Indexer;
import org.lilyproject.indexer.IndexerException;
import org.lilyproject.repository.api.LRepository;
import org.lilyproject.repository.api.LTable;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.repository.api.RepositoryTable;
import org.lilyproject.repository.api.TableManager;
import org.lilyproject.repository.api.TypeBucket;
import org.lilyproject.repository.api.TypeManager;

public class AvroLilyImpl implements AvroLily {

    private final RepositoryManager repositoryManager;
    private final TypeManager typeManager;
    private final Indexer indexer;
    private AvroConverter converter;

    public AvroLilyImpl(RepositoryManager repositoryManager, TypeManager typeManager, Indexer indexer) {
        this.repositoryManager = repositoryManager;
        this.indexer = indexer;
        this.typeManager = typeManager;
        this.converter = new AvroConverter();
    }

    @VisibleForTesting
    public void setAvroConverter(AvroConverter converter) {
        this.converter = converter;
    }

    @Override
    public ByteBuffer create(ByteBuffer record, String tenant, String tableName) throws AvroRepositoryException, AvroInterruptedException {
        try {
            LRepository repository = repositoryManager.getRepository(tenant);
            LTable table = repository.getTable(tableName);
            return converter.convert(table.create(converter.convertRecord(record, repository)), repository);
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }

    @Override
    public ByteBuffer createOrUpdate(ByteBuffer record, String tenant, String tableName, boolean useLatestRecordType)
            throws AvroRepositoryException, AvroInterruptedException {
        try {
            LRepository repository = repositoryManager.getRepository(tenant);
            LTable table = repository.getTable(tableName);
            return converter.convert(
                    table.createOrUpdate(converter.convertRecord(record, repository), useLatestRecordType), repository);
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }

    @Override
    public ByteBuffer delete(ByteBuffer recordId, String tenant, String tableName, List<AvroMutationCondition> conditions,
            Map<String,String> attributes) throws AvroRepositoryException, AvroInterruptedException {
        try {
            LRepository repository = repositoryManager.getRepository(tenant);
            LTable table = repository.getTable(tableName);
            RecordId decodedRecordId = converter.convertAvroRecordId(recordId, repository);
            Record record = null;
            if (attributes == null) {
                record = table.delete(decodedRecordId, converter.convertFromAvro(conditions, repository));
            } else if (conditions == null) {
                Record toDelete = table.newRecord(decodedRecordId);
                toDelete.setAttributes(attributes);
                table.delete(toDelete);
            } else {
                // There is no API call where a full record and MutationConditions can be supplied, so
                // something has gone wrong if we get here
                throw new IllegalStateException("Cannot delete a full record with MutationConditions");
            }
            return record == null ? null : converter.convert(record, repository);
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }

    @Override
    public ByteBuffer update(ByteBuffer record, String tenant, String tableName, boolean updateVersion,
            boolean useLatestRecordType, List<AvroMutationCondition> conditions) throws AvroRemoteException {
        try {
            LRepository repository = repositoryManager.getRepository(tenant);
            LTable table = repository.getTable(tableName);
            return converter.convert(table.update(converter.convertRecord(record, repository), updateVersion,
                    useLatestRecordType, converter.convertFromAvro(conditions, repository)), repository);
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }

    @Override
    public AvroFieldType createFieldType(AvroFieldType avroFieldType)
            throws AvroRepositoryException, AvroInterruptedException {

        try {
            return converter.convert(typeManager.createFieldType(converter.convert(avroFieldType, typeManager)));
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }

    @Override
    public AvroRecordType createRecordType(AvroRecordType avroRecordType)
            throws AvroRepositoryException, AvroInterruptedException {

        try {
            return converter.convert(typeManager.createRecordType(converter.convert(avroRecordType, typeManager)));
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }

    @Override
    public AvroRecordType createOrUpdateRecordType(AvroRecordType avroRecordType, boolean refreshSubtypes)
            throws AvroRepositoryException, AvroInterruptedException {

        try {
            return converter.convert(typeManager.createOrUpdateRecordType(
                    converter.convert(avroRecordType, typeManager), refreshSubtypes));
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }

    @Override
    public AvroRecordType getRecordTypeById(AvroSchemaId id, long avroVersion)
            throws AvroRepositoryException, AvroInterruptedException {
        try {
            return converter.convert(
                    typeManager.getRecordTypeById(converter.convert(id), converter.convertAvroVersion(avroVersion)));
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }

    @Override
    public AvroRecordType getRecordTypeByName(AvroQName name, long avroVersion)
            throws AvroRepositoryException, AvroInterruptedException {
        try {
            return converter.convert(typeManager
                    .getRecordTypeByName(converter.convert(name), converter.convertAvroVersion(avroVersion)));
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }

    @Override
    public AvroRecordType updateRecordType(AvroRecordType recordType, boolean refreshSubtypes)
            throws AvroRepositoryException, AvroInterruptedException {

        try {
            return converter.convert(
                    typeManager.updateRecordType(converter.convert(recordType, typeManager), refreshSubtypes));
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }

    @Override
    public AvroFieldType updateFieldType(AvroFieldType fieldType)
            throws AvroRepositoryException, AvroInterruptedException {

        try {
            return converter.convert(typeManager.updateFieldType(converter.convert(fieldType, typeManager)));
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }

    @Override
    public AvroFieldType createOrUpdateFieldType(AvroFieldType fieldType) throws AvroRepositoryException,
            AvroInterruptedException {

        try {
            return converter.convert(typeManager.createOrUpdateFieldType(converter.convert(fieldType, typeManager)));
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }

    @Override
    public AvroFieldType getFieldTypeById(AvroSchemaId id) throws AvroRepositoryException, AvroInterruptedException {
        try {
            return converter.convert(typeManager.getFieldTypeById(converter.convert(id)));
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }

    @Override
    public AvroFieldType getFieldTypeByName(AvroQName name) throws AvroRepositoryException, AvroInterruptedException {
        try {
            return converter.convert(typeManager.getFieldTypeByName(converter.convert(name)));
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }

    @Override
    public List<AvroFieldType> getFieldTypes() throws AvroRepositoryException, AvroInterruptedException {
        try {
            return converter.convertFieldTypes(typeManager.getFieldTypes());
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }

    @Override
    public List<AvroRecordType> getRecordTypes() throws AvroRepositoryException, AvroInterruptedException {
        try {
            return converter.convertRecordTypes(typeManager.getRecordTypes());
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }

    @Override
    public List<AvroFieldType> getFieldTypesWithoutCache() throws AvroRepositoryException, AvroInterruptedException {
        try {
            return converter.convertFieldTypes(typeManager.getFieldTypesWithoutCache());
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }

    @Override
    public List<AvroRecordType> getRecordTypesWithoutCache() throws AvroRepositoryException, AvroInterruptedException {
        try {
            return converter.convertRecordTypes(typeManager.getRecordTypesWithoutCache());
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }

    @Override
    public AvroFieldAndRecordTypes getTypesWithoutCache()
            throws AvroRepositoryException,
            AvroInterruptedException {
        try {
            return converter.convertFieldAndRecordTypes(typeManager.getTypesWithoutCache());
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }

    @Override
    public AvroTypeBucket getTypeBucketWithoutCache(String bucketId)
            throws AvroRepositoryException, AvroInterruptedException {
        try {
            TypeBucket typeBucket = typeManager.getTypeBucketWithoutCache(bucketId);
            return converter.convertTypeBucket(typeBucket);
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }

    @Override
    public List<String> getVariants(ByteBuffer recordId, String tenant, String tableName) throws AvroRepositoryException, AvroInterruptedException {
        try {
            LRepository repository = repositoryManager.getDefaultRepository();
            LTable table = repository.getTable(tableName);
            return converter.convert(table.getVariants(converter.convertAvroRecordId(recordId, repository)));
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }

    @Override
    public Object disableSchemaCacheRefresh() throws AvroRepositoryException, AvroInterruptedException {
        try {
            typeManager.disableSchemaCacheRefresh();
            return null;
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }

    @Override
    public Object enableSchemaCacheRefresh() throws AvroRepositoryException, AvroInterruptedException {
        try {
            typeManager.enableSchemaCacheRefresh();
            return null;
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }

    @Override
    public boolean isSchemaCacheRefreshEnabled() throws AvroRepositoryException, AvroInterruptedException {
        try {
            return typeManager.isSchemaCacheRefreshEnabled();
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }

    @Override
    public Object triggerSchemaCacheRefresh() throws AvroRepositoryException, AvroInterruptedException {
        try {
            typeManager.triggerSchemaCacheRefresh();
            return null;
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }

    @Override
    public Object index(String tenant, String table, ByteBuffer recordId) throws AvroInterruptedException, AvroIndexerException {
        try {
            // TODO multitenancy
            LRepository repository = repositoryManager.getRepository(tenant);
            indexer.index(table, converter.convertAvroRecordId(recordId, repository));
            return null;
        } catch (InterruptedException e) {
            throw converter.convert(e);
        } catch (IndexerException e) {
            throw converter.convert(e);
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object indexOn(String tenant, String table, ByteBuffer recordId, List<String> indexes)
            throws AvroInterruptedException, AvroIndexerException {
        try {
            // TODO multitenancy
            LRepository repository = repositoryManager.getRepository(tenant);
            indexer.indexOn(table, converter.convertAvroRecordId(recordId, repository), new HashSet<String>(indexes));
            return null;
        } catch (IndexerException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object createTable(String tenant, AvroTableCreateDescriptor tableCreateDescriptor)
            throws AvroInterruptedException, AvroIOException, AvroRepositoryException {
        try {
            TableManager tableMgr = repositoryManager.getRepository(tenant).getTableManager();
            tableMgr.createTable(converter.convert(tableCreateDescriptor));
            return null;
        } catch (InterruptedException e) {
            throw converter.convert(e);
        } catch (IOException e) {
            throw converter.convert(e);
        } catch (RepositoryException e) {
            throw converter.convert(e);
        }
    }

    @Override
    public Object dropTable(String tenant, String name)
            throws AvroInterruptedException, AvroIOException, AvroRepositoryException {
        try {
            TableManager tableMgr = repositoryManager.getRepository(tenant).getTableManager();
            tableMgr.dropTable(name);
            return null;
        } catch (InterruptedException e) {
            throw converter.convert(e);
        } catch (IOException e) {
            throw converter.convert(e);
        } catch (RepositoryException e) {
            throw converter.convert(e);
        }
    }

    @Override
    public List<String> getTables(String tenant)
            throws AvroInterruptedException, AvroIOException, AvroRepositoryException {
        try {
            TableManager tableMgr = repositoryManager.getRepository(tenant).getTableManager();
            List<String> tables = new ArrayList<String>();
            for (RepositoryTable table : tableMgr.getTables()) {
                tables.add(table.getName());
            }
            return tables;
        } catch (InterruptedException e) {
            throw converter.convert(e);
        } catch (IOException e) {
            throw converter.convert(e);
        } catch (RepositoryException e) {
            throw converter.convert(e);
        }
    }

    @Override
    public boolean tableExists(String tenant, String name)
            throws AvroInterruptedException, AvroIOException, AvroRepositoryException {
        try {
            TableManager tableMgr = repositoryManager.getRepository(tenant).getTableManager();
            return tableMgr.tableExists(name);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        } catch (IOException e) {
            throw converter.convert(e);
        } catch (RepositoryException e) {
            throw converter.convert(e);
        }
    }
}
