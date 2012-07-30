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

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;

import org.apache.avro.AvroRemoteException;
import org.lilyproject.indexer.Indexer;
import org.lilyproject.indexer.IndexerException;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.TypeBucket;
import org.lilyproject.repository.api.TypeManager;

public class AvroLilyImpl implements AvroLily {

    private final Repository repository;
    private final TypeManager typeManager;
    private final Indexer indexer;
    private final AvroConverter converter;

    public AvroLilyImpl(Repository repository, Indexer indexer, AvroConverter converter) {
        this.repository = repository;
        this.indexer = indexer;
        this.typeManager = repository.getTypeManager();
        this.converter = converter;
    }

    @Override
    public ByteBuffer create(ByteBuffer record) throws AvroRepositoryException, AvroInterruptedException {
        try {
            return converter.convert(repository.create(converter.convertRecord(record)));
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }

    @Override
    public ByteBuffer createOrUpdate(ByteBuffer record, boolean useLatestRecordType)
            throws AvroRepositoryException, AvroInterruptedException {
        try {
            return converter.convert(repository.createOrUpdate(converter.convertRecord(record), useLatestRecordType));
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }

    @Override
    public ByteBuffer delete(ByteBuffer recordId, List<AvroMutationCondition> conditions)
            throws AvroRepositoryException, AvroInterruptedException {
        try {
            Record record =
                    repository.delete(converter.convertAvroRecordId(recordId), converter.convertFromAvro(conditions));
            return record == null ? null : converter.convert(record);
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }

    @Override
    public ByteBuffer update(ByteBuffer record, boolean updateVersion, boolean useLatestRecordType,
                             List<AvroMutationCondition> conditions) throws AvroRemoteException {
        try {
            return converter.convert(repository.update(converter.convertRecord(record), updateVersion,
                    useLatestRecordType, converter.convertFromAvro(conditions)));
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
            return converter.convert(typeManager.createFieldType(converter.convert(avroFieldType)));
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
            return converter.convert(typeManager.createRecordType(converter.convert(avroRecordType)));
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }

    @Override
    public AvroRecordType createOrUpdateRecordType(AvroRecordType avroRecordType) throws AvroRepositoryException,
            AvroInterruptedException {

        try {
            return converter.convert(typeManager.createOrUpdateRecordType(converter.convert(avroRecordType)));
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
    public AvroRecordType updateRecordType(AvroRecordType recordType)
            throws AvroRepositoryException, AvroInterruptedException {

        try {
            return converter.convert(typeManager.updateRecordType(converter.convert(recordType)));
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
            return converter.convert(typeManager.updateFieldType(converter.convert(fieldType)));
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
            return converter.convert(typeManager.createOrUpdateFieldType(converter.convert(fieldType)));
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
    public List<String> getVariants(ByteBuffer recordId) throws AvroRepositoryException, AvroInterruptedException {
        try {
            return converter.convert(repository.getVariants(converter.convertAvroRecordId(recordId)));
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
    public Object index(ByteBuffer recordId) throws AvroInterruptedException, AvroIndexerException {
        try {
            indexer.index(converter.convertAvroRecordId(recordId));
            return null;
        } catch (InterruptedException e) {
            throw converter.convert(e);
        } catch (IndexerException e) {
            throw converter.convert(e);
        }
    }

    @Override
    public Object indexOn(ByteBuffer recordId, List<String> indexes)
            throws AvroInterruptedException, AvroIndexerException {
        try {
            indexer.indexOn(converter.convertAvroRecordId(recordId), new HashSet<String>(indexes));
            return null;
        } catch (IndexerException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }
}
