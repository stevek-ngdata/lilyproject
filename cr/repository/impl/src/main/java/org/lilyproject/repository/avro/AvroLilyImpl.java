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
package org.lilyproject.repository.avro;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.AvroRemoteException;
import org.lilyproject.repository.api.*;

public class AvroLilyImpl implements AvroLily {

    private final Repository repository;
    private final TypeManager typeManager;
    private final AvroConverter converter;

    public AvroLilyImpl(Repository repository, AvroConverter converter) {
        this.repository = repository;
        this.typeManager = repository.getTypeManager();
        this.converter = converter;
    }

    public AvroRecord create(AvroRecord record) throws AvroRepositoryException, AvroInterruptedException {
        try {
            return converter.convert(repository.create(converter.convert(record)));
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }

    public AvroRecord createOrUpdate(AvroRecord record, boolean useLatestRecordType)
            throws AvroRepositoryException, AvroInterruptedException {
        try {
            return converter.convert(repository.createOrUpdate(converter.convert(record), useLatestRecordType));
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }

    @Override
    public AvroRecord delete(ByteBuffer recordId, List<AvroMutationCondition> conditions)
            throws AvroRemoteException, AvroRepositoryException, AvroGenericException, AvroInterruptedException {
        try {
            Record record = repository.delete(converter.convertAvroRecordId(recordId), converter.convertFromAvro(conditions));
            return record == null ? null : converter.convert(record);
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }

    public AvroRecord read(ByteBuffer avroRecordId, long avroVersion, List<AvroQName> avroFieldNames)
            throws AvroRepositoryException, AvroInterruptedException {
        Long version = converter.convertAvroVersion(avroVersion);
        RecordId recordId = converter.convertAvroRecordId(avroRecordId);
        QName[] fieldNames = converter.convert(avroFieldNames);
        try {
            if (version == null) {
                return converter.convert(repository.read(recordId, fieldNames));
            } else {
                return converter.convert(repository.read(recordId, version, fieldNames));
            }
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }
    
    public List<AvroRecord> readRecords(List<ByteBuffer> avroRecordIds, List<AvroQName> avroFieldNames)
            throws AvroRepositoryException, AvroInterruptedException {
        List<RecordId> recordIds = null;
        if (avroRecordIds != null) {
            recordIds = new ArrayList<RecordId>();
            for (ByteBuffer avroRecordId: avroRecordIds) {
                recordIds.add(converter.convertAvroRecordId(avroRecordId));
            }
        }
        try {
            return converter.convertRecords(repository.read(recordIds, converter.convert(avroFieldNames)));
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }

    public List<AvroRecord> readVersions(ByteBuffer recordId, long avroFromVersion, long avroToVersion,
            List<AvroQName> avroFieldNames) throws AvroRepositoryException, AvroInterruptedException {
        try {
            return converter.convertRecords(repository.readVersions(converter.convertAvroRecordId(
                    recordId), converter.convertAvroVersion(avroFromVersion), converter.convertAvroVersion(avroToVersion), converter.convert(avroFieldNames)));
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }
    
    public List<AvroRecord> readSpecificVersions(ByteBuffer recordId, List<Long> avroVersions,
            List<AvroQName> avroFieldNames) throws AvroRepositoryException, AvroInterruptedException {
        // The avroVersions are a GenericData$Array which for instance cannot be sorted, so we convert it to an ArrayList
        List<Long> versions = new ArrayList<Long>(avroVersions.size());
        versions.addAll(avroVersions);
        try {
            return converter.convertRecords(repository.readVersions(converter.convertAvroRecordId(
                    recordId), versions, converter.convert(avroFieldNames)));
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }

    @Override
    public AvroRecord update(AvroRecord record, boolean updateVersion, boolean useLatestRecordType,
            List<AvroMutationCondition> conditions) throws AvroRemoteException {
        try {
            return converter.convert(repository.update(converter.convert(record), updateVersion, useLatestRecordType,
                    converter.convertFromAvro(conditions)));
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }

    public AvroFieldType createFieldType(AvroFieldType avroFieldType) throws AvroRepositoryException, AvroInterruptedException {

        try {
            return converter.convert(typeManager.createFieldType(converter.convert(avroFieldType)));
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }

    public AvroRecordType createRecordType(AvroRecordType avroRecordType) throws AvroRepositoryException, AvroInterruptedException {

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

    public AvroRecordType getRecordTypeById(AvroSchemaId id, long avroVersion) throws AvroRepositoryException, AvroInterruptedException {
        try {
            return converter.convert(typeManager.getRecordTypeById(converter.convert(id), converter.convertAvroVersion(avroVersion)));
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }

    public AvroRecordType getRecordTypeByName(AvroQName name, long avroVersion) throws AvroRepositoryException, AvroInterruptedException {
        try {
            return converter.convert(typeManager.getRecordTypeByName(converter.convert(name), converter.convertAvroVersion(avroVersion)));
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }

    public AvroRecordType updateRecordType(AvroRecordType recordType) throws AvroRepositoryException, AvroInterruptedException {

        try {
            return converter.convert(typeManager.updateRecordType(converter.convert(recordType)));
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }

    public AvroFieldType updateFieldType(AvroFieldType fieldType) throws AvroRepositoryException, AvroInterruptedException {

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

    public AvroFieldType getFieldTypeById(AvroSchemaId id) throws AvroRepositoryException, AvroInterruptedException {
        try {
            return converter.convert(typeManager.getFieldTypeById(converter.convert(id)));
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }

    public AvroFieldType getFieldTypeByName(AvroQName name) throws AvroRepositoryException, AvroInterruptedException {
        try {
            return converter.convert(typeManager.getFieldTypeByName(converter.convert(name)));
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }

    public List<AvroFieldType> getFieldTypes() throws AvroRepositoryException, AvroInterruptedException {
        try {
            return converter.convertFieldTypes(typeManager.getFieldTypes());
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }

    public List<AvroRecordType> getRecordTypes() throws AvroRepositoryException, AvroInterruptedException {
        try {
            return converter.convertRecordTypes(typeManager.getRecordTypes());
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }
    
    public List<AvroFieldType> getFieldTypesWithoutCache() throws AvroRepositoryException, AvroInterruptedException {
        try {
            return converter.convertFieldTypes(typeManager.getFieldTypesWithoutCache());
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }

    public List<AvroRecordType> getRecordTypesWithoutCache() throws AvroRepositoryException, AvroInterruptedException {
        try {
            return converter.convertRecordTypes(typeManager.getRecordTypesWithoutCache());
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        } 
    }

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

    public List<AvroTypeBucket> getTypeBucketsWithoutCache(List<CharSequence> rowPrefixes)
            throws AvroRepositoryException, AvroInterruptedException {
        try {
            List<TypeBucket> typeBuckets = typeManager.getTypeBucketsWithoutCache(converter
                    .convert(rowPrefixes));
            List<AvroTypeBucket> avroTypeBuckets = new ArrayList<AvroTypeBucket>(typeBuckets.size());
            for (TypeBucket typeBucket : typeBuckets) {
                avroTypeBuckets.add(converter.convertTypeBucket(typeBucket));
            }
            return avroTypeBuckets;
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }

    public List<CharSequence> getVariants(ByteBuffer recordId) throws AvroRepositoryException, AvroInterruptedException {
        try {
            return converter.convert(repository.getVariants(converter.convertAvroRecordId(recordId)));
        } catch (RepositoryException e) {
            throw converter.convert(e);
        } catch (InterruptedException e) {
            throw converter.convert(e);
        }
    }

    public AvroIdRecord readWithIds(ByteBuffer recordId, long avroVersion, List<AvroSchemaId> avroFieldIds)
            throws AvroRepositoryException, AvroInterruptedException {
        try {
            List<SchemaId> fieldIds = null;
            if (avroFieldIds != null) {
                fieldIds = new ArrayList<SchemaId>();
                for (AvroSchemaId avroFieldId : avroFieldIds) {
                    fieldIds.add(converter.convert(avroFieldId));
                }
            }
            return converter.convert(repository.readWithIds(converter.convertAvroRecordId(recordId), converter.convertAvroVersion(avroVersion), fieldIds));
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
}
