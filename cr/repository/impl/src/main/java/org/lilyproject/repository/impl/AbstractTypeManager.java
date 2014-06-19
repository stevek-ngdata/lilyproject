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

import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.FieldTypeBuilder;
import org.lilyproject.repository.api.FieldTypeEntry;
import org.lilyproject.repository.api.FieldTypes;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.RecordTypeBuilder;
import org.lilyproject.repository.api.RecordTypeNotFoundException;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TypeException;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.api.ValueType;
import org.lilyproject.repository.api.ValueTypeFactory;
import org.lilyproject.repository.impl.valuetype.BlobValueType;
import org.lilyproject.repository.impl.valuetype.BooleanValueType;
import org.lilyproject.repository.impl.valuetype.ByteArrayValueType;
import org.lilyproject.repository.impl.valuetype.DateTimeValueType;
import org.lilyproject.repository.impl.valuetype.DateValueType;
import org.lilyproject.repository.impl.valuetype.DecimalValueType;
import org.lilyproject.repository.impl.valuetype.DoubleValueType;
import org.lilyproject.repository.impl.valuetype.IntegerValueType;
import org.lilyproject.repository.impl.valuetype.LinkValueType;
import org.lilyproject.repository.impl.valuetype.ListValueType;
import org.lilyproject.repository.impl.valuetype.LongValueType;
import org.lilyproject.repository.impl.valuetype.PathValueType;
import org.lilyproject.repository.impl.valuetype.RecordValueType;
import org.lilyproject.repository.impl.valuetype.StringValueType;
import org.lilyproject.repository.impl.valuetype.UriValueType;
import org.lilyproject.util.ArgumentValidator;
import org.lilyproject.util.Pair;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public abstract class AbstractTypeManager implements TypeManager {
    protected Log log;

    protected Map<String, ValueTypeFactory> valueTypeFactories = new HashMap<String, ValueTypeFactory>();
    protected IdGenerator idGenerator;

    protected ZooKeeperItf zooKeeper;

    protected SchemaCache schemaCache;

    public AbstractTypeManager(ZooKeeperItf zooKeeper) {
        this.zooKeeper = zooKeeper;
    }

    @Override
    public FieldTypes getFieldTypesSnapshot() throws InterruptedException {
        return schemaCache.getFieldTypesSnapshot();
    }

    @Override
    abstract public List<FieldType> getFieldTypesWithoutCache() throws RepositoryException, InterruptedException;
    @Override
    abstract public List<RecordType> getRecordTypesWithoutCache() throws RepositoryException, InterruptedException;

    protected void updateFieldTypeCache(FieldType fieldType) throws TypeException, InterruptedException {
        schemaCache.updateFieldType(fieldType);
    }

    protected void updateRecordTypeCache(RecordType recordType) throws TypeException, InterruptedException {
        schemaCache.updateRecordType(recordType);
    }

    @Override
    public Collection<RecordType> getRecordTypes() throws InterruptedException {
        return schemaCache.getRecordTypes();
    }

    @Override
    public List<FieldType> getFieldTypes() throws TypeException, InterruptedException {
        return schemaCache.getFieldTypes();
    }

    protected RecordType getRecordTypeFromCache(QName name, Long version) throws InterruptedException {
        return schemaCache.getRecordType(name, version);
    }

    protected RecordType getRecordTypeFromCache(SchemaId id, Long version) {
        return schemaCache.getRecordType(id, version);
    }

    @Override
    public RecordType getRecordTypeById(SchemaId id, Long version) throws RecordTypeNotFoundException, TypeException, RepositoryException, InterruptedException {
        ArgumentValidator.notNull(id, "id");
        RecordType recordType = getRecordTypeFromCache(id, version);
        if (recordType == null) {
            throw new RecordTypeNotFoundException(id, version);
        }
        return recordType.clone();
    }

    @Override
    public RecordType getRecordTypeByName(QName name, Long version) throws RecordTypeNotFoundException, TypeException, RepositoryException, InterruptedException {
        ArgumentValidator.notNull(name, "name");
        RecordType recordType = getRecordTypeFromCache(name, version);
        if (recordType == null) {
            throw new RecordTypeNotFoundException(name, version);
        }
        return recordType.clone();
    }

    @Override
    public Set<QName> findSubtypes(QName recordTypeName) throws InterruptedException, RepositoryException {
        return findSubTypes(recordTypeName, true);
    }

    @Override
    public Set<QName> findDirectSubtypes(QName recordTypeName) throws InterruptedException, RepositoryException {
        return findSubTypes(recordTypeName, false);
    }

    private Set<QName> findSubTypes(QName recordTypeName, boolean recursive)
            throws InterruptedException, RepositoryException {
        ArgumentValidator.notNull(recordTypeName, "recordTypeName");

        RecordType recordType = getRecordTypeByName(recordTypeName, null);
        Set<SchemaId> result = new HashSet<SchemaId>();
        collectSubTypes(recordType.getId(), result, recursive);

        // Translate schema id's to QName's
        Set<QName> names = new HashSet<QName>();
        for (SchemaId id : result) {
            try {
                names.add(getRecordTypeById(id, null).getName());
            } catch (RecordTypeNotFoundException e) {
                // skip, this should only occur in border cases, i.e. the schema is being modified while
                // this method is called
            }
        }

        return names;
    }
    
    @Override
    public Collection<FieldTypeEntry> getFieldTypesForRecordType(RecordType recordType, boolean includeSupertypes) throws RecordTypeNotFoundException, TypeException, RepositoryException, InterruptedException {
        if (!includeSupertypes) {
            return recordType.getFieldTypeEntries();
        } else {
            // Pairs of record type id and version
            Map<Pair<SchemaId, Long>, RecordType> recordSupertypeMap = Maps.newHashMap();
            collectRecordSupertypes(Pair.create(recordType.getId(), recordType.getVersion()), recordSupertypeMap);
            
            // We use a map of SchemaId to FieldTypeEntry so that we can let mandatory field type entries
            // for the same field type override non-mandatory versions
            Map<SchemaId,FieldTypeEntry> fieldTypeMap = Maps.newHashMap();
            
            for (Pair<SchemaId,Long> recordSuperTypePair : recordSupertypeMap.keySet()) {
                RecordType superRecordType = recordSupertypeMap.get(recordSuperTypePair);
                for (FieldTypeEntry fieldTypeEntry : superRecordType.getFieldTypeEntries()) {
                    SchemaId fieldTypeId = fieldTypeEntry.getFieldTypeId();
                    if (fieldTypeMap.containsKey(fieldTypeId)) {
                        // Only overwrite an existing entry if we have one that is mandatory
                        if (fieldTypeEntry.isMandatory()) {
                            fieldTypeMap.put(fieldTypeId, fieldTypeEntry);
                        }
                    } else {
                        fieldTypeMap.put(fieldTypeId, fieldTypeEntry);
                    }
                }
            }
            return fieldTypeMap.values();
        }
    }
    

    private void collectRecordSupertypes(Pair<SchemaId, Long> recordTypeAndVersion, Map<Pair<SchemaId, Long>, RecordType> recordSuperTypes)
                throws RecordTypeNotFoundException, TypeException, RepositoryException, InterruptedException {
        if (recordSuperTypes.containsKey(recordTypeAndVersion)) {
            return;
        }
        RecordType recordType = getRecordTypeById(recordTypeAndVersion.getV1(), recordTypeAndVersion.getV2());
        recordSuperTypes.put(recordTypeAndVersion, recordType);
        for (Entry<SchemaId, Long> entry : recordType.getSupertypes().entrySet()) {
            collectRecordSupertypes(Pair.create(entry.getKey(), entry.getValue()), recordSuperTypes);
        }
        
    }

    @Override
    public Set<SchemaId> findSubtypes(SchemaId recordTypeId) throws InterruptedException, RepositoryException {
        return findSubTypes(recordTypeId, true);
    }

    @Override
    public Set<SchemaId> findDirectSubtypes(SchemaId recordTypeId) throws InterruptedException, RepositoryException {
        return findSubTypes(recordTypeId, false);
    }

    private Set<SchemaId> findSubTypes(SchemaId recordTypeId, boolean recursive)
            throws InterruptedException, RepositoryException {
        ArgumentValidator.notNull(recordTypeId, "recordTypeId");

        // This is to validate the requested ID exists
        getRecordTypeById(recordTypeId, null);

        Set<SchemaId> result = new HashSet<SchemaId>();
        collectSubTypes(recordTypeId, result, recursive);

        return result;
    }

    private void collectSubTypes(SchemaId recordTypeId, Set<SchemaId> result, boolean recursive)
            throws InterruptedException {
        collectSubTypes(recordTypeId, result, new ArrayDeque<SchemaId>(), recursive);
    }

    private void collectSubTypes(SchemaId recordTypeId, Set<SchemaId> result, Deque<SchemaId> parents,
            boolean recursive) throws InterruptedException {
        // the parent-stack is to protect against endless loops in the type hierarchy. If a type is a subtype
        // of itself, it will not be included in the result. Thus if record type A extends (directly or indirectly)
        // from A, and we search the subtypes of A, then the resulting set will not include A.
        parents.push(recordTypeId);
        Set<SchemaId> subtypes = schemaCache.findDirectSubTypes(recordTypeId);
        for (SchemaId subtype : subtypes) {
            if (!parents.contains(subtype)) {
                result.add(subtype);
                if (recursive) {
                    collectSubTypes(subtype, result, parents, recursive);
                }
            } else {
                // Loop detected in type hierarchy, log a warning about this
                log.warn(formatSupertypeLoopError(subtype, parents));
            }
        }
        parents.pop();
    }

    protected String formatSupertypeLoopError(SchemaId subtype, Deque<SchemaId> parents) {
        // Loop detected in type hierarchy, log a warning about this
        List<SchemaId> parentsList = new ArrayList<SchemaId>(parents);
        Collections.reverse(parentsList);

        // find the place where the current childType occurred (we don't want to log any parents higher
        // up, doesn't add any value for the user)
        int pos = parentsList.indexOf(subtype);

        StringBuilder msg = new StringBuilder();
        for (int i = pos; i < parentsList.size(); i++) {
            if (msg.length() > 0) {
                msg.append(" <- ");
            }
            msg.append(getNameSafe(parentsList.get(i)));
        }
        msg.append(" <- ");
        msg.append(getNameSafe(subtype));

        return "Loop in record type inheritance: " + msg;
    }

    /**
     * Tries to look up the name of the record type, but returns the id if it fails.
     */
    private String getNameSafe(SchemaId schemaId) {
        try {
            return getRecordTypeById(schemaId, null).getName().toString();
        } catch (Exception e) {
            return schemaId.toString();
        }
    }

    @Override
    public FieldType getFieldTypeById(SchemaId id) throws TypeException, InterruptedException {
        return schemaCache.getFieldType(id);
    }

    @Override
    public FieldType getFieldTypeByName(QName name) throws InterruptedException, TypeException {
        return schemaCache.getFieldType(name);
    }

    //
    // Object creation methods
    //
    @Override
    public RecordType newRecordType(QName name) {
        return new RecordTypeImpl(null, name);
    }

    @Override
    public RecordType newRecordType(SchemaId recordTypeId, QName name) {
        return new RecordTypeImpl(recordTypeId, name);
    }

    @Override
    public FieldType newFieldType(ValueType valueType, QName name, Scope scope) {
        return newFieldType(null, valueType, name, scope);
    }

    @Override
    public FieldType newFieldType(String valueType, QName name, Scope scope) throws RepositoryException,
            InterruptedException {
        return newFieldType(null, getValueType(valueType), name, scope);
    }

    @Override
    public FieldTypeEntry newFieldTypeEntry(SchemaId fieldTypeId, boolean mandatory) {
        ArgumentValidator.notNull(fieldTypeId, "fieldTypeId");
        ArgumentValidator.notNull(mandatory, "mandatory");
        return new FieldTypeEntryImpl(fieldTypeId, mandatory);
    }


    @Override
    public FieldType newFieldType(SchemaId id, ValueType valueType, QName name, Scope scope) {
        return new FieldTypeImpl(id, valueType, name, scope);
    }

    @Override
    public RecordTypeBuilder recordTypeBuilder() throws TypeException {
        return new RecordTypeBuilderImpl(this);
    }

    @Override
    public FieldTypeBuilder fieldTypeBuilder() throws TypeException {
        return new FieldTypeBuilderImpl(this);
    }

    //
    // Value types
    //
    @Override
    public void registerValueType(String valueTypeName, ValueTypeFactory valueTypeFactory) {
        valueTypeFactories.put(valueTypeName, valueTypeFactory);
    }

    @Override
    public ValueType getValueType(String valueTypeSpec) throws RepositoryException, InterruptedException {
        ValueType valueType;

        int indexOfParams = valueTypeSpec.indexOf("<");
        if (indexOfParams == -1) {
            ValueTypeFactory valueTypeFactory = valueTypeFactories.get(valueTypeSpec);
            if (valueTypeFactory == null) {
                throw new TypeException("Unkown value type: " + valueTypeSpec);
            }
            valueType = valueTypeFactory.getValueType(null);
        } else {
            if (!valueTypeSpec.endsWith(">")) {
                throw new IllegalArgumentException("Invalid value type string, no closing angle bracket: '" +
                        valueTypeSpec + "'");
            }

            String arg = valueTypeSpec.substring(indexOfParams + 1, valueTypeSpec.length() - 1);

            if (arg.length() == 0) {
                throw new IllegalArgumentException("Invalid value type string, type arg is zero length: '" +
                        valueTypeSpec + "'");
            }

            ValueTypeFactory valueTypeFactory = valueTypeFactories.get(valueTypeSpec.substring(0, indexOfParams));
            if (valueTypeFactory == null) {
                throw new TypeException("Unkown value type: " + valueTypeSpec);
            }
            valueType = valueTypeFactory.getValueType(arg);
        }

        return valueType;
    }

    // TODO get this from some configuration file
    protected void registerDefaultValueTypes() {
        //
        // Important:
        //
        // When adding a type below, please update the list of built-in
        // types in the javadoc of the method TypeManager.getValueType.
        //

        // TODO or rather use factories?
        registerValueType(StringValueType.NAME, StringValueType.factory());
        registerValueType(IntegerValueType.NAME, IntegerValueType.factory());
        registerValueType(LongValueType.NAME, LongValueType.factory());
        registerValueType(DoubleValueType.NAME, DoubleValueType.factory());
        registerValueType(DecimalValueType.NAME, DecimalValueType.factory());
        registerValueType(BooleanValueType.NAME, BooleanValueType.factory());
        registerValueType(DateValueType.NAME, DateValueType.factory());
        registerValueType(DateTimeValueType.NAME, DateTimeValueType.factory());
        registerValueType(LinkValueType.NAME, LinkValueType.factory(idGenerator, this));
        registerValueType(BlobValueType.NAME, BlobValueType.factory());
        registerValueType(UriValueType.NAME, UriValueType.factory());
        registerValueType(ListValueType.NAME, ListValueType.factory(this));
        registerValueType(PathValueType.NAME, PathValueType.factory(this));
        registerValueType(RecordValueType.NAME, RecordValueType.factory(this));
        registerValueType(ByteArrayValueType.NAME, ByteArrayValueType.factory());
    }
}
