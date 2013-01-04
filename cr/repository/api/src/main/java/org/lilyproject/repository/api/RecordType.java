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
package org.lilyproject.repository.api;

import java.util.Collection;
import java.util.Map;

/**
 * A record type describes the schema to be followed by a {@link Record}.
 *
 * <p>Record types are managed via the {@link TypeManager}. To instantiate a RecordType use
 * {@link TypeManager#newRecordType(QName)}  TypeManager.newRecordType}. As all entities within this API,
 * record types are dumb data objects.
 *
 * <p>A record type consists of:
 *
 * <ul>
 * <li>a list of field types, associated via {@link FieldTypeEntry} which defines properties specific to the use
 * of a field type within this record type.
 * <li>a list of mixins, these are references to other record types to be mixed in (imported within) this record
 * type.
 * </ul>
 *
 * <p>Record types are versioned: upon each update, a new version of the record type is created. Record store a
 * pointer to the particular version of a record type that was used when creating/updating a record type. The
 * references
 * to the mixin record types are also to specific versions.
 *
 * <p>A record type has two unique identifiers:
 * <ul>
 * <li>a system-generated id, immutable after creation of the record type
 * <li>a name in the form of a {@link QName qualified (namespaced) name}, which is mutable after creation of the record
 * type. Changing the name of a record type affects all versions of the record type (the name is a non-versioned
 * property of the record type).
 * </ul>
 */
public interface RecordType {

    /**
     * The id is unique, immutable and system-generated.
     */
    SchemaId getId();

    /**
     * The name is unique, user-provided but can be changed after initial creation of the record type.
     */
    QName getName();

    Long getVersion();

    /**
     * Adds a field type entry. A field type entry can be instantiated via {@link TypeManager#newFieldTypeEntry(SchemaId,
     * boolean)}.
     */
    RecordType withFieldTypeEntry(FieldTypeEntry fieldTypeEntry);

    /**
     * A shortcut for adding a field type entry without having to instantiate it yourself.
     */
    RecordType withFieldTypeEntry(SchemaId fieldTypeId, boolean mandatory);

    /**
     * @return null if there is not field type entry for this field type
     */
    FieldTypeEntry getFieldTypeEntry(SchemaId fieldTypeId);

    RecordType withoutFieldTypeEntry(SchemaId fieldTypeId);

    Collection<FieldTypeEntry> getFieldTypeEntries();

    /**
     * Adds a mixin to the record type.
     * When no version is given, the latest recordType version will be filled in.
     */
    RecordType withMixin(SchemaId recordTypeId, Long recordTypeVersion);

    /**
     * Same as {@link #withMixin(SchemaId, Long)} but with null for the recordTypeVersion.
     */
    RecordType withMixin(SchemaId recordTypeId);

    /**
     * Removes a mixin from the recordType.
     */
    RecordType withoutMixin(SchemaId recordTypeId);

    /**
     * Returns a map of the recordTypeIds and versions of the mixins of the RecordType.
     */
    Map<SchemaId, Long> getMixins();
}
