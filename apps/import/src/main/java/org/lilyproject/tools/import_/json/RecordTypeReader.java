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
package org.lilyproject.tools.import_.json;

import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.impl.id.SchemaIdImpl;

import static org.lilyproject.util.json.JsonUtil.getArray;
import static org.lilyproject.util.json.JsonUtil.getBoolean;
import static org.lilyproject.util.json.JsonUtil.getLong;
import static org.lilyproject.util.json.JsonUtil.getString;

public class RecordTypeReader implements EntityReader<RecordType> {
    public static final EntityReader<RecordType> INSTANCE  = new RecordTypeReader();

    @Override
    public RecordType fromJson(JsonNode node, Repository repository) throws JsonFormatException, RepositoryException,
            InterruptedException {
        return fromJson(node, null, repository);
    }

    @Override
    public RecordType fromJson(JsonNode nodeNode, Namespaces namespaces, Repository repository)
            throws JsonFormatException, RepositoryException, InterruptedException {

        if (!nodeNode.isObject()) {
            throw new JsonFormatException("Expected a json object for record type, got: " +
                    nodeNode.getClass().getName());
        }

        ObjectNode node = (ObjectNode)nodeNode;

        namespaces = NamespacesConverter.fromContextJson(node, namespaces);

        TypeManager typeManager = repository.getTypeManager();
        QName name = QNameConverter.fromJson(getString(node, "name"), namespaces);

        RecordType recordType = typeManager.newRecordType(name);

        String id = getString(node, "id", null);
        if (id != null) {
            recordType.setId(new SchemaIdImpl(id));
        }

        if (node.get("fields") != null) {
            ArrayNode fields = getArray(node, "fields");
            for (int i = 0; i < fields.size(); i++) {
                JsonNode field = fields.get(i);

                boolean mandatory = getBoolean(field, "mandatory", false);

                String fieldIdString = getString(field, "id", null);
                String fieldName = getString(field, "name", null);

                if (fieldIdString != null) {
                    recordType.addFieldTypeEntry(new SchemaIdImpl(fieldIdString), mandatory);
                } else if (fieldName != null) {
                    QName fieldQName = QNameConverter.fromJson(fieldName, namespaces);

                    try {
                        SchemaId fieldId = typeManager.getFieldTypeByName(fieldQName).getId();
                        recordType.addFieldTypeEntry(fieldId, mandatory);
                    } catch (RepositoryException e) {
                        throw new JsonFormatException("Record type " + name + ": error looking up field type with name: " +
                                fieldQName, e);
                    }
                } else {
                    throw new JsonFormatException("Record type " + name + ": field entry should specify an id or name");
                }
            }
        }

        if (node.get("supertypes") != null && node.get("mixins") != null) {
            throw new JsonFormatException("Only one of 'supertypes' or 'mixins' can be specified " +
                    "(they are synonyms, and mixins is deprecated).");
        }

        if (node.get("supertypes") != null) {
            ArrayNode supertypes = getArray(node, "supertypes", null);
            for (int i = 0; i < supertypes.size(); i++) {
                JsonNode supertype = supertypes.get(i);

                String rtIdString = getString(supertype, "id", null);
                String rtName = getString(supertype, "name", null);
                Long rtVersion = getLong(supertype, "version", null);

                if (rtIdString != null) {
                    recordType.addSupertype(new SchemaIdImpl(rtIdString), rtVersion);
                } else if (rtName != null) {
                    QName rtQName = QNameConverter.fromJson(rtName, namespaces);

                    try {
                        SchemaId rtId = typeManager.getRecordTypeByName(rtQName, null).getId();
                        recordType.addSupertype(rtId, rtVersion);
                    } catch (RepositoryException e) {
                        throw new JsonFormatException("Record type " + name +
                                ": error looking up supertype record type with name: " + rtQName, e);
                    }
                } else {
                    throw new JsonFormatException("Record type " + name + ": supertype should specify an id or name");
                }
            }
        } else if (node.get("mixins") != null) {
            // This was deprecated in 2.2, and can be removed in 2.4
            LogFactory.getLog("lily.deprecation").warn("The use of 'mixins' is deprecated, please use supertypes instead");
            ArrayNode mixins = getArray(node, "mixins", null);
            for (int i = 0; i < mixins.size(); i++) {
                JsonNode mixin = mixins.get(i);

                String rtIdString = getString(mixin, "id", null);
                String rtName = getString(mixin, "name", null);
                Long rtVersion = getLong(mixin, "version", null);

                if (rtIdString != null) {
                    recordType.addMixin(new SchemaIdImpl(rtIdString), rtVersion);
                } else if (rtName != null) {
                    QName rtQName = QNameConverter.fromJson(rtName, namespaces);

                    try {
                        SchemaId rtId = typeManager.getRecordTypeByName(rtQName, null).getId();
                        recordType.addMixin(rtId, rtVersion);
                    } catch (RepositoryException e) {
                        throw new JsonFormatException("Record type " + name + ": error looking up mixin record type with name: " +
                                rtQName, e);
                    }
                } else {
                    throw new JsonFormatException("Record type " + name + ": mixin should specify an id or name");
                }
            }
        }

        return recordType;
    }

    @Override
    public RecordType fromJson(JsonNode node, Namespaces namespaces, Repository repository,
            LinkTransformer linkTransformer) throws JsonFormatException, RepositoryException, InterruptedException {
        return fromJson(node, namespaces, repository);
    }
}
