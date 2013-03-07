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

import static org.lilyproject.util.json.JsonUtil.getString;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.api.ValueType;
import org.lilyproject.repository.impl.id.SchemaIdImpl;
import org.lilyproject.util.repo.VersionTag;

public class FieldTypeReader implements EntityReader<FieldType> {
    public static EntityReader<FieldType> INSTANCE = new FieldTypeReader();

    @Override
    public FieldType fromJson(JsonNode node, RepositoryManager repositoryManager) throws JsonFormatException, RepositoryException, InterruptedException {
        return fromJson(node, null, repositoryManager);
    }

    @Override
    public FieldType fromJson(JsonNode nodeNode, Namespaces namespaces, RepositoryManager repositoryManager)
            throws JsonFormatException, RepositoryException, InterruptedException {

        if (!nodeNode.isObject()) {
            throw new JsonFormatException("Expected a json object for field type, got: " +
                    nodeNode.getClass().getName());
        }

        ObjectNode node = (ObjectNode)nodeNode;

        namespaces = NamespacesConverter.fromContextJson(node, namespaces);

        QName name = QNameConverter.fromJson(getString(node, "name"), namespaces);


        String scopeName = getString(node, "scope", "non_versioned");
        Scope scope = parseScope(scopeName);

        TypeManager typeManager = repositoryManager.getTypeManager();

        // Be gentle to users of Lily 1.0
        if (node.has("valueType") && node.get("valueType").isObject() && node.get("valueType").has("primitive")) {
            throw new JsonFormatException("Lily 1.1 format change: the valueType should now be specified as a string" +
                    " rather than a nested object with the 'primitive' property.");
        }

        String valueTypeString = getString(node, "valueType");
        ValueType valueType = typeManager.getValueType(ValueTypeNSConverter.fromJson(valueTypeString, namespaces));
        FieldType fieldType = typeManager.newFieldType(valueType, name, scope);

        String idString = getString(node, "id", null);
        SchemaId id = null;
        if (idString != null) {
            id = new SchemaIdImpl(idString);
        }
        fieldType.setId(id);

        // Some sanity checks for version tag fields
        if (fieldType.getName().getNamespace().equals(VersionTag.NAMESPACE)) {
            if (fieldType.getScope() != Scope.NON_VERSIONED) {
                throw new JsonFormatException("vtag fields should be in the non-versioned scope");
            }

            if (!fieldType.getValueType().getBaseName().equals("LONG")) {
                throw new JsonFormatException("vtag fields should be of type LONG");
            }
        }

        return fieldType;
    }

    private static Scope parseScope(String scopeName) {
        scopeName = scopeName.toLowerCase();
        if (scopeName.equals("non_versioned")) {
            return Scope.NON_VERSIONED;
        } else if (scopeName.equals("versioned")) {
            return Scope.VERSIONED;
        } else if (scopeName.equals("versioned_mutable")) {
            return Scope.VERSIONED_MUTABLE;
        } else {
            throw new RuntimeException("Unrecognized scope name: " + scopeName);
        }
    }

    @Override
    public FieldType fromJson(JsonNode node, Namespaces namespaces, RepositoryManager repositoryManager,
            LinkTransformer linkTransformer) throws JsonFormatException, RepositoryException, InterruptedException {
        return fromJson(node, namespaces, repositoryManager);
    }
}
