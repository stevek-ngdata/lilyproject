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
package org.lilyproject.rest.providers.json;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.repository.api.*;
import org.lilyproject.rest.PostAction;
import org.lilyproject.rest.RepositoryEnabled;
import org.lilyproject.rest.ResourceException;
import org.lilyproject.tools.import_.json.*;
import org.lilyproject.util.json.JsonFormat;
import org.lilyproject.util.json.JsonUtil;
import org.lilyproject.util.repo.SystemFields;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;

@Provider
public class PostActionMessageBodyReader extends RepositoryEnabled implements MessageBodyReader<PostAction> {

    public boolean isReadable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        if (type.equals(PostAction.class) && mediaType.equals(MediaType.APPLICATION_JSON_TYPE)) {
            if (genericType instanceof ParameterizedType) {
                ParameterizedType pt = (ParameterizedType)genericType;
                Type[] types = pt.getActualTypeArguments();
                if (types.length == 1 && EntityRegistry.SUPPORTED_TYPES.containsKey(types[0])) {
                    return true;
                }
            }
        }

        return false;
    }

    public PostAction readFrom(Class<PostAction> type, Type genericType, Annotation[] annotations, MediaType mediaType,
            MultivaluedMap<String, String> httpHeaders, InputStream entityStream)
            throws IOException, WebApplicationException {

        Type entityType = ((ParameterizedType)genericType).getActualTypeArguments()[0];

        JsonNode node = JsonFormat.deserializeNonStd(entityStream);

        if (!(node instanceof ObjectNode)) {
            throw new ResourceException("Request body should be a JSON object.", BAD_REQUEST.getStatusCode());
        }

        ObjectNode postNode = (ObjectNode)node;
        String action = JsonUtil.getString(postNode, "action");
        List<MutationCondition> conditions;
        Object entity = null;

        try {
            Namespaces namespaces = NamespacesConverter.fromContextJsonIfAvailable(postNode);

            conditions = readMutationConditions(postNode, namespaces);

            // Hardcoded behavior that action 'delete' does not need a submitted entity (and any other does)
            if (!action.equals("delete")) {
                EntityRegistry.RegistryEntry registryEntry = EntityRegistry.findReaderRegistryEntry((Class)entityType);
                ObjectNode objectNode = JsonUtil.getObject(postNode, registryEntry.getPropertyName());
                entity = EntityRegistry.findReader((Class)entityType).fromJson(objectNode, namespaces, repository);
            }
        } catch (JsonFormatException e) {
            throw new ResourceException("Error in submitted JSON.", e, BAD_REQUEST.getStatusCode());
        } catch (Exception e) {
            throw new ResourceException("Error reading submitted JSON.", e, INTERNAL_SERVER_ERROR.getStatusCode());
        }

        return new PostAction(action, entity, conditions);
    }

    private List<MutationCondition> readMutationConditions(ObjectNode postNode, Namespaces namespaces) throws JsonFormatException, RepositoryException, InterruptedException {
        ArrayNode conditions = JsonUtil.getArray(postNode, "conditions", null);
        if (conditions == null) {
            return null;
        }

        List<MutationCondition> result = new ArrayList<MutationCondition>();
        SystemFields systemFields = SystemFields.getInstance(repository.getTypeManager(), repository.getIdGenerator());

        for (int i = 0; i < conditions.size(); i++) {
            JsonNode conditionNode = conditions.get(i);
            if (!conditionNode.isObject()) {
                throw new JsonFormatException("Each element in the conditions array should be an object.");
            }

            QName fieldName = QNameConverter.fromJson(JsonUtil.getString(conditionNode, "field"), namespaces);

            JsonNode valueNode = conditionNode.get("value");
            Object value = null;
            if (!valueNode.isNull()) {
                FieldType fieldType = systemFields.isSystemField(fieldName) ? systemFields.get(fieldName) :
                        repository.getTypeManager().getFieldTypeByName(fieldName);
                value = RecordReader.INSTANCE.readValue(valueNode, fieldType.getValueType(), "value", repository);
            }

            boolean allowMissing = JsonUtil.getBoolean(conditionNode, "allowMissing", false);

            String operator = JsonUtil.getString(conditionNode, "operator", null);
            CompareOp op = CompareOp.EQUAL;
            if (operator != null) {
                try {
                    op = CompareOp.valueOf(operator.toUpperCase());
                } catch (IllegalArgumentException e) {
                    throw new JsonFormatException("Invalid comparison operator in mutation condition: " + operator);
                }
            }

            MutationCondition condition = new MutationCondition(fieldName, op, value, allowMissing);
            result.add(condition);
        }

        return result;
    }

}
