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
package org.lilyproject.util.repo;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.util.ObjectUtils;
import org.lilyproject.util.json.JsonFormat;

import java.awt.image.IndexColorModel;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Represents the payload of an event about a create-update-delete operation on the repository.
 *
 * <p>The actual payload is json, this class helps in parsing or constructing that json.
 */
public class RecordEvent {
    private long versionCreated = -1;
    private long versionUpdated = -1;
    private Type type;
    private Set<SchemaId> updatedFields;
    private boolean recordTypeChanged = false;
    /** For index-type events: name of the index for which this event matters. */
    private String indexName;
    /** For index-type events: affected vtags */
    private Set<SchemaId> vtagsToIndex;

    public enum Type {
        CREATE("repo:record-created"),
        UPDATE("repo:record-updated"),
        DELETE("repo:record-deleted"),
        INDEX("repo:index");

        private String name;

        private Type(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    public RecordEvent() {
    }

    /**
     * Creates a record event from the json data supplied as bytes.
     */
    public RecordEvent(byte[] data, IdGenerator idGenerator) throws IOException {
        JsonNode msgData = JsonFormat.deserialize(data);

        String messageType = msgData.get("type").getTextValue();
        if (messageType.equals(Type.CREATE.getName())) {
            type = Type.CREATE;
        } else if (messageType.equals(Type.DELETE.getName())) {
            type = Type.DELETE;
        } else if (messageType.equals(Type.UPDATE.getName())) {
            type = Type.UPDATE;
        } else if (messageType.equals(Type.INDEX.getName())) {
            type = Type.INDEX;
        } else {
            throw new RuntimeException("Unexpected kind of message type: " + messageType);
        }

        if (msgData.get("versionCreated") != null) {
            versionCreated = msgData.get("versionCreated").getLongValue();
        }

        if (msgData.get("versionUpdated") != null) {
            versionUpdated = msgData.get("versionUpdated").getLongValue();
        }

        if (msgData.get("recordTypeChanged") != null) {
            recordTypeChanged = msgData.get("recordTypeChanged").getBooleanValue();
        }

        JsonNode updatedFieldsNode = msgData.get("updatedFields");
        if (updatedFieldsNode != null && updatedFieldsNode.size() > 0) {
            for (int i = 0; i < updatedFieldsNode.size(); i++) {
                addUpdatedField(idGenerator.getSchemaId(updatedFieldsNode.get(i).getBinaryValue()));
            }
        }

        if (msgData.get("index") != null) {
            indexName = msgData.get("index").getValueAsText();
        }

        JsonNode vtagsToIndexNode = msgData.get("vtagsToIndex");
        if (vtagsToIndexNode != null && vtagsToIndexNode.size() > 0) {
            for (int i = 0; i < vtagsToIndexNode.size(); i++) {
                addVTagToIndex(idGenerator.getSchemaId(vtagsToIndexNode.get(i).getBinaryValue()));
            }
        }
    }

    public long getVersionCreated() {
        return versionCreated;
    }

    public void setVersionCreated(long versionCreated) {
        this.versionCreated = versionCreated;
    }

    public long getVersionUpdated() {
        return versionUpdated;
    }

    public void setVersionUpdated(long versionUpdated) {
        this.versionUpdated = versionUpdated;
    }

    /**
     * Indicates if the record type of the non-versioned scope changed as part of this event.
     * Should return false for newly created records.
     */
    public boolean getRecordTypeChanged() {
        return recordTypeChanged;
    }

    public void setRecordTypeChanged(boolean recordTypeChanged) {
        this.recordTypeChanged = recordTypeChanged;
    }

    public Type getType() {
        return type;
    }
    
    public void setType(Type type) {
        this.type = type;
    }

    /**
     * The fields which were updated (= added, deleted or changed), identified by their FieldType ID.
     *
     * <p>In case of a delete event, this list is empty.
     */
    public Set<SchemaId> getUpdatedFields() {
        return updatedFields != null ? updatedFields : Collections.<SchemaId>emptySet();
    }

    public void addUpdatedField(SchemaId fieldTypeId) {
        if (updatedFields == null) {
            updatedFields = new HashSet<SchemaId>();
        }
        updatedFields.add(fieldTypeId);
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public Set<SchemaId> getVtagsToIndex() {
        return vtagsToIndex;
    }

    public void addVTagToIndex(SchemaId vtag) {
        if (vtagsToIndex == null) {
            vtagsToIndex = new HashSet<SchemaId>();
        }
        vtagsToIndex.add(vtag);
    }

    public ObjectNode toJson() {
        JsonNodeFactory factory = JsonNodeFactory.instance;
        ObjectNode object = factory.objectNode();

        if (type != null)
            object.put("type", type.getName());

        if (updatedFields != null && updatedFields.size() > 0) {
            ArrayNode updatedFieldsNode = object.putArray("updatedFields");
            for (SchemaId updatedField : updatedFields) {
                updatedFieldsNode.add(updatedField.getBytes());
            }
        }

        if (versionUpdated != -1) {
            object.put("versionUpdated", versionUpdated);
        }

        if (versionCreated != -1) {
            object.put("versionCreated", versionCreated);
        }

        if (recordTypeChanged) {
            object.put("recordTypeChanged", true);
        }

        if (indexName != null) {
            object.put("index", indexName);
        }

        if (vtagsToIndex != null && vtagsToIndex.size() > 0) {
            ArrayNode vtagsToIndexNode = object.putArray("vtagsToIndex");
            for (SchemaId vtag : vtagsToIndex) {
                vtagsToIndexNode.add(vtag.getBytes());
            }
        }

        return object;
    }

    public byte[] toJsonBytes() {
        try {
            return JsonFormat.serializeAsBytes(toJson());
        } catch (IOException e) {
            throw new RuntimeException("Error serializing record event to JSON", e);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RecordEvent other = (RecordEvent)obj;

        if (other.type != this.type)
            return false;

        if (other.recordTypeChanged != this.recordTypeChanged)
            return false;

        if (other.versionCreated != this.versionCreated)
            return false;

        if (other.versionUpdated != this.versionUpdated)
            return false;

        if (!ObjectUtils.safeEquals(other.updatedFields, this.updatedFields))
            return false;

        if (!ObjectUtils.safeEquals(other.indexName, this.indexName))
            return false;

        if (!ObjectUtils.safeEquals(other.vtagsToIndex, this.vtagsToIndex))
            return false;

        return true;
    }
}


