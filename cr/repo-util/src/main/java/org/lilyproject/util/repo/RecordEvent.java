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

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.util.ByteArrayBuilder;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.util.ObjectUtils;
import org.lilyproject.util.json.JsonFormat;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
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
    private IndexSelection indexSelection;

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
        // Using streaming JSON parsing for performance. We expect the JSON to be correct, validation
        // is absent/minimal.

        JsonParser jp = JsonFormat.JSON_FACTORY.createJsonParser(data);

        JsonToken current;
        current = jp.nextToken();

        if (current != JsonToken.START_OBJECT) {
            throw new RuntimeException("Not a JSON object.");
        }

        while (jp.nextToken() != JsonToken.END_OBJECT) {
            String fieldName = jp.getCurrentName();
            current = jp.nextToken(); // move from field name to field value
            if (fieldName.equals("type")) {
                String messageType = jp.getText();
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
            } else if (fieldName.equals("versionCreated")) {
                versionCreated = jp.getLongValue();
            } else if (fieldName.equals("versionUpdated")) {
                versionUpdated = jp.getLongValue();
            } else if (fieldName.equals("recordTypeChanged")) {
                recordTypeChanged = jp.getBooleanValue();
            } else if (fieldName.equals("index")) {
                indexName = jp.getText();
            } else if (fieldName.equals("updatedFields")) {
                if (current != JsonToken.START_ARRAY) {
                    throw new RuntimeException("updatedFields is not a JSON array");
                }
                while (jp.nextToken() != JsonToken.END_ARRAY) {
                    addUpdatedField(idGenerator.getSchemaId(jp.getBinaryValue()));
                }
            } else if (fieldName.equals("vtagsToIndex")) {
                if (current != JsonToken.START_ARRAY) {
                    throw new RuntimeException("vtagsToIndex is not a JSON array");
                }
                while (jp.nextToken() != JsonToken.END_ARRAY) {
                    addVTagToIndex(idGenerator.getSchemaId(jp.getBinaryValue()));
                }
            } else if (fieldName.equals("indexSelection")) {
                this.indexSelection = new IndexSelection(jp, idGenerator);
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

    public IndexSelection getIndexSelection() {
        return indexSelection;
    }

    public void setIndexSelection(IndexSelection indexSelection) {
        this.indexSelection = indexSelection;
    }

    public void toJson(JsonGenerator gen) throws IOException {
        gen.writeStartObject();

        if (type != null) {
            gen.writeStringField("type", type.getName());
        }

        if (versionUpdated != -1) {
            gen.writeNumberField("versionUpdated", versionUpdated);
        }

        if (versionCreated != -1) {
            gen.writeNumberField("versionCreated", versionCreated);
        }

        if (recordTypeChanged) {
            gen.writeBooleanField("recordTypeChanged", true);
        }

        if (indexName != null) {
            gen.writeStringField("index", indexName);
        }

        if (updatedFields != null && updatedFields.size() > 0) {
            gen.writeArrayFieldStart("updatedFields");
            for (SchemaId updatedField : updatedFields) {
                gen.writeBinary(updatedField.getBytes());
            }
            gen.writeEndArray();
        }

        if (vtagsToIndex != null && vtagsToIndex.size() > 0) {
            gen.writeArrayFieldStart("vtagsToIndex");
            for (SchemaId vtag : vtagsToIndex) {
                gen.writeBinary(vtag.getBytes());
            }
            gen.writeEndArray();
        }

        if (indexSelection != null) {
            gen.writeFieldName("indexSelection");
            indexSelection.toJson(gen);
        }

        gen.writeEndObject();
        gen.flush();
    }

    public String toJson() {
        try {
            StringWriter writer = new StringWriter();
            JsonGenerator gen = JsonFormat.JSON_FACTORY.createJsonGenerator(writer);
            toJson(gen);
            return writer.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] toJsonBytes() {
        try {
            ByteArrayBuilder bb = new ByteArrayBuilder(JsonFormat.JSON_FACTORY._getBufferRecycler());
            JsonGenerator gen = JsonFormat.JSON_FACTORY.createJsonGenerator(bb, JsonEncoding.UTF8);
            toJson(gen);
            byte[] result = bb.toByteArray();
            bb.release();
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
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

        // TODO implement equals for IndexSelection

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (versionCreated ^ (versionCreated >>> 32));
        result = 31 * result + (int) (versionUpdated ^ (versionUpdated >>> 32));
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (updatedFields != null ? updatedFields.hashCode() : 0);
        result = 31 * result + (recordTypeChanged ? 1 : 0);
        result = 31 * result + (indexName != null ? indexName.hashCode() : 0);
        result = 31 * result + (vtagsToIndex != null ? vtagsToIndex.hashCode() : 0);
        return result;
    }

    /**
     * Information needed by MQFeeder to know what indexer subscriptions should receive what
     * events. At the time of this writing, the indexerconf only allows to expression selection
     * based on record type, on 1 STRING-type field, and on information that is part of the record id.
     * The model here is already a bit more flexible (can contain info on multiple fields) to allow for
     * more powerful selections in the future.
     */
    public static class IndexSelection {
        private SchemaId newRecordType;
        private SchemaId oldRecordType;
        private List<FieldChange> fieldChanges;

        public IndexSelection() {
        }

        public IndexSelection(JsonParser jp, IdGenerator idGenerator) throws IOException {
            JsonToken current = jp.getCurrentToken();

            if (current != JsonToken.START_OBJECT) {
                throw new RuntimeException("Not a JSON object.");
            }

            while (jp.nextToken() != JsonToken.END_OBJECT) {
                String fieldName = jp.getCurrentName();
                current = jp.nextToken(); // move from field name to field value
                if (fieldName.equals("newRecordType")) {
                    newRecordType = idGenerator.getSchemaId(jp.getBinaryValue());
                } else if (fieldName.equals("oldRecordType")) {
                    oldRecordType = idGenerator.getSchemaId(jp.getBinaryValue());
                } else if (fieldName.equals("fields")) {
                    if (current != JsonToken.START_ARRAY) {
                        throw new RuntimeException("updatedFields is not a JSON array");
                    }
                    fieldChanges = new ArrayList<FieldChange>();
                    while (jp.nextToken() != JsonToken.END_ARRAY) {
                        fieldChanges.add(new FieldChange(jp, idGenerator));
                    }
                }
            }
        }

        public SchemaId getNewRecordType() {
            return newRecordType;
        }

        public void setNewRecordType(SchemaId newRecordType) {
            this.newRecordType = newRecordType;
        }

        public SchemaId getOldRecordType() {
            return oldRecordType;
        }

        public void setOldRecordType(SchemaId oldRecordType) {
            this.oldRecordType = oldRecordType;
        }

        public void addChangedField(SchemaId id, byte[] oldValue, byte[] newValue) {
            if (fieldChanges == null) {
                fieldChanges = new ArrayList<FieldChange>();
            }
            fieldChanges.add(new FieldChange(id, oldValue, newValue));
        }

        public List<FieldChange> getFieldChanges() {
            return fieldChanges;
        }

        public void toJson(JsonGenerator gen) throws IOException {
            gen.writeStartObject();

            if (newRecordType != null) {
                gen.writeBinaryField("newRecordType", newRecordType.getBytes());
            }

            if (oldRecordType != null) {
                gen.writeBinaryField("oldRecordType", oldRecordType.getBytes());
            }

            if (fieldChanges != null) {
                gen.writeArrayFieldStart("fields");

                for (FieldChange fieldChange : fieldChanges) {
                    fieldChange.toJson(gen);
                }

                gen.writeEndArray();
            }

            gen.writeEndObject();
        }
    }

    public static class FieldChange {
        private SchemaId id;
        private byte[] oldValue;
        private byte[] newValue;

        public FieldChange(SchemaId id, byte[] oldValue, byte[] newValue) {
            this.id = id;
            this.oldValue = oldValue;
            this.newValue = newValue;
        }

        public FieldChange(JsonParser jp, IdGenerator idGenerator) throws IOException {
            JsonToken current = jp.getCurrentToken();

            if (current != JsonToken.START_OBJECT) {
                throw new RuntimeException("Not a JSON object.");
            }

            while (jp.nextToken() != JsonToken.END_OBJECT) {
                String fieldName = jp.getCurrentName();
                current = jp.nextToken(); // move from field name to field value
                if (fieldName.equals("id")) {
                    this.id = idGenerator.getSchemaId(jp.getBinaryValue());
                } else if (fieldName.equals("old")) {
                    oldValue = jp.getBinaryValue();
                } else if (fieldName.equals("new")) {
                    newValue = jp.getBinaryValue();
                }
            }
        }

        public SchemaId getId() {
            return id;
        }

        public byte[] getOldValue() {
            return oldValue;
        }

        public byte[] getNewValue() {
            return newValue;
        }

        public void toJson(JsonGenerator gen) throws IOException {
            gen.writeStartObject();

            gen.writeBinaryField("id", id.getBytes());

            if (oldValue != null) {
                gen.writeBinaryField("old", oldValue);
            }

            if (newValue != null) {
                gen.writeBinaryField("new", newValue);
            }

            gen.writeEndObject();
        }
    }
}


