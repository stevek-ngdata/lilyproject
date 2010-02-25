package org.lilycms.repository.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.lilycms.repository.api.Field;
import org.lilycms.repository.api.FieldNotFoundException;
import org.lilycms.repository.api.Record;

public class RecordImpl implements Record {
    private String recordId;
    private Map<String, Field> fields = new HashMap<String, Field>();
    private Set<String> deleteFields = new HashSet<String>();
    private String recordTypeName;
    private long recordTypeVersion;

    
    public RecordImpl(String recordId) {
        this.recordId = recordId;
    }
    
    public void setRecordType(String recordTypeName, long recordTypeVersion) {
        this.recordTypeName = recordTypeName;
        this.recordTypeVersion = recordTypeVersion;
    }
    
    public String getRecordTypeName() {
        return recordTypeName;
    }
    
    public long getRecordTypeVersion() {
        return recordTypeVersion;
    }
    
    public void addField(Field field) {
        fields.put(field.getName(), field);
    }
    
    public Field getField(String fieldName) throws FieldNotFoundException {
        Field field = fields.get(fieldName);
        if (field == null) {
            throw new FieldNotFoundException(fieldName);
        }
        return fields.get(fieldName);
    }
    
    public Set<Field> getFields() {
        return new HashSet<Field>(fields.values());
    }
    
    public void setRecordId(String recordId) {
        this.recordId = recordId;
    }

    public String getRecordId() {
        return recordId;
    }
    
    public void deleteField(String fieldName) {
        deleteFields.add(fieldName);
    }
    
    public Set<String> getDeleteFields() {
        return deleteFields;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((deleteFields == null) ? 0 : deleteFields.hashCode());
        result = prime * result + ((fields == null) ? 0 : fields.hashCode());
        result = prime * result + ((recordId == null) ? 0 : recordId.hashCode());
        result = prime * result + ((recordTypeName == null) ? 0 : recordTypeName.hashCode());
        result = prime * result + (int) (recordTypeVersion ^ (recordTypeVersion >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RecordImpl other = (RecordImpl) obj;
        if (deleteFields == null) {
            if (other.deleteFields != null)
                return false;
        } else if (!deleteFields.equals(other.deleteFields))
            return false;
        if (fields == null) {
            if (other.fields != null)
                return false;
        } else if (!fields.equals(other.fields))
            return false;
        if (recordId == null) {
            if (other.recordId != null)
                return false;
        } else if (!recordId.equals(other.recordId))
            return false;
        if (recordTypeName == null) {
            if (other.recordTypeName != null)
                return false;
        } else if (!recordTypeName.equals(other.recordTypeName))
            return false;
        if (recordTypeVersion != other.recordTypeVersion)
            return false;
        return true;
    }

    @Override
    public String toString() {
        StringBuffer stringBuffer= new StringBuffer();
        stringBuffer.append("[");
        stringBuffer.append(recordId).append(", ");
        stringBuffer.append(recordTypeName).append(", ");
        stringBuffer.append(recordTypeVersion).append(", ");
        stringBuffer.append(getFields());
        stringBuffer.append("]");
        return stringBuffer.toString();
    }
}