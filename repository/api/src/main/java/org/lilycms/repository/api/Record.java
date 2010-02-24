package org.lilycms.repository.api;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Record {
    
    private String recordId;
    private Map<String, Field> fields = new HashMap<String, Field>();
    private Set<String> deleteFields = new HashSet<String>();

    public Record(String recordId) {
        this.recordId = recordId;
    }
    
    public void addField(Field field) {
        fields.put(field.getName(), field);
    }
    
    public Field getField(String fieldName) {
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
        result = prime * result + ((fields == null) ? 0 : fields.hashCode());
        result = prime * result + ((recordId == null) ? 0 : recordId.hashCode());
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
        Record other = (Record) obj;
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
        return true;
    }

    @Override
    public String toString() {
        return "["+recordId+", "+getFields()+"]";
    }
}
