package org.lilyproject.tools.tester;

import java.util.ArrayList;
import java.util.List;

import org.lilyproject.repository.api.RecordType;

public class TestRecordType {

    private RecordType recordType;
    private List<TestFieldType> fieldTypes = new ArrayList<TestFieldType>();
    
    public TestRecordType() {
    }

    public void setRecordType(RecordType recordType) {
        this.recordType = recordType;
    }
    
    public RecordType getRecordType() {
        return recordType;
    }
    
    public void addFieldType(TestFieldType fieldType) {
        fieldTypes.add(fieldType);
    }
    
    public List<TestFieldType> getFieldTypes() {
        return fieldTypes;
    }
} 
