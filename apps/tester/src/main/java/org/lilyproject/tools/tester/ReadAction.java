package org.lilyproject.tools.tester;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.Map.Entry;

import javax.naming.OperationNotSupportedException;

import org.codehaus.jackson.JsonNode;
import org.lilyproject.repository.api.*;
import org.lilyproject.repository.impl.primitivevaluetype.BlobValueType;
import org.lilyproject.util.json.JsonUtil;

public class ReadAction extends AbstractTestAction implements TestAction {
    private Map<QName, FieldType> fieldTypes;

    private boolean readBlobs = false;
    public ReadAction(JsonNode jsonNode, TestActionContext testActionContext) {
        super(jsonNode, testActionContext);
        
        readBlobs = JsonUtil.getBoolean(actionNode, "readBlobs", false);
        
        fieldTypes = new HashMap<QName, FieldType>();
        Collection<TestFieldType> values = testActionContext.fieldTypes.values();
        for (TestFieldType testFieldType : values) {
            FieldType fieldType = testFieldType.getFieldType();
            fieldTypes.put(fieldType.getName(), fieldType);
        }
    }

    public int run() {
        failureCount = 0;
        for (int i = 0; i < count; i++) {
            TestRecord testRecord = testActionContext.records.getRecord(source);

            if (testRecord == null)
                continue;

            long before = System.nanoTime();
            try {
                Record readRecord = testActionContext.repository.read(testRecord.getRecordId());
                long after = System.nanoTime();
                report(true, (int) (after - before), "R", null);
                
                // Read blobs
                if (readBlobs)
                    readBlobs(readRecord);
                
                // if (!readRecord.equals(testRecord.record)) {
                // System.out.println("Read record does not match written record!");
                // }
            } catch (Throwable t) {
                long after = System.nanoTime();
                report(false, (int) (after - before));
                reportError("Error reading record.", t);
            }
        }
        return failureCount;
    }

    private void readBlobs(Record readRecord) throws IOException, RepositoryException, InterruptedException {
        Map<QName, Object> readFields = readRecord.getFields();
        for (Entry<QName, Object> entry : readFields.entrySet()) {
            QName fieldName = entry.getKey();
            FieldType fieldType = fieldTypes.get(fieldName);
            ValueType valueType = fieldType.getValueType();
            PrimitiveValueType primitiveValueType = valueType.getPrimitive();
            if (primitiveValueType instanceof BlobValueType) {
                if (valueType.isMultiValue()) {
                    List<Object> multivalues = (List<Object>)(entry.getValue());
                    int multivalueIndex = randomIndex(multivalues.size());
                    if (valueType.isHierarchical()) {
                        Object[] hierarchyValues = ((HierarchyPath)(multivalues.get(multivalueIndex))).getElements();
                        int hierarchyIndex = randomIndex(hierarchyValues.length);
                        Blob blob = (Blob)hierarchyValues[hierarchyIndex];
                        InputStream inputStream = testActionContext.repository.getInputStream(readRecord, fieldName, multivalueIndex, hierarchyIndex);
                        readBlobBytes(blob, inputStream);
                    } else {
                        Blob blob = (Blob)(multivalues.get(multivalueIndex));
                        InputStream inputStream = testActionContext.repository.getInputStream(readRecord.getId(), readRecord.getVersion(), fieldName, multivalueIndex, null);
                        readBlobBytes(blob, inputStream);
                    }
                } else if (valueType.isHierarchical()) {
                    Object[] hierarchyValues = ((HierarchyPath)(entry.getValue())).getElements();
                    int hierarchyIndex = randomIndex(hierarchyValues.length);
                    Blob blob = (Blob)hierarchyValues[hierarchyIndex];
                    BlobAccess blobAccess = testActionContext.repository.getBlob(readRecord.getId(), readRecord.getVersion(), fieldName, null, hierarchyIndex);
                    InputStream inputStream = blobAccess.getInputStream();
                    readBlobBytes(blob, inputStream);
                } else {
                    Set<Object> values = valueType.getValues(entry.getValue());
                    Blob blob = (Blob)values.toArray()[0];
                    InputStream inputStream = testActionContext.repository.getInputStream(readRecord, fieldName);
                    readBlobBytes(blob, inputStream);
                }
            }
            
        }
    }
    
    private int randomIndex(int arrayLength) {
        return (int)(Math.random() * arrayLength);
    }

    private void readBlobBytes(Blob blob, InputStream inputStream) throws IOException {
        byte[] readBytes = new byte[blob.getSize().intValue()];
        int offset = 0;
        int len = blob.getSize().intValue();
        
        while (true) {
            int amountRead = inputStream.read(readBytes, offset, len);
            if (amountRead == -1) {
                break;
            }
            offset = offset + amountRead;
            len = len - amountRead;
        }
//        System.out.println("Blob read len="+offset+", expected="+blob.getSize());
    }
    
    @Override
    public ActionResult linkFieldAction(TestFieldType testFieldType, RecordId recordId) throws OperationNotSupportedException {
        throw new OperationNotSupportedException();
    }
}
