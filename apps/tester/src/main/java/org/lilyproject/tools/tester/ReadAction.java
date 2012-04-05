/*
 * Copyright 2012 NGDATA nv
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
package org.lilyproject.tools.tester;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.lang.ArrayUtils;
import org.codehaus.jackson.JsonNode;
import org.lilyproject.repository.api.*;
import org.lilyproject.repository.impl.valuetype.BlobValueType;
import org.lilyproject.util.json.JsonUtil;

public class ReadAction extends AbstractTestAction implements TestAction {

    private boolean readBlobs = false;
    public ReadAction(JsonNode jsonNode, TestActionContext testActionContext) {
        super(jsonNode, testActionContext);
        readBlobs = JsonUtil.getBoolean(actionNode, "readBlobs", false);
    }

    @Override
    protected void runAction() {
        TestRecord testRecord = testActionContext.records.getRecord(source);

        if (testRecord == null)
            return;

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

    private void readBlobs(Record readRecord) throws IOException, RepositoryException, InterruptedException {
        Map<QName, Object> readFields = readRecord.getFields();
        for (Entry<QName, Object> entry : readFields.entrySet()) {
            QName fieldName = entry.getKey();
            FieldType fieldType = testActionContext.fieldTypes.get(fieldName).getFieldType();
            ValueType valueType = fieldType.getValueType();
            if (valueType.getDeepestValueType() instanceof BlobValueType) {
                readBlobs(readRecord, fieldName, entry.getValue(), valueType);
            }
        }
    }
    
    private void readBlobs(Record readRecord, QName fieldName, Object value, ValueType valueType, int... indexes)
            throws RepositoryException, InterruptedException, IOException {
        if (valueType.getBaseName().equals("LIST")) {
            List<Object> multivalues = (List<Object>) (value);
            int multivalueIndex = randomIndex(multivalues.size());
            Object subValue = (multivalues.get(multivalueIndex));
            indexes = ArrayUtils.add(indexes, multivalueIndex);
            readBlobs(readRecord, fieldName, subValue, valueType.getNestedValueType(), indexes);
        } else if (valueType.getBaseName().equals("PATH")) {
            Object[] hierarchyValues = ((HierarchyPath) (value)).getElements();
            int hierarchyIndex = randomIndex(hierarchyValues.length);
            Object subValue = hierarchyValues[hierarchyIndex];
            indexes = ArrayUtils.add(indexes, hierarchyIndex);
            readBlobs(readRecord, fieldName, subValue, valueType.getNestedValueType(), indexes);
        } else {
            Blob blob = (Blob) value;
            InputStream inputStream = testActionContext.repository.getInputStream(readRecord, fieldName, indexes);
            readBlobBytes(blob, inputStream);
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
    public ActionResult linkFieldAction(TestFieldType testFieldType, RecordId recordId) {
        throw new UnsupportedOperationException();
    }
}
