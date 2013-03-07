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

import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.lilyproject.repository.api.Link;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordException;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.tools.import_.json.JsonFormatException;
import org.lilyproject.tools.import_.json.QNameConverter;
import org.lilyproject.util.json.JsonUtil;

public class UpdateAction extends AbstractTestAction implements TestAction {
    private String pattern;
    private String patternDetail;

    public UpdateAction(JsonNode actionNode, TestActionContext testActionContext) {
        super(actionNode, testActionContext);
        pattern = JsonUtil.getString(actionNode, "pattern", "all");
        patternDetail = JsonUtil.getString(actionNode, "patternDetail", null);
    }

    @Override
    protected void runAction() {
        TestRecord testRecord = testActionContext.records.getRecord(source);

        if (testRecord == null) {
            return;
        }

        TestRecordType recordTypeToUpdate = testRecord.getRecordType();
        RecordId recordId = testRecord.getRecordId();
        ActionResult result = updateRecord(recordTypeToUpdate, recordId);
        report(result.success, result.duration, "U", null);
        if (result.success) {
            testActionContext.records.addRecord(destination, new TestRecord(((Record)result.object).getId(),
                    recordTypeToUpdate));
        }
    }

    private ActionResult updateRecord(TestRecordType recordTypeToUpdate, RecordId recordId) {
        double duration = 0;
        long before = 0;
        List<TestFieldType> fieldsToUpdate = new ArrayList<TestFieldType>();
        // Select x random fields to update
        if ("random".equals(pattern)) {
            List<TestFieldType> recordFields = recordTypeToUpdate.getFieldTypes();
            for (int i = 0; i < Integer.valueOf(patternDetail); i++) {
                int selectedField = (int) Math.floor(Math.random() * recordFields.size());
                fieldsToUpdate.add(recordFields.get(selectedField));
            }
        // Select specified fields to update
        } else if ("fields".equals(pattern)) {
            String[] fieldNames = patternDetail.split(",");
            List<QName> fieldQNames = new ArrayList<QName>(fieldNames.length);
            for (String fieldName: fieldNames) {
                try {
                    fieldQNames.add(QNameConverter.fromJson(fieldName, testActionContext.nameSpaces));
                } catch (JsonFormatException e) {
                    throw new RuntimeException("Error updating record", e);
                }
            }
            for (TestFieldType testFieldType : recordTypeToUpdate.getFieldTypes()) {
                if (fieldQNames.contains(testFieldType.getFieldType().getName())) {
                    fieldsToUpdate.add(testFieldType);
                }
            }
        }
        // Update all fields
        else {
            fieldsToUpdate.addAll(recordTypeToUpdate.getFieldTypes());
        }

        Record record = null;
        try {
            record = testActionContext.repository.newRecord(recordId);
        } catch (RecordException e) {
            reportError("Error preparing update record.", e);
            return new ActionResult(false, null, 0);
        }

        // If there is a Link-field that links to specified record type we need to read the field
        // in order to update the record that is linked to
        boolean readRecord = false;
        for (TestFieldType field: fieldsToUpdate) {
            if (field.getLinkedRecordTypeName() != null || field.getLinkedRecordSource() != null) {
                readRecord = true;
                break;
            }
        }
        Record originalRecord = null;
        if (readRecord == true) {
            before = System.nanoTime();
            try {
                originalRecord = testActionContext.repository.read(record.getId());
                long readDuration = System.nanoTime() - before;
                report(true, readDuration, "repoRead");
                duration += readDuration;
            } catch (Throwable t) {
                long readDuration = System.nanoTime() - before;
                report(false, readDuration, "readLinkFields");
                reportError("Error updating subrecord.", t);
                duration += readDuration;
                return new ActionResult(false, null, duration);
            }
        }

        // Prepare the record with updated field values
        for (TestFieldType testFieldType : fieldsToUpdate) {
            ActionResult result = testFieldType.updateValue(this, originalRecord);
            duration += result.duration;
            if (!result.success) {
                return new ActionResult(false, null, duration);
            }
            // In case of a link field to a specific recordType we only update that record, but not the link itself
            if (result.object != null) {
                record.setField(testFieldType.fieldType.getName(), result.object);
            }
        }

        boolean success;
        before = System.nanoTime();
        long updateDuration = 0;
        try {
            record = testActionContext.repository.update(record);
            updateDuration = System.nanoTime() - before;
            success = true;
        } catch (Throwable t) {
            updateDuration = System.nanoTime() - before;
            success = false;
            reportError("Error updating record.", t);
        }
        duration += updateDuration;
        report(success, updateDuration, "repoUpdate."+recordTypeToUpdate.getRecordType().getName().getName());
        return new ActionResult(success, record, duration);
    }

    @Override
    public ActionResult linkFieldAction(TestFieldType testFieldType, RecordId recordId) {
        double duration = 0;
        String linkedRecordSource = testFieldType.getLinkedRecordSource();
        // Update the record where the link points to
        String linkedRecordTypeName = testFieldType.getLinkedRecordTypeName();
        if (linkedRecordTypeName != null) {
            TestRecordType linkedRecordType;
            try {
                linkedRecordType = testActionContext.recordTypes.get(QNameConverter.fromJson(linkedRecordTypeName,
                        testActionContext.nameSpaces));
            } catch (JsonFormatException e) {
                throw new RuntimeException("Error updating link field", e);
            }
            ActionResult result = updateRecord(linkedRecordType, recordId);
            report(result.success, result.duration, "linkUpdate."+linkedRecordType.getRecordType().getName().getName());
            duration += result.duration;
            if (!result.success) {
                return new ActionResult(false, null, duration);
            }
            // We updated the record that was linked to but not the linkfield itself. So we return null in the ActionResult.
            return new ActionResult(true, null, duration);
        }
        // Pick a link from the RecordSpace source
        if (linkedRecordSource != null) {
            TestRecord record = testActionContext.records.getRecord(linkedRecordSource);
            if (record == null) {
                return new ActionResult(false, null, 0);
            }
            return new ActionResult(true, new Link(record.getRecordId()), 0);
        }
        // Generate a link that possibly (most likely) points to a non-existing record
        return new ActionResult(true, testFieldType.generateLink(), 0);
    }
}
