package org.lilyproject.tools.tester;

import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.lilyproject.repository.api.Link;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordException;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.tools.import_.json.QNameConverter;
import org.lilyproject.util.json.JsonFormatException;
import org.lilyproject.util.json.JsonUtil;

public class CreateAction extends AbstractTestAction implements TestAction {

    private TestRecordType recordTypeToCreate;

    public CreateAction(JsonNode actionNode, TestActionContext testActionContext) throws JsonFormatException,
            org.lilyproject.tools.import_.json.JsonFormatException {
        super(actionNode, testActionContext);
        recordTypeToCreate = testActionContext.recordTypes.get(QNameConverter.fromJson(
                JsonUtil.getString(actionNode, "recordType"), testActionContext.nameSpaces));
    }
    
    @Override
    public int run() {
        failureCount = 0;
        for (int i = 0; i < count; i++) {
            ActionResult result = createRecord(recordTypeToCreate);
            report(result.success, result.duration);
            if (result.success)
                testActionContext.records.addRecord(destination, new TestRecord(((Record)result.object).getId(), recordTypeToCreate));
        }
        return failureCount;
    }
    
    private ActionResult createRecord(TestRecordType recordTypeToCreate) {
        double duration = 0;
        Record record;
        try {
            record = testActionContext.repository.newRecord();
        } catch (RecordException e) {
            reportError("Error preparing create record.", e);
            return new ActionResult(false, null, 0);
        }
        
        // Prepare record by generating values for the fields
        record.setRecordType(recordTypeToCreate.getRecordType().getName());
        List<TestFieldType> fieldTypesToCreate = recordTypeToCreate.getFieldTypes();
        for (TestFieldType testFieldType : fieldTypesToCreate) {
            QName fieldQName = testFieldType.getFieldType().getName();
            ActionResult result = testFieldType.generateValue(this);
            duration += result.duration;
            if (!result.success) {
                return new ActionResult(false, null, duration);
            }
            record.setField(fieldQName, result.object);
        }

        // Perform the actual creation of the record on the repository
        boolean success;
        long before = System.nanoTime();
        long createDuration = 0;
        try {
            record = testActionContext.repository.create(record);
            createDuration = System.nanoTime() - before;
            success = true;
        } catch (Throwable t) {
            createDuration = System.nanoTime() - before; 
            reportError("Error creating record.", t);
            success = false;
        }
        report(success, createDuration, "C", "repoCreate." + recordTypeToCreate.getRecordType().getName().getName());
        duration += createDuration;
        return new ActionResult(success, record, duration);
    }
    
    @Override
    public ActionResult linkFieldAction(TestFieldType testFieldType, RecordId recordId) {
        String linkedRecordSource = testFieldType.getLinkedRecordSource();
        // Pick a link from the RecordSpace source
        if (linkedRecordSource != null) {
            TestRecord record = testActionContext.records.getRecord(linkedRecordSource);
            if (record == null)
                return new ActionResult(false, null, 0);
            return new ActionResult(true, new Link(record.getRecordId()), 0);
        }
        // Create a record of the specified RecordType
        String linkedRecordTypeName = testFieldType.getLinkedRecordTypeName();
        if (linkedRecordTypeName != null) {
            TestRecordType linkedRecordType;
            try {
                linkedRecordType = testActionContext.recordTypes.get(QNameConverter.fromJson(linkedRecordTypeName,
                        testActionContext.nameSpaces));
            } catch (org.lilyproject.tools.import_.json.JsonFormatException e) {
                throw new RuntimeException("Error creating link field", e);
            }
                ActionResult result = createRecord(linkedRecordType);
                report(result.success, result.duration, "linkCreate."+linkedRecordType.getRecordType().getName().getName());
                if (!result.success)
                    return new ActionResult(false, null, 0);
                return new ActionResult(true, new Link(((Record)result.object).getId()), result.duration);
        }
        // Generate a link that possibly (most likely) points to a non-existing record
        return new ActionResult(true, testFieldType.generateLink(), 0);
    }
}
