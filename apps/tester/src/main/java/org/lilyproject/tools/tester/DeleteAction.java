package org.lilyproject.tools.tester;

import javax.naming.OperationNotSupportedException;

import org.codehaus.jackson.JsonNode;
import org.lilyproject.repository.api.RecordId;

public class DeleteAction extends AbstractTestAction implements TestAction {
    public DeleteAction(JsonNode jsonNode, TestActionContext testActionContext) {
        super(jsonNode, testActionContext);
    }

    @Override
    public int run() {
        failureCount = 0;
        for (int i = 0; i < count; i++) {
            TestRecord testRecord = testActionContext.records.getRecord(source);

            if (testRecord == null)
                continue;

            long before = System.nanoTime();
            try {
                testActionContext.repository.delete(testRecord.getRecordId());
                report(true, System.nanoTime() - before, "D", null);
                testActionContext.records.removeRecord(source, testRecord);
                testRecord.setDeleted(true);
                testActionContext.records.addRecord(destination, new TestRecord(testRecord.getRecordId(), testRecord.getRecordType()));
            } catch (Throwable t) {
                report(false, System.nanoTime() - before);
                reportError("Error deleting record.", t);
            }
        }
        return failureCount;
    }

    @Override
    public ActionResult linkFieldAction(TestFieldType testFieldType, RecordId recordId) throws OperationNotSupportedException {
        throw new OperationNotSupportedException();
    }
}
