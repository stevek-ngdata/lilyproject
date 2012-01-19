package org.lilyproject.tools.tester;

import org.codehaus.jackson.JsonNode;
import org.lilyproject.repository.api.RecordId;

public class DeleteAction extends AbstractTestAction implements TestAction {
    public DeleteAction(JsonNode jsonNode, TestActionContext testActionContext) {
        super(jsonNode, testActionContext);
    }

    @Override
    protected void runAction() {
        TestRecord testRecord = testActionContext.records.getRecord(source);

        if (testRecord == null)
            return;

        long before = System.nanoTime();
        try {
            testActionContext.repository.delete(testRecord.getRecordId());
            report(true, System.nanoTime() - before, "D", null);
            testActionContext.records.removeRecord(source, testRecord);
            testRecord.setDeleted(true);
            testActionContext.records.addRecord(destination,
                    new TestRecord(testRecord.getRecordId(), testRecord.getRecordType()));
        } catch (Throwable t) {
            report(false, System.nanoTime() - before);
            reportError("Error deleting record.", t);
        }
    }

    @Override
    public ActionResult linkFieldAction(TestFieldType testFieldType, RecordId recordId) {
        throw new UnsupportedOperationException();
    }
}
