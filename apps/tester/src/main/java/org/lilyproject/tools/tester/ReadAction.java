package org.lilyproject.tools.tester;

import javax.naming.OperationNotSupportedException;

import org.codehaus.jackson.JsonNode;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordId;

public class ReadAction extends AbstractTestAction implements TestAction {
    public ReadAction(JsonNode jsonNode, TestActionContext testActionContext) {
        super(jsonNode, testActionContext);
    }

    public int run() {
        for (int i = 0; i < count; i++) {
            TestRecord testRecord = testActionContext.records.getRecord(source);

            if (testRecord == null)
                continue;

            long before = System.nanoTime();
            try {
                Record readRecord = testActionContext.repository.read(testRecord.getRecordId());
                long after = System.nanoTime();
                report(true, (int) (after - before));
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
    
    @Override
    public ActionResult linkFieldAction(TestFieldType testFieldType, RecordId recordId) throws OperationNotSupportedException {
        throw new OperationNotSupportedException();
    }
}
