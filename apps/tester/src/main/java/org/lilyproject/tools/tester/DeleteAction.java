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
