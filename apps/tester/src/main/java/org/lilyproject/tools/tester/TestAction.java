package org.lilyproject.tools.tester;

import javax.naming.OperationNotSupportedException;

import org.lilyproject.repository.api.RecordId;



public interface TestAction {

    int run();
    TestActionContext getContext();
    ActionResult linkFieldAction(TestFieldType testFieldType, RecordId recordId) throws OperationNotSupportedException;
}
