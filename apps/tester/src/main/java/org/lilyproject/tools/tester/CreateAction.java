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
import java.net.NetworkInterface;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.lilyproject.repository.api.Link;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.tools.import_.json.QNameConverter;
import org.lilyproject.util.json.JsonFormatException;
import org.lilyproject.util.json.JsonUtil;

public class CreateAction extends AbstractTestAction implements TestAction {

    private TestRecordType recordTypeToCreate;

    private static final byte[] MAC_ADDRESS;

    static {
        byte[] macAddress = null;
        try {
            for (Iterator<NetworkInterface> iterator =
                         Collections.list(NetworkInterface.getNetworkInterfaces()).iterator();
                 iterator.hasNext() && macAddress == null; ) {
                macAddress = iterator.next().getHardwareAddress();
            }
        } catch (IOException e) {
            throw new IllegalStateException("failed to initialize localhost mac address", e);
        }

        if (macAddress == null) {
            throw new IllegalStateException("failed to initialize localhost mac address");
        }
        MAC_ADDRESS = macAddress;
    }

    public CreateAction(JsonNode actionNode, TestActionContext testActionContext) throws JsonFormatException,
            org.lilyproject.tools.import_.json.JsonFormatException {
        super(actionNode, testActionContext);
        recordTypeToCreate = testActionContext.recordTypes.get(QNameConverter.fromJson(
                JsonUtil.getString(actionNode, "recordType"), testActionContext.nameSpaces));
    }

    @Override
    protected void runAction() {
        ActionResult result = createRecord(recordTypeToCreate);
        report(result.success, result.duration);
        if (result.success) {
            testActionContext.records.addRecord(destination, new TestRecord(((Record)result.object).getId(),
                    recordTypeToCreate));
        }
    }

    private ActionResult createRecord(TestRecordType recordTypeToCreate) {
        double duration = 0;
        Record record;
        if (testActionContext.roundRobinPrefixGenerator == null) {
            record = testActionContext.repository.getRecordFactory().newRecord();
        } else {
            record = testActionContext.repository.getRecordFactory().newRecord(createPrefixedRecordId());
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
            record = testActionContext.table.create(record);
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

    private RecordId createPrefixedRecordId() {
        final StringBuilder recordId = new StringBuilder();
        recordId.append(testActionContext.roundRobinPrefixGenerator.next());
        recordId.append(System.currentTimeMillis());
        for (byte part : MAC_ADDRESS) {
            recordId.append(String.format("%02X", part));
        }

        return testActionContext.repository.getIdGenerator().newRecordId(recordId.toString());
    }

    @Override
    public ActionResult linkFieldAction(TestFieldType testFieldType, RecordId recordId) {
        String linkedRecordSource = testFieldType.getLinkedRecordSource();
        // Pick a link from the RecordSpace source
        if (linkedRecordSource != null) {
            TestRecord record = testActionContext.records.getRecord(linkedRecordSource);
            if (record == null) {
                return new ActionResult(false, null, 0);
            }
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
            report(result.success, result.duration,
                    "linkCreate." + linkedRecordType.getRecordType().getName().getName());
            if (!result.success) {
                return new ActionResult(false, null, 0);
            }
            return new ActionResult(true, new Link(((Record) result.object).getId()), result.duration);
        }
        // Generate a link that possibly (most likely) points to a non-existing record
        return new ActionResult(true, testFieldType.generateLink(), 0);
    }
}
