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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.set.ListOrderedSet;
import org.codehaus.jackson.JsonNode;
import org.lilyproject.util.json.JsonUtil;

public class RecordSpaces {

    Map<String, RecordSpace> recordSpaces = new HashMap<String, RecordSpace>();

    public RecordSpaces(List<JsonNode> recordSpacesConfig) {
        for (JsonNode recordSpaceNode : recordSpacesConfig) {
            addRecordSpace(recordSpaceNode);
        }
    }

    public void addRecordSpace(JsonNode recordSpaceNode) {
        String name = JsonUtil.getString(recordSpaceNode, "name");
        Integer limit = JsonUtil.getInt(recordSpaceNode, "limit");
        recordSpaces.put(name, new RecordSpace(limit));
    }

    public void addRecord(String space, TestRecord record) {
        RecordSpace recordSpace = recordSpaces.get(space);
        if (recordSpace != null) {
            recordSpace.addRecord(record);
        }
    }

    public void removeRecord(String space, TestRecord record) {
        RecordSpace recordSpace = recordSpaces.get(space);
        if (recordSpace != null) {
            recordSpace.removeRecord(record);
        }
    }

    public TestRecord getRecord(String space) {
        RecordSpace recordSpace = recordSpaces.get(space);
        if (recordSpace != null) {
            return recordSpace.getRecord();
        } else {
            return null;
        }
    }

    private class RecordSpace {
        private int limit;
        private ListOrderedSet records;

        RecordSpace(int limit) {
            this.limit = limit;
            records = new ListOrderedSet();
        }

        public synchronized void addRecord(TestRecord record) {
            if (records.size() < limit) {
                records.add(record);
            } else {
                int index = (int) Math.floor(Math.random() * records.size());
                if (records.remove(index) != null) {
                    records.add(record);
                }
            }
        }

        public synchronized void removeRecord(TestRecord record) {
            records.remove(record);
        }

        public synchronized TestRecord getRecord() {
            TestRecord testRecord;
            int loopCnt = 0;
            do {
                int index = (int) Math.floor(Math.random() * records.size());
                testRecord = (TestRecord)records.get(index);
                loopCnt++;
                if ((loopCnt % 100) == 0) {
                    System.out.println("Already tried " + loopCnt + " times to pick a non-deleted record.");
                }
            } while (testRecord.isDeleted());

            return testRecord;
        }
    }
}
