package org.lilyproject.tools.tester;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.JsonNode;
import org.lilyproject.util.json.JsonUtil;

public class RecordSpaces {

    Map<String, RecordSpace> recordSpaces = new HashMap<String, RecordSpace>(); 
    
    public RecordSpaces(JsonNode recordSpacesNode) {
        for (JsonNode recordIdSpaceNode : recordSpacesNode) {
            String name = JsonUtil.getString(recordIdSpaceNode, "name");
            Integer limit = JsonUtil.getInt(recordIdSpaceNode, "limit");
            recordSpaces.put(name, new RecordSpace(limit));
        }
    }
    
    public void addRecord(String space, TestRecord record) {
        recordSpaces.get(space).addRecord(record);
    }
    
    public void removeRecord(String space, TestRecord record) {
        recordSpaces.get(space).removeRecord(record);
    }
    
    public TestRecord getRecord(String space) {
        return recordSpaces.get(space).getRecord();
    }
    
    private class RecordSpace {
        private int limit;
        private Set<TestRecord> records;
        
        public RecordSpace(int limit) {
            this.limit = limit;
            records = new HashSet<TestRecord>();
        }
        
        public synchronized void addRecord(TestRecord record) {
            
            if (records.size() < limit) {
                records.add(record);
            } else {
                TestRecord[] array = records.toArray(new TestRecord[records.size()]);
                int index = (int) Math.floor(Math.random() * records.size());
                if (records.remove(array[index]))
                    records.add(record);
            }
        }
        
        public synchronized void removeRecord(TestRecord record) {
            records.remove(record);
        }
        
        public synchronized TestRecord getRecord() {
            TestRecord testRecord;
            int loopCnt = 0;
            do {
                TestRecord[] array = records.toArray(new TestRecord[records.size()]);
                int index = (int) Math.floor(Math.random() * records.size());
                testRecord = array[index];
                loopCnt++;
                if ((loopCnt % 100) == 0) {
                    System.out.println("Already tried " + loopCnt + " times to pick a non-deleted record.");
                }
            } while (testRecord.isDeleted());

            return testRecord;
        }
    }
}
