package org.lilycms.tools.tester;

import de.svenjacobs.loremipsum.LoremIpsum;
import org.apache.zookeeper.KeeperException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.lilycms.client.Client;
import org.lilycms.client.ServerUnavailableException;
import org.lilycms.repository.api.*;
import org.lilycms.tools.import_.*;
import org.lilycms.repoutil.JsonUtil;
import org.lilycms.util.io.Closer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

public class Tester {
    private Repository repository;

    private int createCount;
    private int readCount;
    private int updateCount;
    private int deleteCount;

    private int maximumRunTime;
    private int maximumRecordCreated;
    private int maximumFailures;

    private String zookeeperConnectString;
    private String reportFileName;
    private String failuresFileName;

    private long startTime;
    private int failureCount;

    private List<Field> fields;
    private LoremIpsum loremIpsum = new LoremIpsum();
    RecordType recordType;
    List<TestRecord> records = new ArrayList<TestRecord>(50000);

    PrintStream reportStream;
    PrintStream errorStream;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException,
            RepositoryException, ImportConflictException, ServerUnavailableException, ImportException {
        if (args.length < 1) {
            System.out.println("Specify config location as argument.");
            System.exit(1);
        }
        new Tester().run(args[0]);
    }

    public void run(String configFileName) throws IOException, InterruptedException, KeeperException,
            ServerUnavailableException, RepositoryException, ImportConflictException, ImportException {

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        objectMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        JsonNode configNode = objectMapper.readValue(new File(configFileName), JsonNode.class);

        readConfig(configNode);

        Client client = new Client(zookeeperConnectString);
        repository = client.getRepository();

        createSchema(configNode);

        openStreams();
        try {            
            System.out.println("Running tests...");
            System.out.println("Tail the output files if you wonder what is happening.");

            test();
        } finally {
            closeStreams();
        }

        System.out.println("Total records created during test: " + records.size());
        System.out.println("Total failures: " + failureCount);
    }

    private void openStreams() throws FileNotFoundException {
        reportStream = new PrintStream(reportFileName);
        reportStream.println("Success/Failure,Create/Read/Update/Delete,Duration (ms)");
        errorStream = new PrintStream(failuresFileName);
    }

    private void closeStreams() {
        Closer.close(reportStream);
        Closer.close(errorStream);
    }

    public void readConfig(JsonNode configNode) {
        JsonNode scenario = JsonUtil.getNode(configNode, "scenario");
        createCount = JsonUtil.getInt(scenario, "creates");
        readCount = JsonUtil.getInt(scenario, "reads");
        updateCount = JsonUtil.getInt(scenario, "updates");
        deleteCount = JsonUtil.getInt(scenario, "deletes");

        if (createCount < 1) {
            throw new RuntimeException("Number of creates should be at least 1.");
        }

        if (deleteCount >= createCount) {
            throw new RuntimeException("Number of deletes should be less than number of creates.");
        }

        JsonNode stopConditions = JsonUtil.getNode(configNode, "stopConditions");
        maximumRunTime = JsonUtil.getInt(stopConditions, "maximumRunTime");
        maximumRecordCreated = JsonUtil.getInt(stopConditions, "maximumRecordsCreated");
        maximumFailures = JsonUtil.getInt(stopConditions, "maximumFailures");

        zookeeperConnectString = JsonUtil.getString(configNode, "zookeeper");
        reportFileName = JsonUtil.getString(configNode, "reportFile");
        failuresFileName = JsonUtil.getString(configNode, "failuresFile");
    }

    public void createSchema(JsonNode configNode) throws IOException, RepositoryException, ImportConflictException,
            ImportException {

        JsonImportTool importTool = new JsonImportTool(repository, new DefaultImportListener());

        // Namespaces
        JsonNode namespaces = configNode.get("namespaces");
        if (namespaces != null && namespaces.isArray()) {
            for (JsonNode node : namespaces) {
                importTool.addNamespace(node);
            }
        }

        // Fields
        fields = new ArrayList<Field>();
        JsonNode fieldTypes = configNode.get("fieldTypes");
        if (fieldTypes != null && fieldTypes.isArray()) {
            for (JsonNode node : fieldTypes) {
                FieldType importFieldType = importTool.importFieldType(node);
                fields.add(new Field(importFieldType));
            }
        }

        // Record type
        String recordTypeName = JsonUtil.getString(configNode, "recordTypeName");
        recordType = repository.getTypeManager().newRecordType(recordTypeName);
        for (Field field : fields) {
            recordType.addFieldTypeEntry(field.fieldType.getId(), false);
        }
        recordType = importTool.getImportTool().importRecordType(recordType);
    }

    private void test() {
        startTime = System.currentTimeMillis();
        
        while (true) {
            for (int i = 0; i < createCount; i++) {
                Record record = repository.newRecord();
                record.setRecordType(recordType.getId());
                for (Field field : fields) {
                    record.setField(field.fieldType.getName(), field.generateValue());
                }

                long before = System.currentTimeMillis();
                try {
                    record = repository.create(record);
                    long after = System.currentTimeMillis();
                    report("C", true, (int)(after - before));
                    records.add(new TestRecord(record));
                } catch (Throwable t) {
                    long after = System.currentTimeMillis();
                    report("C", false, (int)(after - before));
                    reportError("Error creating record.", t);
                }

                if (checkStopConditions()) return;
            }

            for (int i = 0; i < readCount; i++) {
                TestRecord testRecord = getNonDeletedRecord();

                long before = System.currentTimeMillis();
                try {
                    Record readRecord = repository.read(testRecord.record.getId());
                    long after = System.currentTimeMillis();
                    report("R", true, (int)(after - before));

                    if (!readRecord.equals(testRecord.record)) {
                        System.out.println("Read record does not match written record!");
                    }
                } catch (Throwable t) {
                    long after = System.currentTimeMillis();
                    report("R", true, (int)(after - before));
                    reportError("Error reading record.", t);
                }

                if (checkStopConditions()) return;
            }

            for (int i = 0; i < updateCount; i++) {
                TestRecord testRecord = getNonDeletedRecord();

                int selectedField = (int)Math.floor(Math.random() * fields.size());
                Field field = fields.get(selectedField);

                Record updatedRecord = testRecord.record.clone();
                updatedRecord.setField(field.fieldType.getName(), field.generateValue());

                long before = System.currentTimeMillis();
                try {
                    updatedRecord = repository.update(updatedRecord);
                    long after = System.currentTimeMillis();
                    report("U", true, (int)(after - before));

                    testRecord.record = updatedRecord;
                } catch (Throwable t) {
                    long after = System.currentTimeMillis();
                    report("U", true, (int)(after - before));
                    reportError("Error updating record.", t);
                }

                if (checkStopConditions()) return;
            }

            for (int i = 0; i < deleteCount; i++) {
                TestRecord testRecord = getNonDeletedRecord();

                long before = System.currentTimeMillis();
                try {
                    repository.delete(testRecord.record.getId());
                    long after = System.currentTimeMillis();
                    testRecord.deleted = true;
                    report("D", true, (int)(after - before));
                } catch (Throwable t) {
                    long after = System.currentTimeMillis();
                    report("D", true, (int)(after - before));
                    reportError("Error deleting record.", t);
                }

                if (checkStopConditions()) return;
            }
        }
    }

    private boolean checkStopConditions() {
        if (failureCount >= maximumFailures) {
            System.out.println("Stopping because maximum number of failures is reached: " + maximumFailures);
            return true;
        }

        if (records.size() >= maximumRecordCreated) {
            System.out.println("Stopping because maximum number of records is reached: " + maximumRecordCreated);
            return true;
        }

        int ran = (int)Math.floor((System.currentTimeMillis() - startTime) / 1000 / 60);
        if (ran >= maximumRunTime) {
            System.out.println("Stopping because maximum running time is reached: " + maximumRunTime + " minutes.");
            return true;
        }

        return false;
    }

    private TestRecord getNonDeletedRecord() {
        TestRecord testRecord;
        int loopCnt = 0;
        do {
            int selectedIndex = (int)Math.floor(Math.random() * records.size());
            testRecord = records.get(selectedIndex);
            loopCnt++;
            if ((loopCnt % 100) == 0) {
                System.out.println("Already tried " + loopCnt + " times to pick a non-deleted record.");
            }
        } while (testRecord.deleted);

        return testRecord;
    }

    private void report(String action, boolean success, int duration) {
        reportStream.println((success ? "S" : "F") + "," + action + "," + duration);
    }

    private void reportError(String message, Throwable throwable) {
        failureCount++;
        errorStream.println("[" + new DateTime() + "] " + message);
        throwable.printStackTrace(errorStream);
        errorStream.println("---------------------------------------------------------------------------");        
    }

    private class Field {
        FieldType fieldType;

        public Field(FieldType fieldType) {
            this.fieldType = fieldType;
        }

        public Object generateValue() {
            return generateMultiValue();
        }

        private Object generateMultiValue() {
            if (fieldType.getValueType().isMultiValue()) {
                int size = (int)Math.ceil(Math.random() * 5);
                List<Object> values = new ArrayList<Object>();
                for (int i = 0; i < size; i++) {
                    values.add(generateHierarchical());
                }
                return values;
            } else {
                return generateHierarchical();
            }
        }

        private Object generateHierarchical() {
            if (fieldType.getValueType().isHierarchical()) {
                int size = (int)Math.ceil(Math.random() * 5);
                Object[] elements = new Object[size];
                for (int i = 0; i < size; i++) {
                    elements[i] = generatePrimitiveValue();
                }
                return new HierarchyPath(elements);
            } else {
                return generatePrimitiveValue();
            }
        }

        private Object generatePrimitiveValue() {
            String primitive = fieldType.getValueType().getPrimitive().getName();

            if (primitive.equals("STRING")) {
                int wordCount = (int)Math.floor(Math.random() * 100);
                return loremIpsum.getWords(wordCount);
            } else if (primitive.equals("INTEGER")) {
                double value = Math.floor(Math.random() * Integer.MAX_VALUE * 2);
                return (int)(value - Integer.MAX_VALUE);
            } else if (primitive.equals("LONG")) {
                double value = (long)Math.floor(Math.random() * Long.MAX_VALUE * 2);
                return (long)(value - Long.MAX_VALUE);
            } else if (primitive.equals("BOOLEAN")) {
                int value = (int)Math.floor(Math.random() * 1);
                return value != 0;
            } else if (primitive.equals("DATE")) {
                int year = 1950 + (int)(Math.random() * 100);
                int month = (int)Math.ceil(Math.random() * 12);
                int day = (int)Math.ceil(Math.random() * 25);
                return new LocalDate(year, month, day);
            } else if (primitive.equals("DATETIME")) {
                int year = 1950 + (int)(Math.random() * 100);
                int month = (int)Math.ceil(Math.random() * 12);
                int day = (int)Math.ceil(Math.random() * 25);
                int hour = (int)Math.floor(Math.random() * 24);
                int minute = (int)Math.floor(Math.random() * 60);
                int second = (int)Math.floor(Math.random() * 60);
                return new DateTime(year, month, day, hour, minute, second, 0);
            } else if (primitive.equals("LINK")) {
                return new Link(repository.getIdGenerator().newRecordId());
            } else {
                throw new RuntimeException("Unsupported primitive value type: " + primitive);
            }
        }
    }

    private static final class TestRecord {
        Record record;
        boolean deleted;

        public TestRecord(Record record) {
            this.record = record;
        }
    }
}
