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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.zookeeper.KeeperException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.node.ObjectNode;
import org.joda.time.DateTime;
import org.lilyproject.cli.OptionUtil;
import org.lilyproject.client.NoServersException;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.testclientfw.BaseRepositoryTestTool;
import org.lilyproject.testclientfw.Util;
import org.lilyproject.tools.import_.cli.DefaultImportListener;
import org.lilyproject.tools.import_.cli.ImportConflictException;
import org.lilyproject.tools.import_.cli.ImportException;
import org.lilyproject.tools.import_.cli.JsonImport;
import org.lilyproject.tools.import_.json.JsonFormatException;
import org.lilyproject.tools.import_.json.QNameConverter;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.json.JsonFormat;
import org.lilyproject.util.json.JsonUtil;

public class Tester extends BaseRepositoryTestTool {

    private Option configFileOption;
    private Option dumpSampleConfigOption;
    private RecordType recordType;
    private int maximumRunTime;
    private int maximumFailures;
    private String failuresFileName;
    private PrintStream errorStream;
    private long startTime;
    private int failureCount = 0;
    private Option iterationsOption;
    private int nrOfIterations;
    private TestActionFactory testActionFactory = new TestActionFactory();
    private List<TestAction> workersTestActions[] = null;
    private List<JsonNode> recordSpacesConfig = new ArrayList<JsonNode>();
    private List<RecordSpaces> workersRecordSpaces = null;

    private Map<QName, TestRecordType> recordTypes = new HashMap<QName, TestRecordType>();
    private Map<QName, TestFieldType> fieldTypes = new HashMap<QName, TestFieldType>();
    private JsonImport jsonImport;

    public static void main(String[] args) throws Exception {
        new Tester().start(args);
    }

    @Override
    protected String getCmdName() {
        return "lily-tester";
    }

    @Override
    protected String getVersion() {
        return org.lilyproject.util.Version.readVersion("org.lilyproject", "lily-tester");
    }

    @Override
    public List<Option> getOptions() {
        List<Option> options = super.getOptions();

        configFileOption = OptionBuilder
                .withArgName("config.json")
                .hasArg()
                .withDescription("Test tool configuration file")
                .withLongOpt("config")
                .create("c");
        options.add(configFileOption);

        dumpSampleConfigOption = OptionBuilder
                .withDescription("Dumps a sample configuration to standard out")
                .withLongOpt("dump-sample-config")
                .create("d");
        options.add(dumpSampleConfigOption);

        iterationsOption = OptionBuilder
                .withArgName("iterations")
                .hasArg()
                .withDescription("Number of times to run the scenario")
                .withLongOpt("iterations")
                .create("i");
        options.add(iterationsOption);

        return options;
    }

    @Override
    public int run(CommandLine cmd) throws Exception {
        int result = super.run(cmd);
        if (result != 0) {
            return result;
        }

        if (cmd.hasOption(dumpSampleConfigOption.getOpt())) {
            return dumpSampleConfig();
        }

        if (!cmd.hasOption(configFileOption.getOpt())) {
            printHelp();
            return 1;
        }

        setupLily();

        setupMetrics();

        String configFileName = cmd.getOptionValue(configFileOption.getOpt());

        workersRecordSpaces = new ArrayList<RecordSpaces>(workers);
        workersTestActions = new ArrayList[workers];
        for (int i = 0; i < workers; i++) {
            workersTestActions[i] = new ArrayList<TestAction>();
        }

        InputStream is = new FileInputStream(configFileName);
        loadConfig(is);
        is.close();

        try {
            System.out.println("Running tests...");
            System.out.println("Tail the output files if you wonder what is happening.");
            nrOfIterations = OptionUtil.getIntOption(cmd, iterationsOption, 1000);
            test();
        } finally {
            closeStreams();
        }

        finishMetrics();

        System.out.println("Test done.");
        return 0;
    }

    private void loadConfig(InputStream is)
            throws IOException, JsonFormatException, RepositoryException, ImportConflictException,
            ImportException, InterruptedException, SecurityException, IllegalArgumentException, NoSuchMethodException,
            InstantiationException, IllegalAccessException, InvocationTargetException {

        jsonImport = new JsonImport(repository, new DefaultImportListener());
        JsonParser jp = JsonFormat.JSON_FACTORY_NON_STD.createJsonParser(is);

        JsonToken current;
        current = jp.nextToken();

        if (current != JsonToken.START_OBJECT) {
            System.out.println("Error: expected object node as root of the input. Giving up.");
            return;
        }

        while (jp.nextToken() != JsonToken.END_OBJECT) {
            String fieldName = jp.getCurrentName();
            current = jp.nextToken(); // move from field name to field value
            if (fieldName.equals("namespaces")) {
                if (current == JsonToken.START_OBJECT) {
                    jsonImport.readNamespaces((ObjectNode) jp.readValueAsTree());
                } else {
                    System.out.println("Error: namespaces property should be an object. Skipping.");
                    jp.skipChildren();
                }
            } else if (fieldName.equals("failuresFile")) {
                if (current == JsonToken.VALUE_STRING) {
                    openStreams(jp.getText());
                }
            } else if (fieldName.equals("fieldTypes")) {
                if (current == JsonToken.START_ARRAY) {
                    while (jp.nextToken() != JsonToken.END_ARRAY) {
                        importFieldType(jp.readValueAsTree());
                    }
                } else {
                    System.out.println("Error: fieldTypes property should be an array. Skipping.");
                    jp.skipChildren();
                }
            } else if (fieldName.equals("recordTypes")) {
                if (current == JsonToken.START_ARRAY) {
                    while (jp.nextToken() != JsonToken.END_ARRAY) {
                        importRecordType(jp.readValueAsTree());
                    }
                } else {
                    System.out.println("Error: recordTypes property should be an array. Skipping.");
                    jp.skipChildren();
                }
            } else if (fieldName.equals("recordSpaces")) {
                if (current == JsonToken.START_ARRAY) {
                    while (jp.nextToken() != JsonToken.END_ARRAY) {
                        recordSpacesConfig.add(jp.readValueAsTree());
                    }
                    for (int i = 0; i < workers; i++) {
                        workersRecordSpaces.add(new RecordSpaces(recordSpacesConfig));
                    }
                } else {
                    System.out.println("Error: recordSpaces property should be an array. Skipping.");
                    jp.skipChildren();
                }
            } else if (fieldName.equals("scenario")) {
                if (current == JsonToken.START_ARRAY) {
                    while (jp.nextToken() != JsonToken.END_ARRAY) {
                        JsonNode actionNode = jp.readValueAsTree();
                        prepareAction(actionNode);
                    }
                } else {
                    System.out.println("Error: recordSpaces property should be an array. Skipping.");
                    jp.skipChildren();
                }
            } else if (fieldName.equals("stopConditions")) {
                if (current == JsonToken.START_OBJECT) {
                    readStopConditions((ObjectNode) jp.readValueAsTree());
                } else {
                    System.out.println("Error: stopConditions property should be an object. Skipping.");
                    jp.skipChildren();
                }
            }
        }
    }

    private void importFieldType(JsonNode fieldTypeNode)
            throws RepositoryException, ImportConflictException, ImportException, JsonFormatException,
            InterruptedException {
        int times = 0;
        JsonNode timesNode = fieldTypeNode.get("times");
        if (timesNode != null) {
            times = timesNode.getIntValue();
        }
        JsonNode propertiesNode = fieldTypeNode.get("properties");
        if (times == 0) {
            FieldType importFieldType = jsonImport.importFieldType(fieldTypeNode);
            fieldTypes.put(importFieldType.getName(), new TestFieldType(importFieldType, repository, propertiesNode));
        } else {
            List<FieldType> importFieldTypes = jsonImport.importFieldTypes(fieldTypeNode, times);
            for (FieldType importFieldType : importFieldTypes) {
                fieldTypes.put(importFieldType.getName(),
                        new TestFieldType(importFieldType, repository, propertiesNode));
            }
        }
    }

    private void importRecordType(JsonNode recordTypeNode)
            throws JsonFormatException, RepositoryException, ImportException, InterruptedException {
        String recordTypeName = JsonUtil.getString(recordTypeNode, "name");
        QName recordTypeQName = QNameConverter.fromJson(recordTypeName, jsonImport.getNamespaces());
        recordType = repository.getTypeManager().newRecordType(recordTypeQName);
        TestRecordType testRecordType = new TestRecordType();
        // Fields
        for (JsonNode fieldNode : recordTypeNode.get("fields")) {
            String fieldName = JsonUtil.getString(fieldNode, "name");
            int times = 0;
            JsonNode timesNode = fieldNode.get("times");
            if (timesNode != null) {
                times = timesNode.getIntValue();
            }
            if (times == 0) {
                TestFieldType fieldType = fieldTypes
                        .get(QNameConverter.fromJson(fieldName, jsonImport.getNamespaces()));
                recordType.addFieldTypeEntry(fieldType.getFieldType().getId(), false);
                testRecordType.addFieldType(fieldType);
            } else {
                for (int i = 0; i < times; i++) {
                    TestFieldType fieldType = fieldTypes.get(QNameConverter.fromJson(fieldName + i,
                            jsonImport.getNamespaces()));
                    recordType.addFieldTypeEntry(fieldType.getFieldType().getId(), false);
                    testRecordType.addFieldType(fieldType);
                }
            }
        }
        testRecordType.setRecordType(jsonImport.importRecordType(recordType));
        recordTypes.put(recordType.getName(), testRecordType);
    }

    private int dumpSampleConfig() throws IOException {
        InputStream is = getClass().getClassLoader().getResourceAsStream("org/lilyproject/tools/tester/config.json");
        try {
            IOUtils.copy(is, System.out);
        } finally {
            Closer.close(is);
        }
        return 0;
    }

    private void prepareAction(JsonNode actionNode)
            throws IOException, SecurityException, IllegalArgumentException, NoSuchMethodException,
            InstantiationException, IllegalAccessException, InvocationTargetException {
        final RoundRobinPrefixGenerator roundRobinPrefixGenerator = actionNode.get("recordIdPrefixNbrOfChars") != null ?
                new RoundRobinPrefixGenerator(actionNode.get("recordIdPrefixNbrOfChars").getIntValue()) : null;

        for (int i = 0; i < workers; i++) {
            new RecordSpaces(recordSpacesConfig);
            TestActionContext testActionContext = new TestActionContext(recordTypes, fieldTypes,
                    jsonImport.getNamespaces(), workersRecordSpaces.get(i), repository, metrics, errorStream,
                    roundRobinPrefixGenerator);
            TestAction testAction = testActionFactory.getTestAction(actionNode, testActionContext);
            workersTestActions[i].add(testAction);
        }
    }

    private void readStopConditions(JsonNode stopConditions) {
        maximumRunTime = JsonUtil.getInt(stopConditions, "maximumRunTime");
        maximumFailures = JsonUtil.getInt(stopConditions, "maximumFailures");
    }

    private void createSchema(JsonNode configNode) throws IOException, RepositoryException, ImportConflictException,
            ImportException, JsonFormatException, NoServersException, InterruptedException, KeeperException {

        JsonImport jsonImport = new JsonImport(repository, new DefaultImportListener());

        // Namespaces
        ObjectNode namespacesNode = JsonUtil.getObject(configNode, "namespaces", null);
        if (namespacesNode != null) {
            jsonImport.readNamespaces(namespacesNode);
        }

        // Fields
        JsonNode fieldTypesNode = configNode.get("fieldTypes");
        if (fieldTypesNode != null && fieldTypesNode.isArray()) {
            for (JsonNode fieldTypeNode : fieldTypesNode) {
                FieldType importFieldType = jsonImport.importFieldType(fieldTypeNode);
                JsonNode propertiesNode = fieldTypeNode.get("properties");

                fieldTypes.put(importFieldType.getName(),
                        new TestFieldType(importFieldType, repository, propertiesNode));
            }
        }

        // Record type
        JsonNode recordTypesNode = configNode.get("recordTypes");
        if (recordTypesNode != null && recordTypesNode.isArray()) {
            for (JsonNode recordTypeNode : recordTypesNode) {
                String recordTypeName = JsonUtil.getString(recordTypeNode, "name");
                QName recordTypeQName = QNameConverter.fromJson(recordTypeName, jsonImport.getNamespaces());
                recordType = repository.getTypeManager().newRecordType(recordTypeQName);
                TestRecordType testRecordType = new TestRecordType();
                // Fields
                for (JsonNode fieldNode : recordTypeNode.get("fields")) {
                    String fieldName = JsonUtil.getString(fieldNode, "name");
                    TestFieldType fieldType = fieldTypes.get(fieldName);
                    recordType.addFieldTypeEntry(fieldType.getFieldType().getId(), false);
                    testRecordType.addFieldType(fieldType);
                }
                testRecordType.setRecordType(jsonImport.importRecordType(recordType));
                recordTypes.put(recordType.getName(), testRecordType);
            }
        }
    }

    private void openStreams(String failuresFileName) throws IOException {
        errorStream = new PrintStream(Util.getOutputFileRollOldOne(failuresFileName));
        errorStream.println(new DateTime() + " Opening file");
    }

    private void closeStreams() {
        errorStream.println(new DateTime() + " Closing file");
        Closer.close(errorStream);
    }

    private void test() throws InterruptedException {
        startTime = System.currentTimeMillis();
        HashSet<Thread> threads = new HashSet<Thread>(workers);
        for (int i = 0; i < workers; i++) {
            threads.add(new WorkerThread(workersTestActions[i]));
        }

        for (Thread thread : threads) {
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
    }

    private class WorkerThread extends Thread {
        private final List<TestAction> testActions;

        WorkerThread(List<TestAction> testActions) {
            this.testActions = testActions;
        }

        @Override
        public void run() {
            for (int j = 0; j < nrOfIterations; j++) {
                for (TestAction testAction : testActions) {
                    incFailureCount(testAction.run());
                }
                if (checkStopConditions()) {
                    return;
                }
            }
        }

    }

    private synchronized void incFailureCount(int amount) {
        failureCount = failureCount + amount;
    }

    private synchronized int getFailureCount() {
        return failureCount;
    }

    private boolean checkStopConditions() {
        if (getFailureCount() >= maximumFailures) {
            System.out.println("Stopping because maximum number of failures is reached: " + maximumFailures);
            return true;
        }

        int ran = (int) Math.floor((System.currentTimeMillis() - startTime) / 1000 / 60);
        if (ran >= maximumRunTime) {
            System.out.println("Stopping because maximum running time is reached: " + maximumRunTime + " minutes.");
            return true;
        }

        return false;
    }
}
