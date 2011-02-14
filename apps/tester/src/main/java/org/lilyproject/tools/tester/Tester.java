package org.lilyproject.tools.tester;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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
import org.codehaus.jackson.node.ObjectNode;
import org.joda.time.DateTime;
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
    private RecordSpaces records ;
    private int failureCount = 0;
    private Option iterationsOption;
    private int nrOfIterations;
    private TestActionFactory testActionFactory = new TestActionFactory();
    private List<List<TestAction>> workersTestActions = new ArrayList<List<TestAction>>();

    private Map<String, TestRecordType> recordTypes = new HashMap<String, TestRecordType>();
    private Map<String, TestFieldType> fieldTypes = new HashMap<String, TestFieldType>();

    public static void main(String[] args) throws Exception {
        new Tester().start(args);
    }
    
    @Override
    protected String getCmdName() {
        return "lily-tester";
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
        if (result != 0)
            return result;
        
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
        InputStream is = new FileInputStream(configFileName);
        JsonNode configNode = JsonFormat.deserializeNonStd(is);
        is.close();

        openStreams(configNode); // Only failures stream

        createSchema(configNode);

        setupRecordSpaces(configNode);

        prepareActions(configNode);
        
        readStopConditions(configNode);
        
        try {            
            System.out.println("Running tests...");
            System.out.println("Tail the output files if you wonder what is happening.");
            nrOfIterations = Util.getIntOption(cmd, iterationsOption, 1000);
            test();
        } finally {
            closeStreams();
        }
        
        finishMetrics();
        
        System.out.println("Test done.");
        return 1;
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
    
    private void prepareActions(JsonNode configNode) throws IOException, SecurityException, IllegalArgumentException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        for (int i = 0; i < workers; i++) {
            List<TestAction> testActions = new ArrayList<TestAction>();
            JsonNode scenarioNode = JsonUtil.getNode(configNode, "scenario");
            TestActionContext testActionContext = new TestActionContext(recordTypes, fieldTypes, records, repository, metrics, errorStream);
            for (JsonNode actionNode : scenarioNode) {
                testActions.add(testActionFactory.getTestAction(actionNode, testActionContext));
            }
            workersTestActions.add(testActions);
        }
    }
    
    private void readStopConditions(JsonNode configNode) {
        JsonNode stopConditions = JsonUtil.getNode(configNode, "stopConditions");
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
                
                fieldTypes.put(JsonUtil.getString(fieldTypeNode, "name"), new TestFieldType(importFieldType, repository, propertiesNode));
            }
        }
        
        // Record type
        JsonNode recordTypesNode = configNode.get("recordTypes");
        if (recordTypesNode != null && recordTypesNode.isArray()) {
            for (JsonNode recordTypeNode : recordTypesNode) {
                String recordTypeName = JsonUtil.getString(recordTypeNode, "name");
                QName recordTypeQName = QNameConverter.fromJson(recordTypeName, jsonImport.getNamespaces());
                recordType = repository.getTypeManager().newRecordType(recordTypeQName);
                TestRecordType testRecordType = new TestRecordType(recordType);
                // Fields
                for (JsonNode fieldNode : recordTypeNode.get("fields")) {
                    String fieldName = JsonUtil.getString(fieldNode, "name");
                    TestFieldType fieldType = fieldTypes.get(fieldName);
                    recordType.addFieldTypeEntry(fieldType.getFieldType().getId(), false);
                    testRecordType.addFieldType(fieldType);
                }
                recordType = jsonImport.importRecordType(recordType);
                recordTypes.put(recordTypeName, testRecordType);
            }
        }
    }
    
    private void setupRecordSpaces(JsonNode configNode) {
        JsonNode recordSpacesNode = configNode.get("recordSpaces");
        records = new RecordSpaces(recordSpacesNode);
    }
    
    private void openStreams(JsonNode configNode) throws FileNotFoundException {
        failuresFileName = JsonUtil.getString(configNode, "failuresFile");
        errorStream = new PrintStream(new File(failuresFileName));
        errorStream.print(new DateTime() + " Opening file");
    }

    private void closeStreams() {
        errorStream.print(new DateTime() + " Closing file");
        Closer.close(errorStream);
    }

    private void test() throws InterruptedException {
        startTime = System.currentTimeMillis();
        HashSet<Thread> threads = new HashSet<Thread>(workers);
        for (int i = 0; i < workers; i++) {
            threads.add(new WorkerThread(workersTestActions.get(i)));
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

        public WorkerThread(List<TestAction> testActions) {
            this.testActions = testActions;
        }
        
        public void run() {
            for (int j = 0; j < nrOfIterations; j++) {
                for (TestAction testAction : testActions) {
                    incFailureCount(testAction.run());
                }
                if (checkStopConditions()) return;
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
        
        int ran = (int)Math.floor((System.currentTimeMillis() - startTime) / 1000 / 60);
        if (ran >= maximumRunTime) {
            System.out.println("Stopping because maximum running time is reached: " + maximumRunTime + " minutes.");
            return true;
        }

        return false;
    }
}
