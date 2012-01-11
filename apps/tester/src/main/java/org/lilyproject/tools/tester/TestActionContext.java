package org.lilyproject.tools.tester;

import java.io.PrintStream;
import java.util.Map;

import org.lilyproject.clientmetrics.Metrics;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.util.ArgumentValidator;

public class TestActionContext {
    public final Map<String, TestRecordType> recordTypes;
    public final Map<String, TestFieldType> fieldTypes;
    public final RecordSpaces records;
    public final Repository repository;
    public final Metrics metrics;
    public final PrintStream errorStream;

    public TestActionContext(Map<String, TestRecordType> recordTypes, Map<String, TestFieldType> fieldTypes, RecordSpaces records, Repository repository, Metrics metrics, PrintStream errorStream) {
        ArgumentValidator.notNull(recordTypes, "recordTypes");
        ArgumentValidator.notNull(fieldTypes, "fieldTypes");
        ArgumentValidator.notNull(records, "records");
        ArgumentValidator.notNull(repository, "repository");
        ArgumentValidator.notNull(metrics, "metrics");
        ArgumentValidator.notNull(errorStream, "errorStream");
        this.recordTypes = recordTypes;
        this.fieldTypes = fieldTypes;
        this.records = records;
        this.repository = repository;
        this.metrics = metrics;
        this.errorStream = errorStream;
    }
}
