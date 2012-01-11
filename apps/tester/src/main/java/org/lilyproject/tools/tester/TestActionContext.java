package org.lilyproject.tools.tester;

import java.io.PrintStream;
import java.util.Map;

import org.lilyproject.clientmetrics.Metrics;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.tools.import_.json.Namespaces;
import org.lilyproject.util.ArgumentValidator;

public class TestActionContext {
    public final Map<QName, TestRecordType> recordTypes;
    public final Map<QName, TestFieldType> fieldTypes;
    public final RecordSpaces records;
    public final Repository repository;
    public final Metrics metrics;
    public final PrintStream errorStream;
    public final Namespaces nameSpaces;

    public TestActionContext(Map<QName, TestRecordType> recordTypes, Map<QName, TestFieldType> fieldTypes,
            Namespaces nameSpaces, RecordSpaces records, Repository repository, Metrics metrics, PrintStream errorStream) {
        ArgumentValidator.notNull(recordTypes, "recordTypes");
        ArgumentValidator.notNull(fieldTypes, "fieldTypes");
        ArgumentValidator.notNull(nameSpaces, "nameSpaces");
        ArgumentValidator.notNull(records, "records");
        ArgumentValidator.notNull(repository, "repository");
        ArgumentValidator.notNull(metrics, "metrics");
        ArgumentValidator.notNull(errorStream, "errorStream");
        this.recordTypes = recordTypes;
        this.fieldTypes = fieldTypes;
        this.nameSpaces = nameSpaces;
        this.records = records;
        this.repository = repository;
        this.metrics = metrics;
        this.errorStream = errorStream;
    }
}
