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

import java.io.PrintStream;
import java.util.Map;

import org.lilyproject.clientmetrics.Metrics;
import org.lilyproject.repository.api.LRepository;
import org.lilyproject.repository.api.LTable;
import org.lilyproject.repository.api.QName;
import org.lilyproject.tools.import_.json.Namespaces;
import org.lilyproject.util.ArgumentValidator;

public class TestActionContext {
    public final Map<QName, TestRecordType> recordTypes;
    public final Map<QName, TestFieldType> fieldTypes;
    public final RecordSpaces records;
    public final LTable table;
    public final LRepository repository;
    public final Metrics metrics;
    public final PrintStream errorStream;
    public final Namespaces nameSpaces;
    public final RoundRobinPrefixGenerator roundRobinPrefixGenerator;

    public TestActionContext(Map<QName, TestRecordType> recordTypes, Map<QName, TestFieldType> fieldTypes,
            Namespaces nameSpaces, RecordSpaces records, LTable table, LRepository repository, Metrics metrics,
            PrintStream errorStream, RoundRobinPrefixGenerator roundRobinPrefixGenerator) {
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
        this.table = table;
        this.repository = repository;
        this.metrics = metrics;
        this.errorStream = errorStream;
        this.roundRobinPrefixGenerator = roundRobinPrefixGenerator;
    }
}
