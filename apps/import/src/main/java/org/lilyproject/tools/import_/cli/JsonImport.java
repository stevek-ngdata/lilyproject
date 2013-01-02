/*
 * Copyright 2010 Outerthought bvba
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
package org.lilyproject.tools.import_.cli;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.tools.import_.core.FieldTypeImport;
import org.lilyproject.tools.import_.core.IdentificationMode;
import org.lilyproject.tools.import_.core.ImportMode;
import org.lilyproject.tools.import_.core.ImportResult;
import org.lilyproject.tools.import_.core.RecordImport;
import org.lilyproject.tools.import_.core.RecordTypeImport;
import org.lilyproject.tools.import_.json.FieldTypeReader;
import org.lilyproject.tools.import_.json.JsonFormatException;
import org.lilyproject.tools.import_.json.Namespaces;
import org.lilyproject.tools.import_.json.NamespacesConverter;
import org.lilyproject.tools.import_.json.NamespacesImpl;
import org.lilyproject.tools.import_.json.RecordReader;
import org.lilyproject.tools.import_.json.RecordTypeReader;
import org.lilyproject.tools.import_.json.UnmodifiableNamespaces;
import org.lilyproject.util.concurrent.WaitPolicy;
import org.lilyproject.util.json.JsonFormat;

public class JsonImport {
    private Namespaces namespaces = new NamespacesImpl();
    private Repository repository;
    private TypeManager typeManager;
    private ImportListener importListener;
    private int threadCount;
    private ThreadPoolExecutor executor;
    private volatile boolean errorHappened = false;

    public static void load(Repository repository, InputStream is, boolean schemaOnly, int threadCount) throws Exception {
        load(repository, new DefaultImportListener(), is, schemaOnly, threadCount);
    }

    public static void load(Repository repository, InputStream is, boolean schemaOnly) throws Exception {
        load(repository, new DefaultImportListener(), is, schemaOnly);
    }

    public static void load(Repository repository, ImportListener importListener, InputStream is, boolean schemaOnly)
            throws Exception {
        load(repository, importListener, is, schemaOnly, 1);
    }

    public static void load(Repository repository, ImportListener importListener, InputStream is, boolean schemaOnly,
                            int threadCount) throws Exception {
        new JsonImport(repository, importListener, threadCount).load(is, schemaOnly);
    }

    public JsonImport(Repository repository, ImportListener importListener) {
        this(repository, importListener, 1);
    }

    public JsonImport(Repository repository, ImportListener importListener, int threadCount) {
        this.importListener = new SynchronizedImportListener(importListener);
        this.repository = repository;
        this.typeManager = repository.getTypeManager();
        this.threadCount = threadCount;
    }

    public void load(InputStream is, boolean schemaOnly) throws Exception {
        // A combination of the Jackson streaming and tree APIs is used: we move streaming through the
        // whole of the file, but use the tree API to load individual items (field types, records, ...).
        // This way things should still work fast and within little memory if anyone would use this to
        // load large amounts of records.

        try {
            namespaces = new NamespacesImpl();

            JsonParser jp = JsonFormat.JSON_FACTORY_NON_STD.createJsonParser(is);

            JsonToken current;
            current = jp.nextToken();

            if (current != JsonToken.START_OBJECT) {
                System.out.println("Error: expected object node as root of the input. Giving up.");
                return;
            }

            while (jp.nextToken() != JsonToken.END_OBJECT && !errorHappened) {
                String fieldName = jp.getCurrentName();
                current = jp.nextToken(); // move from field name to field value
                if (fieldName.equals("namespaces")) {
                    if (current == JsonToken.START_OBJECT) {
                        readNamespaces((ObjectNode)jp.readValueAsTree());
                    } else {
                        System.out.println("Error: namespaces property should be an object. Skipping.");
                        jp.skipChildren();
                    }
                } else if (fieldName.equals("fieldTypes")) {
                    if (current == JsonToken.START_ARRAY) {
                        startExecutor();
                        while (jp.nextToken() != JsonToken.END_ARRAY && !errorHappened) {
                            pushTask(new FieldTypeImportTask(jp.readValueAsTree()));
                        }
                        waitTasksFinished();
                    } else {
                        System.out.println("Error: fieldTypes property should be an array. Skipping.");
                        jp.skipChildren();
                    }
                } else if (fieldName.equals("recordTypes")) {
                    if (current == JsonToken.START_ARRAY) {
                        startExecutor();
                        while (jp.nextToken() != JsonToken.END_ARRAY && !errorHappened) {
                            pushTask(new RecordTypeImportTask(jp.readValueAsTree()));
                        }
                        waitTasksFinished();
                    } else {
                        System.out.println("Error: recordTypes property should be an array. Skipping.");
                        jp.skipChildren();
                    }
                } else if (fieldName.equals("records")) {
                    if (!schemaOnly) {
                        if (current == JsonToken.START_ARRAY) {
                            startExecutor();
                            while (jp.nextToken() != JsonToken.END_ARRAY && !errorHappened) {
                                pushTask(new RecordImportTask(jp.readValueAsTree()));
                            }
                            waitTasksFinished();
                        } else {
                            System.out.println("Error: records property should be an array. Skipping.");
                            jp.skipChildren();
                        }
                    } else {
                        jp.skipChildren();
                    }
                }
            }
        } finally {
            waitTasksFinished();
        }

        if (errorHappened) {
            throw new ImportException("Errors happened during import.");
        }
    }

    public void readNamespaces(ObjectNode node) throws JsonFormatException {
        // We don't expect the namespaces to be modified since we're reading rather than writing, still wrap it
        // to make sure they are really not modified.
        this.namespaces = new UnmodifiableNamespaces(NamespacesConverter.fromJson(node));
    }

    public Namespaces getNamespaces() {
        return namespaces;
    }

    public FieldType importFieldType(JsonNode node) throws RepositoryException, ImportConflictException,
            ImportException, JsonFormatException, InterruptedException {

        if (!node.isObject()) {
            throw new ImportException("Field type should be specified as object node.");
        }

        FieldType fieldType = FieldTypeReader.INSTANCE.fromJson((ObjectNode)node, namespaces, repository);

        if (fieldType.getName() == null) {
            throw new ImportException("Missing name property on field type.");
        }

        ImportResult<FieldType> result = FieldTypeImport.importFieldType(fieldType, ImportMode.CREATE_OR_UPDATE,
                IdentificationMode.NAME, fieldType.getName(), typeManager);
        FieldType newFieldType = result.getEntity();

        switch (result.getResultType()) {
            case CREATED:
                importListener.created(EntityType.FIELD_TYPE, newFieldType.getName().toString(), newFieldType.getId().toString());
                break;
            case UP_TO_DATE:
                importListener.existsAndEqual(EntityType.FIELD_TYPE, newFieldType.getName().toString(), null);
                break;
            case CONFLICT:
                importListener.conflict(EntityType.FIELD_TYPE, fieldType.getName().toString(),
                        result.getConflictingProperty(), result.getConflictingOldValue(),
                        result.getConflictingNewValue());
                break;
            default:
                throw new ImportException("Unexpected import result type for field type: " + result.getResultType());
        }

        return newFieldType;
    }

    public List<FieldType> importFieldTypes(JsonNode node, int times) throws RepositoryException,
            ImportConflictException, ImportException, JsonFormatException, InterruptedException {
        List<FieldType> newFieldTypes = new ArrayList<FieldType>(times);

        if (!node.isObject()) {
            throw new ImportException("Field type should be specified as object node.");
        }

        FieldType fieldType = FieldTypeReader.INSTANCE.fromJson((ObjectNode)node, namespaces, repository);

        if (fieldType.getName() == null) {
            throw new ImportException("Missing name property on field type.");
        }

        for (int i = 0; i < times; i++) {
            FieldType ftToCreate = fieldType.clone();
            ftToCreate.setName(new QName(fieldType.getName().getNamespace(), fieldType.getName().getName() + i));
            ImportResult<FieldType> result = FieldTypeImport.importFieldType(ftToCreate, ImportMode.CREATE_OR_UPDATE,
                    IdentificationMode.NAME, ftToCreate.getName(), typeManager);
            FieldType newFieldType = result.getEntity();

            switch (result.getResultType()) {
                case CREATED:
                    importListener.created(EntityType.FIELD_TYPE, newFieldType.getName().toString(), newFieldType.getId()
                            .toString());
                    break;
                case UP_TO_DATE:
                    importListener.existsAndEqual(EntityType.FIELD_TYPE, newFieldType.getName().toString(), null);
                    break;
                case CONFLICT:
                    importListener.conflict(EntityType.FIELD_TYPE, ftToCreate.getName().toString(),
                            result.getConflictingProperty(), result.getConflictingOldValue(),
                            result.getConflictingNewValue());
                    break;
                default:
                    throw new ImportException("Unexpected import result type for field type: " + result.getResultType());
            }
            newFieldTypes.add(newFieldType);
        }

        return newFieldTypes;
    }

    public RecordType importRecordType(JsonNode node) throws RepositoryException, ImportException, JsonFormatException,
            InterruptedException {

        if (!node.isObject()) {
            throw new ImportException("Record type should be specified as object node.");
        }

        RecordType recordType = RecordTypeReader.INSTANCE.fromJson((ObjectNode)node, namespaces, repository);
        return importRecordType(recordType);
    }

    public RecordType importRecordType(RecordType recordType) throws RepositoryException, ImportException,
            JsonFormatException, InterruptedException {

        if (recordType.getName() == null) {
            throw new ImportException("Missing name property on record type.");
        }

        ImportResult<RecordType> result = RecordTypeImport.importRecordType(recordType, ImportMode.CREATE_OR_UPDATE,
                IdentificationMode.NAME, recordType.getName(), typeManager);
        RecordType newRecordType = result.getEntity();

        switch (result.getResultType()) {
            case CREATED:
                importListener.created(EntityType.RECORD_TYPE, newRecordType.getName().toString(), newRecordType.getId().toString());
                break;
            case UPDATED:
                importListener.updated(EntityType.RECORD_TYPE, null, newRecordType.getId().toString(), newRecordType.getVersion());
                break;
            case UP_TO_DATE:
                importListener.existsAndEqual(EntityType.RECORD_TYPE, recordType.getName().toString(), null);
                break;
            default:
                throw new ImportException("Unexpected import result type for record type: " + result.getResultType());
        }

        return newRecordType;
    }

    private Record importRecord(JsonNode node) throws RepositoryException, ImportException, JsonFormatException,
            InterruptedException {

        if (!node.isObject()) {
            throw new ImportException("Record should be specified as object node.");
        }

        Record record = RecordReader.INSTANCE.fromJson((ObjectNode)node, namespaces, repository);

        // Create-or-update requires client to specify the ID
        if (record.getId() == null) {
            record.setId(repository.getIdGenerator().newRecordId());
        }

        ImportResult<Record> result = RecordImport.importRecord(record, ImportMode.CREATE_OR_UPDATE, repository);
        record = result.getEntity();

        switch (result.getResultType()) {
            case CREATED:
                importListener.created(EntityType.RECORD, null, record.getId().toString());
                break;
            case UP_TO_DATE:
                importListener.existsAndEqual(EntityType.RECORD, null, record.getId().toString());
                break;
            case UPDATED:
                importListener.updated(EntityType.RECORD, null, record.getId().toString(), record.getVersion());
                break;
            default:
                throw new ImportException("Unexpected import result type for record: " + result.getResultType());
        }

        return record;
    }

    private void handleImportError(Throwable throwable) {
        // In case of an error, we want to stop the import asap. Since it's multi-threaded, it can
        // be that there are still a few operations done before it's done.
        // We don't do an immediate shutdown of the ExecutorService since we don't want to interrupt running threads,
        // they are allowed to finish what they are doing.
        errorHappened = true;
        executor.getQueue().clear();
        importListener.exception(throwable);
    }

    private void startExecutor() {
        executor = new ThreadPoolExecutor(threadCount, threadCount, 10, TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(5));
        executor.setRejectedExecutionHandler(new WaitPolicy());
    }

    private void waitTasksFinished() throws InterruptedException, ExecutionException {
        if (executor == null) {
            return;
        }

        executor.shutdown();
        boolean successfulFinish = executor.awaitTermination(10, TimeUnit.MINUTES);
        if (!successfulFinish) {
            throw new RuntimeException("JSON import executor did not end successfully.");
        }
        executor = null;
    }

    private void pushTask(Runnable runnable) {
        executor.submit(runnable);
    }

    private class FieldTypeImportTask implements Runnable {
        private JsonNode json;

        public FieldTypeImportTask(JsonNode json) {
            this.json = json;
        }

        @Override
        public void run() {
            try {
                importFieldType(json);
            } catch (Throwable t) {
                handleImportError(t);
            }
        }
    }

    private class RecordTypeImportTask implements Runnable {
        private JsonNode json;

        public RecordTypeImportTask(JsonNode json) {
            this.json = json;
        }

        @Override
        public void run() {
            try {
                importRecordType(json);
            } catch (Throwable t) {
                handleImportError(t);
            }
        }
    }

    private class RecordImportTask implements Runnable {
        private JsonNode json;

        public RecordImportTask(JsonNode json) {
            this.json = json;
        }

        @Override
        public void run() {
            try {
                importRecord(json);
            } catch (Throwable t) {
                handleImportError(t);
            }
        }
    }
}
