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

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.LRepository;
import org.lilyproject.repository.api.LTable;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
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
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.json.JsonFormat;

import static org.lilyproject.util.json.JsonUtil.getArray;
import static org.lilyproject.util.json.JsonUtil.getBoolean;
import static org.lilyproject.util.json.JsonUtil.getString;

public class JsonImport {
    private Namespaces namespaces = new NamespacesImpl();
    private LRepository repository;
    private LTable table;
    private ImportListener importListener;
    private int threadCount;
    private RecordReader recordReader;
    private ThreadPoolExecutor executor;
    private volatile boolean errorHappened = false;

    private static final int DEFAULT_THREAD_COUNT = 1;

    public static class ImportSettings {
        public int threadCount = DEFAULT_THREAD_COUNT;
        public RecordReader recordReader = RecordReader.INSTANCE;
        public ImportListener importListener = new DefaultImportListener();

        public ImportSettings() {
        }

        public ImportSettings(int threadCount, ImportListener importListener, RecordReader recordReader) {
            this.threadCount = threadCount;
            this.importListener = importListener;
            this.recordReader = recordReader;
        }
    }

    /**
     * The standard loading method: loads both schema and records from the default JSON format.
     */
    public static void load(LTable table, LRepository repository, InputStream is, ImportSettings settings)
            throws Exception {
        new JsonImport(table, repository, settings).load(is, false);
    }

    /**
     * Same as {@link #load(LTable, LRepository, InputStream, ImportSettings)}
     * but using default settings.
     */
    public static void load(LTable table, LRepository repository, InputStream is)
            throws Exception {
        new JsonImport(table, repository, new ImportSettings()).load(is, false);
    }

    /**
     * Loads only the schema, ignores any records in the input.
     */
    public static void loadSchema(LRepository repository, InputStream is, ImportSettings settings)
            throws Exception {
        new JsonImport(null, repository, settings).load(is, false);
    }

    /**
     * Same as {@link #loadSchema(LRepository, InputStream, ImportSettings)}
     * but using default settings.
     */
    public static void loadSchema(LRepository repository, InputStream is)
            throws Exception {
        new JsonImport(null, repository, new ImportSettings()).load(is, false);
    }

    /**
     * Imports an alternative input format where each line in the input contains a full json
     * object describing a Lily record. This format does not support schemas.
     */
    public static void loadJsonLines(LTable table, LRepository repository, InputStream is, ImportSettings settings)
            throws Exception {
        new JsonImport(table, repository, settings).loadJsonLines(is);
    }

    /**
     * @deprecated  use one of the variants taking LRepository and/or LTable as argument
     */
    public static void load(Repository repository, InputStream is, boolean schemaOnly, int threadCount) throws Exception {
        ImportSettings settings = new ImportSettings();
        settings.threadCount = threadCount;
        new JsonImport(repository, repository, settings).load(is, schemaOnly);
    }

    /**
     * @deprecated  use one of the variants taking LRepository and/or LTable as argument
     */
    public static void load(Repository repository, InputStream is, boolean schemaOnly) throws Exception {
        new JsonImport(repository, repository, new ImportSettings()).load(is, schemaOnly);
    }

    /**
     * @deprecated  use one of the variants taking LRepository and/or LTable as argument
     */
    public static void load(Repository repository, ImportListener importListener, InputStream is, boolean schemaOnly)
            throws Exception {
        ImportSettings settings = new ImportSettings();
        settings.importListener = importListener;
        new JsonImport(repository, repository, settings).load(is, schemaOnly);
    }

    /**
     * @deprecated  use one of the variants taking LRepository and/or LTable as argument
     */
    public static void load(Repository repository, ImportListener importListener, InputStream is, boolean schemaOnly,
            int threadCount) throws Exception {
        ImportSettings settings = new ImportSettings();
        settings.importListener = importListener;
        settings.threadCount = threadCount;
        new JsonImport(repository, repository, settings).load(is, schemaOnly);
    }

    public JsonImport(LTable table, LRepository repository, ImportListener importListener) {
        this(table, repository, new ImportSettings(1, importListener, RecordReader.INSTANCE));
    }

    public JsonImport(LTable table, LRepository repository, ImportSettings settings) {
        this.importListener = new SynchronizedImportListener(settings.importListener);
        this.table = table;
        this.repository = repository;
        this.threadCount = settings.threadCount;
        this.recordReader = settings.recordReader;
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
                            pushTask(new FieldTypeImportTask(parseFieldType(jp.readValueAsTree())));
                        }
                        waitTasksFinished();
                    } else {
                        System.out.println("Error: fieldTypes property should be an array. Skipping.");
                        jp.skipChildren();
                    }
                } else if (fieldName.equals("recordTypes")) {
                    if (current == JsonToken.START_ARRAY) {
                        Map<QName, FieldType> inlineDeclaredFieldTypes = new HashMap<QName, FieldType>();
                        List<RecordTypeImportTask> rtImportTasks = new ArrayList<RecordTypeImportTask>();

                        while (jp.nextToken() != JsonToken.END_ARRAY && !errorHappened) {
                            JsonNode rtJson = jp.readValueAsTree();
                            extractFieldTypesFromRecordType(rtJson, inlineDeclaredFieldTypes);
                            rtImportTasks.add(new RecordTypeImportTask(rtJson));
                        }

                        if (inlineDeclaredFieldTypes.size() > 0) {
                            startExecutor();
                            for (FieldType fieldType : inlineDeclaredFieldTypes.values()) {
                                if (errorHappened)
                                    break;
                                pushTask(new FieldTypeImportTask(fieldType));
                            }
                            waitTasksFinished();
                        }

                        if (rtImportTasks.size() > 0) {
                            startExecutor();
                            pushTasks(rtImportTasks);
                            waitTasksFinished();
                        }
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
                } else {
                    System.out.println("Encountered unexpected field: " + fieldName);
                    System.out.println("Maybe you want to use '--format json_lines'?");
                    jp.skipChildren();
                }
            }
        } finally {
            waitTasksFinished();
        }

        if (errorHappened) {
            throw new ImportException("Errors happened during import.");
        }
    }

    private void loadJsonLines(InputStream is) throws Exception {
        BufferedReader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
        try {
            startExecutor();

            String line;
            while ((line = reader.readLine()) != null && !errorHappened) {
                // skip comment lines and whitespace lines
                if (line.startsWith("#") || StringUtils.isBlank(line)) {
                    continue;
                }
                JsonNode node = JsonFormat.deserializeNonStd(line);
                pushTask(new RecordImportTask(node));
            }

        } finally {
            waitTasksFinished();
            Closer.close(reader);
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

    public FieldType parseFieldType(JsonNode node) throws RepositoryException, ImportException, JsonFormatException,
            InterruptedException {

        if (!node.isObject()) {
            throw new ImportException("Field type should be specified as object node.");
        }

        FieldType fieldType = FieldTypeReader.INSTANCE.fromJson(node, namespaces, repository);

        if (fieldType.getName() == null) {
            throw new ImportException("Missing name property on field type.");
        }

        return fieldType;
    }

    public FieldType importFieldType(JsonNode node) throws RepositoryException, ImportConflictException,
            ImportException, JsonFormatException, InterruptedException {
        return importFieldType(parseFieldType(node));
    }

    public FieldType importFieldType(FieldType fieldType) throws RepositoryException, ImportConflictException,
            ImportException, JsonFormatException, InterruptedException {


        ImportResult<FieldType> result = FieldTypeImport.importFieldType(fieldType, ImportMode.CREATE_OR_UPDATE,
                IdentificationMode.NAME, fieldType.getName(), repository.getTypeManager());
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

        FieldType fieldType = FieldTypeReader.INSTANCE.fromJson(node, namespaces, repository);

        if (fieldType.getName() == null) {
            throw new ImportException("Missing name property on field type.");
        }

        for (int i = 0; i < times; i++) {
            FieldType ftToCreate = fieldType.clone();
            ftToCreate.setName(new QName(fieldType.getName().getNamespace(), fieldType.getName().getName() + i));
            ImportResult<FieldType> result = FieldTypeImport.importFieldType(ftToCreate, ImportMode.CREATE_OR_UPDATE,
                    IdentificationMode.NAME, ftToCreate.getName(), repository.getTypeManager());
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

        RecordType recordType = RecordTypeReader.INSTANCE.fromJson(node, namespaces, repository);
        return importRecordType(recordType);
    }

    public RecordType importRecordType(RecordType recordType) throws RepositoryException, ImportException,
            JsonFormatException, InterruptedException {

        if (recordType.getName() == null) {
            throw new ImportException("Missing name property on record type.");
        }

        ImportResult<RecordType> result = RecordTypeImport.importRecordType(recordType, ImportMode.CREATE_OR_UPDATE,
                IdentificationMode.NAME, recordType.getName(), true, repository.getTypeManager());
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

    /**
     * Extracts field types declared inline in a record type. An inline definition is recognized by the
     * presence of a valueType attribute on the field. Found field types are added to the passed map after
     * checking for conflicting definitions.
     */
    private void extractFieldTypesFromRecordType(JsonNode node, Map<QName, FieldType> fieldTypes)
            throws RepositoryException, InterruptedException, JsonFormatException, ImportException {
        if (node.has("fields")) {
            ArrayNode fields = getArray(node, "fields");
            for (int i = 0; i < fields.size(); i++) {
                JsonNode field = fields.get(i);
                if (field.has("valueType")) {
                    FieldType fieldType = parseFieldType(field);
                    if (fieldTypes.containsKey(fieldType.getName())) {
                        FieldType prevFieldType = fieldTypes.get(fieldType.getName());
                        if (!fieldType.equals(prevFieldType)) {
                            throw new ImportException("Found conflicting definitions of a field type in two record"
                                    + " types, field types: " + fieldType + " and " + prevFieldType);
                        }
                    } else {
                        fieldTypes.put(fieldType.getName(), fieldType);
                    }
                }
            }
        }
    }

    private Record importRecord(JsonNode node) throws RepositoryException, ImportException, JsonFormatException,
            InterruptedException {

        if (!node.isObject()) {
            throw new ImportException("Record should be specified as object node.");
        }

        Record record = recordReader.fromJson(node, namespaces, repository);

        ImportMode mode = ImportMode.CREATE_OR_UPDATE;
        String modeName = getString(node, "mode", null);
        if (modeName != null) {
            try {
                mode = ImportMode.valueOf(modeName.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new ImportException(String.format("Illegal value for import mode: %s", modeName));
            }
        }

        if (mode == ImportMode.UPDATE && record.getId() == null) {
            throw new ImportException(String.format("Import mode %s is specified but the record has no id.",
                    ImportMode.UPDATE));
        }

        if (mode == ImportMode.CREATE_OR_UPDATE && record.getId() == null) {
            // Create-or-update requires client to specify the ID
            record.setId(repository.getIdGenerator().newRecordId());
        }

        RecordId inputRecordId = record.getId();

        ImportResult<Record> result = RecordImport.importRecord(record, mode, table);
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
            case CANNOT_CREATE_EXISTS:
                boolean failIfExists = getBoolean(node, "failIfExists", true);
                if (!failIfExists) {
                    importListener.allowedFailure(EntityType.RECORD, null, String.valueOf(inputRecordId),
                            "cannot create, record exists");
                    break;
                } else {
                    throw new ImportException("Cannot create record, it already exists: " + inputRecordId);
                }
            case CANNOT_UPDATE_DOES_NOT_EXIST:
                boolean failIfNotExists = getBoolean(node, "failIfNotExists", true);
                if (!failIfNotExists) {
                    importListener.allowedFailure(EntityType.RECORD, null, String.valueOf(inputRecordId),
                            "cannot update, record does not exist");
                    break;
                } else {
                    throw new ImportException("Cannot update record, it does not exist: " + inputRecordId);
                }
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

    private void pushTasks(List<? extends Runnable> runnables) {
        for (Runnable runnable : runnables) {
            if (errorHappened) {
                break;
            }
            executor.submit(runnable);
        }
    }

    private class FieldTypeImportTask implements Runnable {
        private FieldType fieldType;

        public FieldTypeImportTask(FieldType fieldType) {
            this.fieldType = fieldType;
        }

        @Override
        public void run() {
            try {
                importFieldType(fieldType);
            } catch (Throwable t) {
                handleImportError(t);
            }
        }
    }

    private class RecordTypeImportTask implements Runnable {
        private JsonNode json;

        RecordTypeImportTask(JsonNode json) {
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

        RecordImportTask(JsonNode json) {
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
