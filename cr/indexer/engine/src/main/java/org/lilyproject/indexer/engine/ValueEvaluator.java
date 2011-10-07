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
package org.lilyproject.indexer.engine;

import com.google.common.primitives.Ints;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.WriteOutContentHandler;
import org.lilyproject.indexer.model.indexerconf.*;
import org.lilyproject.indexer.model.indexerconf.DerefValue.Follow;
import org.lilyproject.indexer.model.indexerconf.DerefValue.LinkFieldFollow;
import org.lilyproject.indexer.model.indexerconf.DerefValue.RecordFieldFollow;
import org.lilyproject.indexer.model.indexerconf.DerefValue.VariantFollow;
import org.lilyproject.indexer.model.indexerconf.DerefValue.MasterFollow;
import org.lilyproject.indexer.model.indexerconf.Formatter;
import org.lilyproject.repository.api.*;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.repo.SystemFields;
import org.lilyproject.util.repo.VersionTag;

import java.io.InputStream;
import java.util.*;

/**
 * Evaluates an index field value (a {@link Value}) to a value.
 */
public class ValueEvaluator {
    private Log log = LogFactory.getLog(getClass());

    private IndexerConf conf;

    private SystemFields systemFields;

    private Parser tikaParser = new AutoDetectParser();

    public ValueEvaluator(IndexerConf conf) {
        this.conf = conf;
        this.systemFields = conf.getSystemFields();
    }

    /**
     * Evaluates a value for a given record & vtag.
     *
     * @return null if there is no value
     */
    public List<String> eval(Value valueDef, IdRecord record, Repository repository, SchemaId vtag)
            throws RepositoryException, InterruptedException {

        List<IndexValue> indexValues = evalValue(valueDef, record, repository, vtag);
        if (indexValues == null || indexValues.size() == 0)
            return null;

        if (valueDef.extractContent()) {
            return extractContent(indexValues, repository);
        }

        Formatter formatter = conf.getFormatters().getFormatter(valueDef.getFormatter());

        return formatter.format(indexValues, repository);
    }

    /**
     * Direct 'evaluation' (content extraction, formatting) of a given field from a record. Should only be
     * called if the field is present in the record.
     */
    public List<String> format(IdRecord record, FieldType fieldType, boolean extractContent, String formatterName,
            Repository repository) throws InterruptedException {
        Object value = record.getField(fieldType.getId());

        List<IndexValue> indexValues;

        if (fieldType.getValueType().getBaseName().equals("LIST")) {
            List<Object> values = (List<Object>)value;
            indexValues = new ArrayList<IndexValue>(values.size());
            for (int i = 0; i < values.size(); i++) {
                indexValues.add(new IndexValue(record, fieldType, i, values.get(i)));
            }
        } else {
            indexValues = Collections.singletonList(new IndexValue(record, fieldType, value));
        }

        if (fieldType.getValueType().getDeepestValueType().getBaseName().equals("BLOB") && extractContent) {
            return extractContent(indexValues, repository);
        }

        Formatter formatter = conf.getFormatters().getFormatter(formatterName);

        return formatter.format(indexValues, repository);
    }

    private List<String> extractContent(List<IndexValue> indexValues, Repository repository) {
        // At this point we can be sure the value will be a blob, this is validated during
        // the construction of the indexer conf.

        List<String> result = new ArrayList<String>(indexValues.size());

        Deque<Integer> indexes = new ArrayDeque<Integer>();

        for (IndexValue indexValue : indexValues) {
            indexes.clear();

            if (indexValue.listIndex != null)
                indexes.addLast(indexValue.listIndex);

            extractContent(indexValue.value, indexes, indexValue.record, indexValue.fieldType, result, repository);
        }

        return result.isEmpty() ? null : result;
    }

    private void extractContent(Object value, Deque<Integer> indexes, Record record, FieldType fieldType,
            List<String> result, Repository repository) {

        if (value instanceof List) { // this covers both LIST and PATH types
            List values = (List)value;
            for (int i = 0; i < values.size(); i++) {
                indexes.addLast(i);
                extractContent(values.get(i), indexes, record, fieldType, result, repository);
                indexes.removeLast();
            }
        } else {
            extractContent(value, record, fieldType, Ints.toArray(indexes), result, repository);
        }
    }

    private void extractContent(Object value, Record record, FieldType fieldType, int[] indexes, List<String> result,
            Repository repository) {

        Blob blob = (Blob)value;
        InputStream is = null;

        // TODO make write limit configurable
        WriteOutContentHandler woh = new WriteOutContentHandler(500 * 1000); // 500K limit (Tika default: 100K)
        BodyContentHandler ch = new BodyContentHandler(woh);

        try {
            is = repository.getInputStream(record, fieldType.getName(), indexes);

            Metadata metadata = new Metadata();
            metadata.add(Metadata.CONTENT_TYPE, blob.getMediaType());
            if (blob.getName() != null)
                metadata.add(Metadata.RESOURCE_NAME_KEY, blob.getName());

            ParseContext parseContext = new ParseContext();

            tikaParser.parse(is, ch, metadata, parseContext);
        } catch (Throwable t) {
            if (woh.isWriteLimitReached(t)) {
                // ok, we'll just add use the partial result
                if (log.isInfoEnabled()) {
                    log.info("Blob extraction: write limit reached. Field '" + fieldType.getName() + "', record '"
                            + record.getId() + "'.");
                }
            } else {
                log.error("Error extracting blob content. Field '" + fieldType.getName() + "', record '"
                        + record.getId() + "'.", t);
                return;
            }
        } finally {
            Closer.close(is);
        }

        String text = ch.toString();
        if (text.length() > 0)
            result.add(text);
    }

    private List<IndexValue> evalValue(Value value, IdRecord record, Repository repository, SchemaId vtag)
            throws RepositoryException, InterruptedException {
        if (value instanceof FieldValue) {
            return evalFieldValue((FieldValue)value, record, repository);
        } else if (value instanceof DerefValue) {
            return evalDerefValue((DerefValue)value, record, repository, vtag);
        } else {
            throw new RuntimeException("Unexpected type of value: " + value.getClass().getName());
        }
    }

    private List<IndexValue> evalFieldValue(FieldValue value, IdRecord record, Repository repository)
            throws RepositoryException, InterruptedException {
        return getValue(record, value.getFieldType(), null, repository.getTypeManager());
    }

    /**
     *
     * @param indexValues optional, if supplied values will be added to this list, otherwise a new list
     *                    will be created and returned
     * @return null if there's no value
     */
    private List<IndexValue> getValue(IdRecord record, FieldType fieldType, List<IndexValue> indexValues,
            TypeManager typeManager) throws RepositoryException, InterruptedException {

        Object value;

        List<IndexValue> result;

        if (systemFields.isSystemField(fieldType.getId())) {
            value = systemFields.eval(record, fieldType, typeManager);
        } else if (record.hasField(fieldType.getId())) {
            value = record.getField(fieldType.getId());
        } else {
            value = null;
        }

        if (value == null) {
            return null;
        }

        if (fieldType.getValueType().getBaseName().equals("LIST")) {
            List<Object> values = (List<Object>)value;
            result = indexValues != null ? indexValues : new ArrayList<IndexValue>(values.size());
            for (int i = 0; i < values.size(); i++) {
                result.add(new IndexValue(record, fieldType, i, values.get(i)));
            }
            return result;
        } else {
            if (indexValues != null) {
                indexValues.add(new IndexValue(record, fieldType, value));
                result = indexValues;
            } else {
                result = Collections.singletonList(new IndexValue(record, fieldType, value));
            }
        }

        return result;
    }

    private List<IndexValue> evalDerefValue(DerefValue deref, IdRecord record, Repository repository, SchemaId vtag)
            throws RepositoryException, InterruptedException {
        FieldType fieldType = deref.getTargetFieldType();

        List<FollowRecord> records = new ArrayList<FollowRecord>();
        records.add(new FollowRecord(record, record));

        for (Follow follow : deref.getFollows()) {
            List<FollowRecord> linkedRecords = new ArrayList<FollowRecord>();

            for (FollowRecord item : records) {
                List<FollowRecord> evalResult = evalFollow(deref, follow, item, repository, vtag);
                if (evalResult != null) {
                    linkedRecords.addAll(evalResult);
                }
            }

            records = linkedRecords;
        }

        if (records.isEmpty())
            return null;

        List<IndexValue> result = new ArrayList<IndexValue>();
        for (FollowRecord item : records) {
            getValue(item.record, fieldType, result, repository.getTypeManager());
        }

        if (result.isEmpty())
            return null;

        return result;
    }

    /**
     * Evaluates a follow and returns the records that it points to. This method returns null in case there
     * are no results (link doesn't exist, points to non-existing doc, etc.).
     */
    private List<FollowRecord> evalFollow(DerefValue deref, Follow follow, FollowRecord record, Repository repository,
            SchemaId vtag) throws RepositoryException, InterruptedException {
        if (follow instanceof LinkFieldFollow) {
            List<IdRecord> records = evalLinkFieldFollow(deref, (LinkFieldFollow)follow, record, repository, vtag);
            return addContext(records);
        } else if (follow instanceof RecordFieldFollow) {
            List<IdRecord> records = evalRecordFieldFollow(deref, (RecordFieldFollow)follow, record, repository, vtag);
            return addContext(records, record.record);
        } else if (follow instanceof VariantFollow) {
            List<IdRecord> records = evalVariantFollow((VariantFollow)follow, record, repository, vtag);
            return addContext(records);
        } else if (follow instanceof MasterFollow) {
            List<IdRecord> records = evalMasterFollow((MasterFollow)follow, record, repository, vtag);
            return addContext(records);
        } else {
            throw new RuntimeException("Unexpected type of follow: " + follow.getClass().getName());
        }
    }

    private List<FollowRecord> addContext(List<IdRecord> records, IdRecord contextRecord) {
        if (records == null)
            return null;

        List<FollowRecord> result = new ArrayList<FollowRecord>();

        for (IdRecord record : records) {
            result.add(new FollowRecord(record, contextRecord));
        }

        return result;
    }

    private List<FollowRecord> addContext(List<IdRecord> records) {
        if (records == null)
            return null;

        List<FollowRecord> result = new ArrayList<FollowRecord>();

        for (IdRecord record : records) {
            result.add(new FollowRecord(record, record));
        }

        return result;
    }

    private List<IdRecord> evalLinkFieldFollow(DerefValue deref, LinkFieldFollow follow, FollowRecord frecord,
            Repository repository, SchemaId vtag) throws RepositoryException, InterruptedException {

        IdRecord record = frecord.record;
        FieldType fieldType = follow.getFieldType();

        if (!record.hasField(fieldType.getId())) {
            return null;
        }

        Object value = record.getField(fieldType.getId());
        if (value instanceof Link) {
            RecordId recordId = ((Link)value).resolve(frecord.contextRecord, repository.getIdGenerator());
            IdRecord linkedRecord = resolveRecordId(recordId, vtag, repository);
            return linkedRecord == null ? null : Collections.singletonList(linkedRecord);
        } else if (value instanceof List && ((List)value).size() > 0 && ((List)value).get(0) instanceof Link) {
            List list = (List)value;
            List<IdRecord> result = new ArrayList<IdRecord>(list.size());
            for (Object link : list) {
                RecordId recordId = ((Link)link).resolve(frecord.contextRecord, repository.getIdGenerator());
                IdRecord linkedRecord = resolveRecordId(recordId, vtag, repository);
                if (linkedRecord != null) {
                    result.add(linkedRecord);
                }
            }
            return list.isEmpty() ? null : result;
        } else {
            throw new RuntimeException("A link dereference is used but type is not LINK or LIST<LINK>, value: " +
                    value);
        }
    }

    private List<IdRecord> evalRecordFieldFollow(DerefValue deref, RecordFieldFollow follow, FollowRecord frecord,
            Repository repository, SchemaId vtag) throws RepositoryException, InterruptedException {

        IdRecord record = frecord.record;
        FieldType fieldType = follow.getFieldType();

        if (!record.hasField(fieldType.getId())) {
            return null;
        }

        Object value = record.getField(fieldType.getId());
        if (value instanceof Record) {
            return Collections.singletonList((IdRecord)value);
        } else if (value instanceof List && ((List)value).size() > 0 && ((List)value).get(0) instanceof Record) {
            List<IdRecord> records = (List<IdRecord>)value;
            return records.isEmpty() ? null : records;
        } else {
            throw new RuntimeException("A record dereference is used but type is not RECORD or LIST<RECORD>, value: " +
                    value);
        }
    }

    private IdRecord resolveRecordId(RecordId recordId, SchemaId vtag, Repository repository)
            throws RepositoryException, InterruptedException {
        try {
            // TODO we could limit this to only load the field necessary for the next follow in case this is not
            //      the last follow
            return VersionTag.getIdRecord(recordId, vtag, repository);
        } catch (RecordNotFoundException e) {
            // It's ok for a link to point to a non-existing record
            return null;
        } catch (VersionNotFoundException e) {
            // It's ok for a link to point to a non-existing record
            return null;
        }
    }

    private List<IdRecord> evalVariantFollow(VariantFollow follow, FollowRecord frecord, Repository repository,
            SchemaId vtag) throws RepositoryException, InterruptedException {

        RecordId recordId = frecord.record.getId();

        Map<String, String> varProps = new HashMap<String, String>(recordId.getVariantProperties());

        for (String dimension : follow.getDimensions()) {
            if (!varProps.containsKey(dimension)) {
                return null;
            }
            varProps.remove(dimension);
        }

        RecordId resolvedRecordId = repository.getIdGenerator().newRecordId(recordId.getMaster(), varProps);

        try {
            IdRecord lessDimensionedRecord = VersionTag.getIdRecord(resolvedRecordId, vtag, repository);
            return lessDimensionedRecord == null ? null : Collections.singletonList(lessDimensionedRecord);
        } catch (RecordNotFoundException e) {
            // It's ok that the variant does not exist
            return null;
        } catch (VersionNotFoundException e) {
            // It's ok that the variant does not exist
            return null;
        }
    }

    private List<IdRecord> evalMasterFollow(MasterFollow follow, FollowRecord frecord, Repository repository, SchemaId vtag)
            throws RepositoryException, InterruptedException {

        if (frecord.record.getId().isMaster())
            return null;

        RecordId masterId = frecord.record.getId().getMaster();

        try {
            IdRecord master = VersionTag.getIdRecord(masterId, vtag, repository);
            return master == null ? null : Collections.singletonList(master);
        } catch (RecordNotFoundException e) {
            // It's ok that the master does not exist
            return null;
        } catch (VersionNotFoundException e) {
            // It's ok that the master does not exist
            return null;
        }
    }

    /**
     * Combines a record object together with the record that needs to be used for evaluating links.
     *
     * <p>For real (non-nested) records, the two record objects are the same. In case of nested records,
     * the contextRecord is the real record to which it belongs. Nested records don't have ID's, thus
     * can't be used for resolving links.
     */
    private static final class FollowRecord {
        IdRecord record;
        IdRecord contextRecord;

        public FollowRecord(IdRecord record, IdRecord contextRecord) {
            this.record = record;
            this.contextRecord = contextRecord;
        }
    }
}
