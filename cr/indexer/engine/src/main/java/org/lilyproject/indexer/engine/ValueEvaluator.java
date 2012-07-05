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

import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.primitives.Ints;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.WriteOutContentHandler;
import org.lilyproject.indexer.model.indexerconf.DerefValue;
import org.lilyproject.indexer.model.indexerconf.FieldValue;
import org.lilyproject.indexer.model.indexerconf.Follow;
import org.lilyproject.indexer.model.indexerconf.FollowRecord;
import org.lilyproject.indexer.model.indexerconf.Formatter;
import org.lilyproject.indexer.model.indexerconf.ForwardVariantFollow;
import org.lilyproject.indexer.model.indexerconf.IndexValue;
import org.lilyproject.indexer.model.indexerconf.IndexerConf;
import org.lilyproject.indexer.model.indexerconf.LinkFieldFollow;
import org.lilyproject.indexer.model.indexerconf.MasterFollow;
import org.lilyproject.indexer.model.indexerconf.RecordContext;
import org.lilyproject.indexer.model.indexerconf.RecordFieldFollow;
import org.lilyproject.indexer.model.indexerconf.Value;
import org.lilyproject.indexer.model.indexerconf.VariantFollow;
import org.lilyproject.repository.api.Blob;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.IdRecord;
import org.lilyproject.repository.api.IdRecordScanner;
import org.lilyproject.repository.api.Link;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.RecordNotFoundException;
import org.lilyproject.repository.api.RecordScan;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.VersionNotFoundException;
import org.lilyproject.repository.api.filter.RecordVariantFilter;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.repo.SystemFields;
import org.lilyproject.util.repo.VersionTag;

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
    public List<String> eval(Value valueDef, RecordContext recordContext, Repository repository, SchemaId vtag)
            throws RepositoryException, InterruptedException {

        List<IndexValue> indexValues = evalValue(valueDef, recordContext, repository, vtag);
        if (indexValues == null || indexValues.size() == 0)
            return null;

        if (valueDef.extractContent()) {
            return extractContent(indexValues, repository);
        }

        Formatter formatter = conf.getFormatters().getFormatter(valueDef.getFormatter());

        return formatter.format(indexValues, repository);
    }

    /**
     * Direct 'evaluation' (content extraction, formatting) of a given field
     * from a record. Should only be called if the field is present in the
     * record.
     */
    public List<String> format(Record record, FieldType fieldType, boolean extractContent, String formatterName,
            Repository repository) throws InterruptedException {
        Object value = record.getField(fieldType.getName());

        List<IndexValue> indexValues;

        if (fieldType.getValueType().getBaseName().equals("LIST")) {
            List<Object> values = (List<Object>) value;
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
        // At this point we can be sure the value will be a blob, this is
        // validated during
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
            List values = (List) value;
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

        Blob blob = (Blob) value;
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
                log.error(
                        "Error extracting blob content. Field '" + fieldType.getName() + "', record '" + record.getId()
                                + "'.", t);
                return;
            }
        } finally {
            Closer.close(is);
        }

        String text = ch.toString();
        if (text.length() > 0)
            result.add(text);
    }

    private List<IndexValue> evalValue(Value value, RecordContext recordContext, Repository repository, SchemaId vtag)
            throws RepositoryException, InterruptedException {
        if (value instanceof FieldValue) {
            return evalFieldValue((FieldValue) value, recordContext, repository);
        } else if (value instanceof DerefValue) {
            return evalDerefValue((DerefValue) value, recordContext, repository, vtag);
        } else {
            throw new RuntimeException("Unexpected type of value: " + value.getClass().getName());
        }
    }

    private List<IndexValue> evalFieldValue(FieldValue value, RecordContext recordContext, Repository repository)
            throws RepositoryException, InterruptedException {
        return getValue(recordContext.last(), value.getFieldType(), null, repository);
    }

    /**
     * @param indexValues
     *            optional, if supplied values will be added to this list,
     *            otherwise a new list will be created and returned
     * @return null if there's no value
     */
    private List<IndexValue> getValue(Record record, FieldType fieldType, List<IndexValue> indexValues,
            Repository repository) throws RepositoryException, InterruptedException {

        Object value = getValue(repository, record, fieldType);

        List<IndexValue> result;

        if (value == null) {
            return null;
        }

        if (fieldType.getValueType().getBaseName().equals("LIST")) {
            List<Object> values = (List<Object>) value;
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

    private List<IndexValue> evalDerefValue(DerefValue deref, RecordContext recordContext, Repository repository, SchemaId vtag)
            throws RepositoryException, InterruptedException {
        FieldType fieldType = deref.getTargetFieldType();

        List<FollowRecord> records = new ArrayList<FollowRecord>();
        records.add(new FollowRecord(recordContext.lastReal(), recordContext.last()));

        for (Follow follow : deref.getFollows()) {
            List<FollowRecord> linkedRecords = new ArrayList<FollowRecord>();

            for (FollowRecord item: records) {
                List<FollowRecord> evalResult = evalFollow(follow, item, repository, vtag);
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
            getValue(item.record, fieldType, result, repository);
        }

        if (result.isEmpty())
            return null;

        return result;
    }

    /**
     * Evaluates a follow and returns the records that it points to. This method
     * returns null in case there are no results (link doesn't exist, points to
     * non-existing doc, etc.).
     */
    public List<FollowRecord> evalFollow(Follow follow, FollowRecord record, Repository repository, SchemaId vtag) throws RepositoryException, InterruptedException {
        if (follow instanceof LinkFieldFollow) {
            List<Record> records = evalLinkFieldFollow((LinkFieldFollow) follow, record, repository, vtag);
            return addContext(records);
        } else if (follow instanceof RecordFieldFollow) {
            List<Record> records = evalRecordFieldFollow((RecordFieldFollow) follow, record, repository, vtag);
            return addContext(records, record.record);
        } else if (follow instanceof VariantFollow) {
            List<Record> records = evalVariantFollow((VariantFollow) follow, record, repository, vtag);
            return addContext(records);
        } else if (follow instanceof ForwardVariantFollow) {
            List<Record> records = evalForwardVariantFollow((ForwardVariantFollow) follow, record,
                    repository, vtag);
            return addContext(records);
        } else if (follow instanceof MasterFollow) {
            List<Record> records = evalMasterFollow((MasterFollow) follow, record, repository, vtag);
            return addContext(records);
        } else {
            throw new RuntimeException("Unexpected type of follow: " + follow.getClass().getName());
        }
    }

    private List<FollowRecord> addContext(List<Record> records, Record contextRecord) {
        if (records == null)
            return null;

        List<FollowRecord> result = new ArrayList<FollowRecord>();

        for (Record record : records) {
            result.add(new FollowRecord(record, contextRecord));
        }

        return result;
    }

    private List<FollowRecord> addContext(List<Record> records) {
        if (records == null)
            return null;

        List<FollowRecord> result = new ArrayList<FollowRecord>();

        for (Record record : records) {
            result.add(new FollowRecord(record, record));
        }

        return result;
    }

    private List<Record> evalLinkFieldFollow(LinkFieldFollow follow, FollowRecord frecord, Repository repository,
            SchemaId vtag) throws RepositoryException, InterruptedException {

        Record record = frecord.record;
        FieldType fieldType = follow.getFieldType();

        Object value = getValue(repository, record, fieldType);

        if (value == null) {
            return Collections.emptyList();
        }

        if (value instanceof Link) {
            RecordId recordId = ((Link) value).resolve(frecord.contextRecord, repository.getIdGenerator());
            Record linkedRecord = resolveRecordId(recordId, vtag, repository);
            return linkedRecord == null ? null : Collections.singletonList(linkedRecord);
        } else if (value instanceof List && ((List) value).size() > 0 && ((List) value).get(0) instanceof Link) {
            List list = (List) value;
            List<Record> result = new ArrayList<Record>(list.size());
            for (Object link : list) {
                RecordId recordId = ((Link) link).resolve(frecord.contextRecord, repository.getIdGenerator());
                Record linkedRecord = resolveRecordId(recordId, vtag, repository);
                if (linkedRecord != null) {
                    result.add(linkedRecord);
                }
            }
            return list.isEmpty() ? null : result;
        } else {
            throw new RuntimeException("A link dereference is used but type is not LINK or LIST<LINK>, value: " + value);
        }
    }

    private Object getValue(Repository repository, Record record, FieldType fieldType) throws RepositoryException,
            InterruptedException {
        Object value = null;
        if (systemFields.isSystemField(fieldType.getName())) {
            value = systemFields.eval(record, fieldType, repository.getTypeManager());
        } else if (record.hasField(fieldType.getName())) {
            value = record.getField(fieldType.getName());
        }
        return value;
    }

    private List<Record> evalRecordFieldFollow(RecordFieldFollow follow, FollowRecord frecord, Repository repository,
            SchemaId vtag) throws RepositoryException, InterruptedException {

        Record record = frecord.record;
        FieldType fieldType = follow.getFieldType();

        Object value = getValue(repository, record, fieldType);

        if (value == null)
            return Collections.emptyList();

        if (value instanceof Record) {
            return Collections.singletonList((Record) value);
        } else if (value instanceof List && ((List) value).size() > 0 && ((List) value).get(0) instanceof Record) {
            List<Record> records = (List<Record>) value;
            return records.isEmpty() ? null : records;
        } else {
            throw new RuntimeException("A record dereference is used but type is not RECORD or LIST<RECORD>, value: "
                    + value);
        }
    }

    private Record resolveRecordId(RecordId recordId, SchemaId vtag, Repository repository)
            throws RepositoryException, InterruptedException {
        try {
            // TODO we could limit this to only load the field necessary for the
            // next follow in case this is not the last follow
            return VersionTag.getIdRecord(recordId, vtag, repository);
        } catch (RecordNotFoundException e) {
            // It's ok for a link to point to a non-existing record
            return null;
        } catch (VersionNotFoundException e) {
            // It's ok for a link to point to a non-existing record
            return null;
        }
    }

    private List<Record> evalVariantFollow(VariantFollow follow, FollowRecord frecord, Repository repository,
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
            Record lessDimensionedRecord = VersionTag.getIdRecord(resolvedRecordId, vtag, repository);
            return lessDimensionedRecord == null ? null : Collections.singletonList(lessDimensionedRecord);
        } catch (RecordNotFoundException e) {
            // It's ok that the variant does not exist
            return null;
        } catch (VersionNotFoundException e) {
            // It's ok that the variant does not exist
            return null;
        }
    }

    private List<Record> evalForwardVariantFollow(ForwardVariantFollow follow, FollowRecord frecord,
            Repository repository, SchemaId vtag) throws RepositoryException, InterruptedException {

        RecordId recordId = frecord.record.getId();

        if (recordId.getVariantProperties().keySet().containsAll(follow.getDimensions().keySet())) {
            // the record already contains all of the variant dimension -> stop
            // here
            return null;
        } else {
            // build a new set of variant properties which are the ones we
            // started with + the dimensions to follow
            final Map<String, String> varProps = new HashMap<String, String>(recordId.getVariantProperties());
            for (Map.Entry<String, String> dimension : follow.getDimensions().entrySet()) {
                varProps.put(dimension.getKey(), dimension.getValue());
            }

            // now find all the records of this newly defined variant
            final ArrayList<Record> result = new ArrayList<Record>();

            final RecordScan scan = new RecordScan();
            scan.setRecordFilter(new RecordVariantFilter(recordId.getMaster(), varProps));
            final IdRecordScanner scanner = repository.getScannerWithIds(scan);
            IdRecord next;
            while ((next = scanner.next()) != null) {
                final Record record = VersionTag.getIdRecord(next, vtag, repository);
                if (record != null)
                    result.add(record);
            }

            scanner.close();

            return result;
        }
    }

    private List<Record> evalMasterFollow(MasterFollow follow, FollowRecord frecord, Repository repository,
            SchemaId vtag) throws RepositoryException, InterruptedException {

        if (frecord.record.getId().isMaster())
            return null;

        RecordId masterId = frecord.record.getId().getMaster();

        try {
            Record master = VersionTag.getIdRecord(masterId, vtag, repository);
            return master == null ? null : Collections.singletonList(master);
        } catch (RecordNotFoundException e) {
            // It's ok that the master does not exist
            return null;
        } catch (VersionNotFoundException e) {
            // It's ok that the master does not exist
            return null;
        }
    }

}
