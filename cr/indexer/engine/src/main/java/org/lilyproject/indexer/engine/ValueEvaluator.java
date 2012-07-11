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
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.WriteOutContentHandler;
import org.lilyproject.indexer.model.indexerconf.Dep;
import org.lilyproject.indexer.model.indexerconf.DerefValue;
import org.lilyproject.indexer.model.indexerconf.FieldValue;
import org.lilyproject.indexer.model.indexerconf.Follow;
import org.lilyproject.indexer.model.indexerconf.Formatter;
import org.lilyproject.indexer.model.indexerconf.ForwardVariantFollow;
import org.lilyproject.indexer.model.indexerconf.IndexUpdateBuilder;
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
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.IdRecord;
import org.lilyproject.repository.api.IdRecordScanner;
import org.lilyproject.repository.api.Link;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.RecordNotFoundException;
import org.lilyproject.repository.api.RecordScan;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.ValueType;
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
     * @return null if there is no value
     */
    public List<String> eval(Value valueDef, IndexUpdateBuilder indexUpdateBuilder)
            throws RepositoryException, InterruptedException {

        List<IndexValue> indexValues = evalValue(valueDef, indexUpdateBuilder);

        if (indexValues == null || indexValues.size() == 0)
            return null;

        Repository repository = indexUpdateBuilder.getRepository();
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

    private List<IndexValue> evalValue(Value value, IndexUpdateBuilder indexUpdateBuilder)
            throws RepositoryException, InterruptedException {
        if (value instanceof FieldValue) {
            return getValue(indexUpdateBuilder, ((FieldValue)value).getTargetFieldType(), null);
        } else if (value instanceof DerefValue) {
            List<IndexValue> result = Lists.newArrayList();
            evalDerefValue((DerefValue) value, indexUpdateBuilder, result);
            return result;
        } else {
            throw new RuntimeException("Unexpected type of value: " + value.getClass().getName());
        }
    }

    /**
     * @param indexValues
     *            optional, if supplied values will be added to this list,
     *            otherwise a new list will be created and returned
     * @return null if there's no value
     */
    private List<IndexValue> getValue(IndexUpdateBuilder indexUpdateBuilder, FieldType fieldType, List<IndexValue> indexValues) throws RepositoryException, InterruptedException {
        Record record = indexUpdateBuilder.getRecordContext().record;
        Object value = getValue(indexUpdateBuilder, fieldType);

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

    private void evalDerefValue(DerefValue deref, IndexUpdateBuilder indexUpdateBuilder, List<IndexValue> values)
            throws RepositoryException, InterruptedException {
        evalDerefValue(deref, 0, indexUpdateBuilder, values);
    }

    /**
     * Evaluates a follow and returns the records that it points to. This method
     * returns null in case there are no results (link doesn't exist, points to
     * non-existing doc, etc.).
     */
    public void evalDerefValue(DerefValue deref, int fieldNum,
            IndexUpdateBuilder indexUpdateBuilder, List<IndexValue> values) throws RepositoryException, InterruptedException {
        if (fieldNum >= deref.getFollows().size()) {
            getValue(indexUpdateBuilder, deref.getTargetFieldType(), values);
            return;
        }

        Follow follow = deref.getFollows().get(fieldNum);
        RecordContext ctx = indexUpdateBuilder.getRecordContext();
        Repository repository = indexUpdateBuilder.getRepository();
        IdGenerator idGenerator = repository.getIdGenerator();

        // FIXME: some code duplication (see ForEachNode)
        if (follow instanceof LinkFieldFollow) {
            FieldType fieldType = ((LinkFieldFollow)follow).getFieldType();
            indexUpdateBuilder.addDependency(fieldType.getId());

            // FIXME: it's more efficient to read all records at once (see ForEachNode for duplicated code)
            // but make sure missing records are also treated (handled here via null linkedRecord in case of RecordNotFoundException
            if (ctx.record != null) {
                List links = flatList(ctx.record, fieldType);
                for (Link link: (List<Link>)links) {
                    RecordId linkedRecordId = link.resolve(ctx.contextRecord, idGenerator);
                    Record linkedRecord = null;
                    try {
                        linkedRecord = repository.read(linkedRecordId);
                    } catch (RecordNotFoundException rnfe) {
                        // ok
                    }
                    indexUpdateBuilder.push(linkedRecord, new Dep(linkedRecordId, Collections.<String>emptySet()));
                    evalDerefValue(deref, fieldNum + 1, indexUpdateBuilder, values);
                    indexUpdateBuilder.pop();
                }
            }

        } else if (follow instanceof RecordFieldFollow) {
            FieldType fieldType = ((RecordFieldFollow)follow).getFieldType();
            if (!systemFields.isSystemField(fieldType.getName())) {
                indexUpdateBuilder.addDependency(fieldType.getId());
            }
            if (ctx.record != null) {
                List records = flatList(ctx.record, fieldType);
                for (Record record: (List<Record>)records) {
                    indexUpdateBuilder.push(record, ctx.contextRecord, ctx.dep); // TODO: pass null instead of ctx.dep?
                    evalDerefValue(deref, fieldNum + 1, indexUpdateBuilder, values);
                    indexUpdateBuilder.pop();
                }
            }
        } else if (follow instanceof MasterFollow) {
            evalMasterFollow(deref, fieldNum, indexUpdateBuilder, values);
        } else if (follow instanceof VariantFollow) {
            evalVariantFollow(deref, fieldNum, indexUpdateBuilder, values);
        } else if (follow instanceof ForwardVariantFollow) {
            evalForwardVariantFollow(deref, fieldNum, indexUpdateBuilder, values);
        } else {
            throw new RuntimeException("Unexpected type of follow: " + follow.getClass().getName());
        }
    }

    private Object getValue(IndexUpdateBuilder indexUpdateBuilder, FieldType fieldType) throws RepositoryException,
            InterruptedException {
        Object value = null;
        Record record = indexUpdateBuilder.getRecordContext().record;
        if (systemFields.isSystemField(fieldType.getName())) {
            if (record != null) {
                value = systemFields.eval(record, fieldType, indexUpdateBuilder.getRepository().getTypeManager());
            }
        } else {
            indexUpdateBuilder.addDependency(fieldType.getId());
            if (record != null && record.hasField(fieldType.getName())) {
                value = record.getField(fieldType.getName());
            }
        }
        return value;
    }

    private void evalVariantFollow(DerefValue deref, int fieldNum, IndexUpdateBuilder indexUpdateBuilder, List<IndexValue> values) throws RepositoryException, InterruptedException {
        VariantFollow follow = (VariantFollow)deref.getFollows().get(fieldNum);
        RecordContext ctx = indexUpdateBuilder.getRecordContext();
        IdGenerator idGenerator = indexUpdateBuilder.getRepository().getIdGenerator();


        Set<String> dimensions = follow.getDimensions();
        Set<String> currentDimensions = Sets.newHashSet(ctx.dep.id.getVariantProperties().keySet());
        currentDimensions.addAll(ctx.dep.vprops);

        if (!currentDimensions.containsAll(dimensions)) {
            // the current dimension doesn't contain all the dimensions we need to subtract -> stop here
            return;
        }
        Dep newDep = ctx.dep.minus(idGenerator, dimensions);

        Record lessDimensionedRecord = null;
        try {
            lessDimensionedRecord = VersionTag.getIdRecord(newDep.id, indexUpdateBuilder.getVTag(), indexUpdateBuilder.getRepository());
        } catch (RecordNotFoundException e) {
            // It's ok that the variant does not exist
        } catch (VersionNotFoundException e) {
            // It's ok that the variant does not exist
        }

        indexUpdateBuilder.push(lessDimensionedRecord, newDep);
        evalDerefValue(deref, fieldNum + 1, indexUpdateBuilder, values);
        indexUpdateBuilder.pop();
    }

    private void evalForwardVariantFollow(DerefValue deref, int fieldNum, IndexUpdateBuilder indexUpdateBuilder, List<IndexValue> values) throws RepositoryException, InterruptedException {
        ForwardVariantFollow follow = (ForwardVariantFollow) deref.getFollows().get(fieldNum);
        RecordContext ctx = indexUpdateBuilder.getRecordContext();
        IdGenerator idGenerator = indexUpdateBuilder.getRepository().getIdGenerator();

        Map<String, String> dimensions = ((ForwardVariantFollow)follow).getDimensions();
        Set<String> currentDimensions = Sets.newHashSet(ctx.dep.vprops);
        currentDimensions.addAll(ctx.dep.id.getVariantProperties().keySet());

        if (currentDimensions.containsAll(dimensions.keySet())) {
            // the record already contains all of the new dimensions -> stop here
            return;
        } else {
            Dep newDep = ctx.dep.plus(idGenerator, dimensions);
            // now find all the records of this newly defined variant
            final ArrayList<Record> result = scanVariants(indexUpdateBuilder, newDep);

            for (Record record: result) {
                indexUpdateBuilder.push(record, newDep);
                evalDerefValue(deref, fieldNum + 1, indexUpdateBuilder, values);
                indexUpdateBuilder.pop();
            }
        }
    }

    // FIXME: duplicated code - see ForEachNode
    public ArrayList<Record> scanVariants(IndexUpdateBuilder indexUpdateBuilder, Dep newDep) throws RepositoryException, InterruptedException {

        // build a variant properties map which is a combination of dep.id.variantProperties + dep.vprops
        final Map<String, String> varProps = new HashMap<String, String>(newDep.id.getVariantProperties());
        varProps.putAll(newDep.id.getVariantProperties());
        for (String vprop: newDep.vprops) {
            varProps.put(vprop, null);
        }

        final ArrayList<Record> result = new ArrayList<Record>();

        final RecordScan scan = new RecordScan();
        scan.setRecordFilter(new RecordVariantFilter(newDep.id.getMaster(), varProps));
        Repository repository = indexUpdateBuilder.getRepository();
        final IdRecordScanner scanner = repository.getScannerWithIds(scan);
        IdRecord next;
        while ((next = scanner.next()) != null) {
            final Record record = VersionTag.getIdRecord(next, indexUpdateBuilder.getVTag(), indexUpdateBuilder.getRepository());
            if (record != null)
                result.add(record);
        }

        scanner.close();
        return result;
    }

    private void evalMasterFollow(DerefValue deref, int fieldNum, IndexUpdateBuilder indexUpdateBuilder, List<IndexValue> values) throws RepositoryException, InterruptedException {
        RecordContext ctx = indexUpdateBuilder.getRecordContext();

        Dep masterDep = new Dep(ctx.dep.id.getMaster(), Collections.<String>emptySet());
        if (ctx.dep.id.isMaster()) {
            // We're already on a master record, stop here
            return;
        }

        Record master = null;
        try {
            master = VersionTag.getIdRecord(masterDep.id, indexUpdateBuilder.getVTag(), indexUpdateBuilder.getRepository());
        } catch (RecordNotFoundException e) {
            // It's ok that the master does not exist
        } catch (VersionNotFoundException e) {
            // It's ok that the master does not exist
        }

        indexUpdateBuilder.push(master, masterDep);
        evalDerefValue(deref, fieldNum + 1, indexUpdateBuilder, values);
        indexUpdateBuilder.pop();
    }

    private List flatList(Record record, FieldType fieldType) {
        if (record != null && record.hasField(fieldType.getName())) {
            return flatList(record.getField(fieldType.getName()), fieldType.getValueType());
        } else {
            return Collections.emptyList();
        }
    }

    private List flatList(Object value, ValueType type) {
        if (type.getBaseName().equals("LIST")) {
            if (type.getNestedValueType().getBaseName() != "LIST") {
                return (List)value;
            } else {
                List result = Lists.newArrayList();
                for (Object nValue: (List)value) {
                    result.addAll(flatList(nValue, type.getNestedValueType()));
                }
                return result;
            }
        } else {
            return Collections.singletonList(value);
        }
    }

}
