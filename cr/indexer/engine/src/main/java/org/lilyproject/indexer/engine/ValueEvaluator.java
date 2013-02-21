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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;

import com.google.common.collect.Lists;
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
import org.lilyproject.indexer.model.indexerconf.FollowCallback;
import org.lilyproject.indexer.model.indexerconf.Formatter;
import org.lilyproject.indexer.model.indexerconf.IndexUpdateBuilder;
import org.lilyproject.indexer.model.indexerconf.IndexValue;
import org.lilyproject.indexer.model.indexerconf.IndexerConf;
import org.lilyproject.indexer.model.indexerconf.Value;
import org.lilyproject.repository.api.Blob;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.util.hbase.LilyHBaseSchema.Table;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.repo.SystemFields;

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
            throws RepositoryException, IOException, InterruptedException {

        List<IndexValue> indexValues = evalValue(valueDef, indexUpdateBuilder);

        if (indexValues == null || indexValues.size() == 0)
            return null;

        RepositoryManager repositoryManager = indexUpdateBuilder.getRepositoryManager();
        if (valueDef.extractContent()) {
            return extractContent(indexValues, repositoryManager);
        }

        Formatter formatter = conf.getFormatters().getFormatter(valueDef.getFormatter());

        return formatter.format(indexValues, repositoryManager);
    }

    /**
     * Direct 'evaluation' (content extraction, formatting) of a given field
     * from a record. Should only be called if the field is present in the
     * record.
     */
    public List<String> format(Record record, FieldType fieldType, boolean extractContent, String formatterName,
            RepositoryManager repositoryManager) throws InterruptedException {
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
            return extractContent(indexValues, repositoryManager);
        }

        Formatter formatter = conf.getFormatters().getFormatter(formatterName);

        return formatter.format(indexValues, repositoryManager);
    }

    private List<String> extractContent(List<IndexValue> indexValues, RepositoryManager repositoryManager) {
        // At this point we can be sure the value will be a blob, this is
        // validated during
        // the construction of the indexer conf.

        List<String> result = new ArrayList<String>(indexValues.size());

        Deque<Integer> indexes = new ArrayDeque<Integer>();

        for (IndexValue indexValue : indexValues) {
            indexes.clear();

            if (indexValue.listIndex != null)
                indexes.addLast(indexValue.listIndex);

            extractContent(indexValue.value, indexes, indexValue.record, indexValue.fieldType, result, repositoryManager);
        }

        return result.isEmpty() ? null : result;
    }

    private void extractContent(Object value, Deque<Integer> indexes, Record record, FieldType fieldType,
            List<String> result, RepositoryManager repositoryManager) {

        if (value instanceof List) { // this covers both LIST and PATH types
            List values = (List) value;
            for (int i = 0; i < values.size(); i++) {
                indexes.addLast(i);
                extractContent(values.get(i), indexes, record, fieldType, result, repositoryManager);
                indexes.removeLast();
            }
        } else {
            extractContent(value, record, fieldType, Ints.toArray(indexes), result, repositoryManager);
        }
    }

    private void extractContent(Object value, Record record, FieldType fieldType, int[] indexes, List<String> result,
            RepositoryManager repositoryManager) {

        Blob blob = (Blob) value;
        InputStream is = null;

        // TODO make write limit configurable
        WriteOutContentHandler woh = new WriteOutContentHandler(500 * 1000); // 500K limit (Tika default: 100K)
        BodyContentHandler ch = new BodyContentHandler(woh);

        try {
            is = repositoryManager.getRepository(Table.RECORD.name).getInputStream(record, fieldType.getName(), indexes);

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
            throws RepositoryException, IOException, InterruptedException {
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
            throws RepositoryException, IOException, InterruptedException {
        evalDerefValue(deref, 0, indexUpdateBuilder, values);
    }

    /**
     * Evaluates a follow and returns the records that it points to. This method
     * returns null in case there are no results (link doesn't exist, points to
     * non-existing doc, etc.).
     */
    public void evalDerefValue(final DerefValue deref, final int fieldNum,
            final IndexUpdateBuilder indexUpdateBuilder, final List<IndexValue> values)
                    throws RepositoryException, IOException, InterruptedException {
        if (fieldNum >= deref.getFollows().size()) {
            getValue(indexUpdateBuilder, deref.getTargetFieldType(), values);
            return;
        }

        Follow follow = deref.getFollows().get(fieldNum);
        follow.follow(indexUpdateBuilder, new FollowCallback() {
            @Override
            public void call() throws RepositoryException, IOException, InterruptedException {
                evalDerefValue(deref, fieldNum + 1, indexUpdateBuilder, values);
            }
        });

    }

    private Object getValue(IndexUpdateBuilder indexUpdateBuilder, FieldType fieldType) throws RepositoryException,
            InterruptedException {
        Object value = null;
        Record record = indexUpdateBuilder.getRecordContext().record;
        if (systemFields.isSystemField(fieldType.getName())) {
            if (record != null) {
                value = systemFields.eval(record, fieldType, indexUpdateBuilder.getRepositoryManager().getTypeManager());
            }
        } else {
            indexUpdateBuilder.addDependency(fieldType.getId());
            if (record != null && record.hasField(fieldType.getName())) {
                value = record.getField(fieldType.getName());
            }
        }
        return value;
    }

}
