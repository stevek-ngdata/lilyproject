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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.lilyproject.indexer.model.indexerconf.*;
import org.lilyproject.indexer.model.indexerconf.DerefValue.Follow;
import org.lilyproject.indexer.model.indexerconf.DerefValue.FieldFollow;
import org.lilyproject.indexer.model.indexerconf.DerefValue.VariantFollow;
import org.lilyproject.indexer.model.indexerconf.DerefValue.MasterFollow;
import org.lilyproject.indexer.model.indexerconf.Formatter;
import org.lilyproject.repository.api.*;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.repo.VersionTag;

import java.io.InputStream;
import java.util.*;

/**
 * Evaluates an index field value (a {@link Value}) to a value.
 */
public class ValueEvaluator {
    private Log log = LogFactory.getLog(getClass());

    private IndexerConf conf;

    private Parser tikaParser = new AutoDetectParser();

    public ValueEvaluator(IndexerConf conf) {
        this.conf = conf;
    }

    /**
     * Evaluates a value for a given record & vtag.
     *
     * @return null if there is no value
     */
    public List<String> eval(Value valueDef, IdRecord record, Repository repository, SchemaId vtag) {
        List<IndexValue> indexValues = evalValue(valueDef, record, repository, vtag);
        if (indexValues == null || indexValues.size() == 0)
            return null;

        if (valueDef.extractContent()) {
            return extractContent(indexValues, repository);
        }

        ValueType valueType = valueDef.getValueType();
        Formatter formatter = valueDef.getFormatter() != null ? conf.getFormatters().getFormatter(valueDef.getFormatter()) : conf.getFormatters().getFormatter(valueType);

        return formatter.format(indexValues, valueType);
    }

    private List<String> extractContent(List<IndexValue> indexValues, Repository repository) {
        // At this point we can be sure the value will be a blob, this is validated during
        // the construction of the indexer conf.

        List<String> result = new ArrayList<String>(indexValues.size());

        for (IndexValue indexValue : indexValues) {
            if (indexValue.fieldType.getValueType().isHierarchical()) {
                Object[] hierValue = ((HierarchyPath)indexValue.value).getElements();
                for (int i = 0; i < hierValue.length; i++) {
                    extractContent(hierValue[i], indexValue.record, indexValue.fieldType, indexValue.multiValueIndex, i, result, repository);
                }
            } else {
                extractContent(indexValue.value, indexValue.record, indexValue.fieldType, indexValue.multiValueIndex, null, result, repository);
            }
        }

        return result.isEmpty() ? null : result;
    }

    private void extractContent(Object value, Record record, FieldType fieldType, Integer multiValueIndex, Integer hierIndex, List<String> result, Repository repository) {
        Blob blob = (Blob)value;
        InputStream is = null;
        try {
            is = repository.getInputStream(record, fieldType.getName(), multiValueIndex, hierIndex);

            // TODO make write limit configurable
            BodyContentHandler ch = new BodyContentHandler();

            Metadata metadata = new Metadata();
            metadata.add(Metadata.CONTENT_TYPE, blob.getMediaType());
            if (blob.getName() != null)
                metadata.add(Metadata.RESOURCE_NAME_KEY, blob.getName());

            ParseContext parseContext = new ParseContext();

            tikaParser.parse(is, ch, metadata, parseContext);

            String text = ch.toString();
            if (text.length() > 0)
                result.add(text);

        } catch (Throwable t) {
            log.error("Error extracting blob content. Field '" + fieldType.getName() + "', record '"
                    + record.getId() + "'.", t);
        } finally {
            Closer.close(is);
        }
    }

    private List<IndexValue> evalValue(Value value, IdRecord record, Repository repository, SchemaId vtag) {
        if (value instanceof FieldValue) {
            return evalFieldValue((FieldValue)value, record, repository, vtag);
        } else if (value instanceof DerefValue) {
            return evalDerefValue((DerefValue)value, record, repository, vtag);
        } else {
            throw new RuntimeException("Unexpected type of value: " + value.getClass().getName());
        }
    }

    private List<IndexValue> evalFieldValue(FieldValue value, IdRecord record, Repository repository, SchemaId vtag) {
        SchemaId fieldId = value.getFieldType().getId();
        if (record.hasField(fieldId)) {
            if (value.getFieldType().getValueType().isMultiValue()) {
                List<Object> values = (List<Object>)record.getField(fieldId);
                List<IndexValue> result = new ArrayList<IndexValue>(values.size());
                for (int i = 0; i < values.size(); i++) {
                    result.add(new IndexValue(record, value.getFieldType(), i, values.get(i)));
                }
                return result;
            } else {
                return Collections.singletonList(new IndexValue(record, value.getFieldType(), record.getField(fieldId)));
            }
        } else {
            return null;
        }
    }

    private List<IndexValue> evalDerefValue(DerefValue deref, IdRecord record, Repository repository, SchemaId vtag) {
        FieldType fieldType = deref.getTargetFieldType();
        if (vtag.equals(VersionTag.VERSIONLESS_TAG) && fieldType.getScope() != Scope.NON_VERSIONED) {
            // From a versionless record, it is impossible to deref a versioned field.
            return null;
        }

        List<IdRecord> records = new ArrayList<IdRecord>();
        records.add(record);

        for (Follow follow : deref.getFollows()) {
            List<IdRecord> linkedRecords = new ArrayList<IdRecord>();

            for (IdRecord item : records) {
                List<IdRecord> evalResult = evalFollow(deref, follow, item, repository, vtag);
                if (evalResult != null) {
                    linkedRecords.addAll(evalResult);
                }
            }

            records = linkedRecords;
        }

        if (records.isEmpty())
            return null;

        List<IndexValue> result = new ArrayList<IndexValue>();
        for (IdRecord item : records) {
            if (item.hasField(fieldType.getId())) {
                Object value = item.getField(fieldType.getId());
                if (value != null) {
                    if (deref.getTargetField().getValueType().isMultiValue()) {
                        List<Object> multiValues = (List<Object>)value;
                        for (int r = 0; r < multiValues.size(); r++) {
                            result.add(new IndexValue(item, fieldType, r, multiValues.get(r)));
                        }
                    } else {
                        result.add(new IndexValue(item, fieldType, value));
                    }
                }
            }
        }

        if (result.isEmpty())
            return null;

        return result;
    }

    private List<IdRecord> evalFollow(DerefValue deref, Follow follow, IdRecord record, Repository repository, SchemaId vtag) {
        if (follow instanceof FieldFollow) {
            return evalFieldFollow(deref, (FieldFollow)follow, record, repository, vtag);
        } else if (follow instanceof VariantFollow) {
            return evalVariantFollow((VariantFollow)follow, record, repository, vtag);
        } else if (follow instanceof MasterFollow) {
            return evalMasterFollow((MasterFollow)follow, record, repository, vtag);
        } else {
            throw new RuntimeException("Unexpected type of follow: " + follow.getClass().getName());
        }
    }

    private List<IdRecord> evalFieldFollow(DerefValue deref, FieldFollow follow, IdRecord record, Repository repository, SchemaId vtag) {
        FieldType fieldType = follow.getFieldType();

        if (!record.hasField(fieldType.getId())) {
            return null;
        }

        if (vtag.equals(VersionTag.VERSIONLESS_TAG) && fieldType.getScope() != Scope.NON_VERSIONED) {
            // From a versionless record, it is impossible to deref a versioned field.
            // This explicit check could be removed if in case of the versionless vtag we only read
            // the non-versioned fields of the record. However, it is not possible to do this right
            // now with the repository API.
            return null;
        }

        Object value = record.getField(fieldType.getId());
        if (value instanceof Link) {
            RecordId recordId = ((Link)value).resolve(record, repository.getIdGenerator());
            IdRecord linkedRecord = resolveRecordId(recordId, vtag, repository);
            return linkedRecord == null ? null : Collections.singletonList(linkedRecord);
        } else if (value instanceof List && ((List)value).size() > 0 && ((List)value).get(0) instanceof Link) {
            List list = (List)value;
            List<IdRecord> result = new ArrayList<IdRecord>(list.size());
            for (Object link : list) {
                RecordId recordId = ((Link)link).resolve(record, repository.getIdGenerator());
                IdRecord linkedRecord = resolveRecordId(recordId, vtag, repository);
                if (linkedRecord != null) {
                    result.add(linkedRecord);
                }
            }
            return list.isEmpty() ? null : result;
        }
        return null;
    }

    private IdRecord resolveRecordId(RecordId recordId, SchemaId vtag, Repository repository) {
        try {
            // TODO we could limit this to only load the field necessary for the next follow
            return VersionTag.getIdRecord(recordId, vtag, repository);
        } catch (Exception e) {
            return null;
        }
    }

    private List<IdRecord> evalVariantFollow(VariantFollow follow, IdRecord record, Repository repository, SchemaId vtag) {
        RecordId recordId = record.getId();

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
        } catch (Exception e) {
            return null;
        }
    }

    private List<IdRecord> evalMasterFollow(MasterFollow follow, IdRecord record, Repository repository, SchemaId vtag) {
        if (record.getId().isMaster())
            return null;

        RecordId masterId = record.getId().getMaster();

        try {
            IdRecord master = VersionTag.getIdRecord(masterId, vtag, repository);
            return master == null ? null : Collections.singletonList(master);
        } catch (Exception e) {
            return null;
        }
    }
}
