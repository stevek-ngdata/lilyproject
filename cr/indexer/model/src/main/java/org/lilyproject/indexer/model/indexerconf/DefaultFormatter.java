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
package org.lilyproject.indexer.model.indexerconf;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.lilyproject.repository.api.*;

import java.util.*;

/**
 * A default implementation of a formatter than is able to treat any value.
 */
public class DefaultFormatter implements Formatter {

    private static final Map<String, ValueFormatter> FORMATTERS;
    static {
        FORMATTERS = new HashMap<String, ValueFormatter>();
        FORMATTERS.put("LIST", new ListFormatter());
        FORMATTERS.put("PATH", new PathFormatter());
        FORMATTERS.put("RECORD", new RecordFormatter());
        FORMATTERS.put("DATE", new DateFormatter());
        FORMATTERS.put("DATETIME", new DateTimeFormatter());
    }
    private static final ValueFormatter ALL_FORMATTER = new AllFormatter();

    @Override
    public List<String> format(List<IndexValue> indexValues, RepositoryManager repositoryManager)
            throws InterruptedException {

        List<String> results = new ArrayList<String>();

        for (IndexValue value : filterValues(indexValues)) {
            FormatContext formatCtx = new FormatContext(repositoryManager);

            ValueType valueType = value.fieldType.getValueType();
            if (valueType.getBaseName().equals("LIST")) {
                // The values of the first list-level are supplied as individual IndexValues
                valueType = valueType.getNestedValueType();
            }

            String result = formatCtx.format(value.value, valueType, formatCtx);

            if (result != null) {
                results.add(result);
            } else {
                results.addAll(formatCtx.results);
            }
        }

        return results;
    }

    protected List<IndexValue> filterValues(List<IndexValue> indexValues) {
        return indexValues;
    }

    protected ValueFormatter getFormatter(ValueType valueType) {
        ValueFormatter formatter = FORMATTERS.get(valueType.getBaseName());
        return formatter == null ? ALL_FORMATTER : formatter;
    }

    public class FormatContext implements ValueFormatter {
        /**
         * This stack can be useful for formatters that want to behave differently depending on
         * how they are nested in other types. For example in case of nested lists, you might
         * want to format each nesting level differently.
         */
        Deque<ValueType> valueTypeStack = new ArrayDeque<ValueType>();
        RepositoryManager repositoryManager;
        List<String> results = new ArrayList<String>();

        public FormatContext(RepositoryManager repositoryManager) {
            this.repositoryManager = repositoryManager;
        }

        @Override
        public String format(Object value, ValueType valueType, FormatContext formatCtx) throws InterruptedException {
            valueTypeStack.push(valueType);

            String result = getFormatter(valueType).format(value, valueType, this);

            valueTypeStack.pop();

            return result;
        }
    }

    public static interface ValueFormatter {
        /**
         * This method has the choice of either returning a formatted value, or appending it to the supplied
         * results list. Appending it to the results list has the effect of sending a multi-value to the indexer
         * (so the index field needs to support multiple values), while returning a string will cause the value
         * to be used by the parent formatter.
         */
        public String format(Object value, ValueType valueType, FormatContext formatCtx) throws InterruptedException;
    }


    public static void appendNonNull(StringBuilder builder, String text) {
        if (text != null) {
            builder.append(text);
        }
    }

    public static String returnBuilderResult(StringBuilder builder) {
        return builder.length() > 0 ? builder.toString() : null;
    }

    protected static class ListFormatter implements ValueFormatter {
        @Override
        public String format(Object list, ValueType valueType, FormatContext formatCtx) throws InterruptedException {
            StringBuilder builder = new StringBuilder();

            for (Object value : (List)list) {
                String formatted = formatCtx.format(value, valueType.getNestedValueType(), formatCtx);

                // separate the values by a space
                if (builder.length() > 0)
                    builder.append(" ");
                builder.append(formatted);
            }

            return returnBuilderResult(builder);
        }
    }

    protected static class PathFormatter implements ValueFormatter {
        @Override
        public String format(Object path, ValueType valueType, FormatContext formatCtx) throws InterruptedException {
            StringBuilder builder = new StringBuilder();

            for (Object value : (HierarchyPath)path) {
                String formatted = formatCtx.format(value, valueType.getNestedValueType(), formatCtx);
                builder.append("/");
                builder.append(formatted);
            }

            return returnBuilderResult(builder);
        }
    }

    protected static class RecordFormatter implements ValueFormatter {
        @Override
        public String format(Object record, ValueType valueType, FormatContext formatCtx) throws InterruptedException {
            StringBuilder builder = new StringBuilder();
            TypeManager typeManager = formatCtx.repositoryManager.getTypeManager();

            for (Map.Entry<QName, Object> field : ((Record)record).getFields().entrySet()) {
                ValueType fieldValueType;
                try {
                    fieldValueType = typeManager.getFieldTypeByName(field.getKey()).getValueType();
                } catch (RepositoryException e) {
                    // error loading field type: skip this field
                    continue;
                }

                String result = formatCtx.format(field.getValue(), fieldValueType, formatCtx);
                if (result != null) {
                    if (builder.length() > 0)
                        builder.append(" ");
                    builder.append(result);
                }
            }

            return returnBuilderResult(builder);
        }
    }

    protected static class DateFormatter implements ValueFormatter {
        @Override
        public String format(Object value, ValueType valueType, FormatContext formatCtx) throws InterruptedException {
            LocalDate date = (LocalDate)value;
            return date.toDateTimeAtStartOfDay(DateTimeZone.UTC).toString() + "/DAY";
        }
    }

    protected static class DateTimeFormatter implements ValueFormatter {
        @Override
        public String format(Object value, ValueType valueType, FormatContext formatCtx) throws InterruptedException {
            DateTime dateTime = (DateTime)value;
            return dateTime.toDateTime(DateTimeZone.UTC).toString();
        }
    }

    protected static class AllFormatter implements ValueFormatter {
        @Override
        public String format(Object value, ValueType valueType, FormatContext formatCtx) throws InterruptedException {
            return value.toString();
        }
    }

}
