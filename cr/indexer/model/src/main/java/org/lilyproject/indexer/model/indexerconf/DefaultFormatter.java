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
import org.lilyproject.repository.api.HierarchyPath;
import org.lilyproject.repository.api.ValueType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class DefaultFormatter implements Formatter {
    public List<String> format(List<IndexValue> indexValues, ValueType valueType) {
        List<String> result = new ArrayList<String>();
        formatMultiValue(indexValues, valueType, result);
        return result;
    }

    private void formatMultiValue(List<IndexValue> indexValues, ValueType valueType, List<String> result) {
        for (IndexValue item : indexValues) {
            formatHierarchicalValue(item, valueType, result);
        }
    }

    private void formatHierarchicalValue(IndexValue indexValue, ValueType valueType, List<String> result) {
        if (valueType.isHierarchical()) {
            HierarchyPath path = (HierarchyPath)indexValue.value;
            StringBuffer formattedPath = new StringBuffer();
            for (Object item : path.getElements()) {
                formattedPath.append("/");
                formattedPath.append(formatPrimitiveValue(item, valueType));
            }
            result.add(formattedPath.toString());
        } else {
            result.add(formatPrimitiveValue(indexValue.value, valueType));
        }
    }

    protected String formatPrimitiveValue(Object value, ValueType valueType) {
        String type = valueType.getBaseValueType().getName();

        if (type.equals("DATE")) {
            LocalDate date = (LocalDate)value;
            return date.toDateTimeAtStartOfDay(DateTimeZone.UTC).toString() + "/DAY";
        } else if (type.equals("DATETIME")) {
            DateTime dateTime = (DateTime)value;
            return dateTime.toDateTime(DateTimeZone.UTC).toString();
        } else {
            return value.toString();
        }
    }

    public Set<String> getSupportedPrimitiveValueTypes() {
        return Collections.emptySet();
    }

    public boolean supportsSingleValue() {
        return true;
    }

    public boolean supportsMultiValue() {
        return true;
    }

    public boolean supportsNonHierarchicalValue() {
        return true;
    }

    public boolean supportsHierarchicalValue() {
        return true;
    }
}
