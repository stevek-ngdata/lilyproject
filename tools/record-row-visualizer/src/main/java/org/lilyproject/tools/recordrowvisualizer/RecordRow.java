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
package org.lilyproject.tools.recordrowvisualizer;

import org.lilyproject.repository.api.RecordId;

import java.util.*;

/**
 * The root data object passed to the template, containing information
 * about the HBase-storage of a Lily record.
 */
public class RecordRow {
    public RecordId recordId;

    public List<String> unknownColumns = new ArrayList<String>();
    public SystemFields systemFields = new SystemFields();
    public Fields fields = new Fields();

    // MQ row log
    public SortedMap<RowLogKey, List<String>> mqPayload = new TreeMap<RowLogKey, List<String>>();
    public SortedMap<RowLogKey, List<ExecutionData>> mqState = new TreeMap<RowLogKey, List<ExecutionData>>();

    // WAL row log
    public SortedMap<RowLogKey, List<String>> walPayload = new TreeMap<RowLogKey, List<String>>();
    public SortedMap<RowLogKey, List<ExecutionData>> walState = new TreeMap<RowLogKey, List<ExecutionData>>();

    public List<String> unknownColumnFamilies = new ArrayList<String>();

    private TreeSet<Long> allVersions;

    public RecordId getRecordId() {
        return recordId;
    }

    public List<String> getUnknownColumns() {
        return unknownColumns;
    }

    public Fields getFields() {
        return fields;
    }

    public SystemFields getSystemFields() {
        return systemFields;
    }

    public SortedMap<RowLogKey, List<String>> getMqPayload() {
        return mqPayload;
    }

    public SortedMap<RowLogKey, List<ExecutionData>> getMqState() {
        return mqState;
    }

    public SortedMap<RowLogKey, List<String>> getWalPayload() {
        return walPayload;
    }

    public SortedMap<RowLogKey, List<ExecutionData>> getWalState() {
        return walState;
    }

    public List<String> getUnknownColumnFamilies() {
        return unknownColumnFamilies;
    }

    public Set<Long> getAllVersions() {
        if (allVersions == null) {
            allVersions = new TreeSet<Long>();
            fields.collectVersions(allVersions);
            systemFields.collectVersions(allVersions);
        }
        return allVersions;
    }

    public int getAllVersionsLength() {
        return getAllVersions().size();
    }
}
