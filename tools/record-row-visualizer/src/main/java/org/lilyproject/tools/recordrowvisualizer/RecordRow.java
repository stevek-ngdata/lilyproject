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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.lilyproject.repository.api.RecordId;

/**
 * The root data object passed to the template, containing information
 * about the HBase-storage of a Lily record.
 */
public class RecordRow {
    public RecordId recordId;

    public List<String> unknownColumns = new ArrayList<String>();
    public SystemFields systemFields = new SystemFields();
    public Fields fields = new Fields();

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
