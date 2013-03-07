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
package org.lilyproject.indexer.model.indexerconf;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.util.Pair;

/**
 * Decides what records to include in an index based on inclusion/exclusion rules.
 */
public class IndexRecordFilter {
    private List<Pair<RecordMatcher, IndexCase>> includes = new ArrayList<Pair<RecordMatcher, IndexCase>>();
    private List<RecordMatcher> excludes = new ArrayList<RecordMatcher>();

    public IndexRecordFilter() {

    }

    public void addExclude(RecordMatcher exclude) {
        excludes.add(exclude);
    }

    public void addInclude(RecordMatcher include, IndexCase indexCase) {
        includes.add(new Pair<RecordMatcher, IndexCase>(include, indexCase));
    }

    public Set<QName> getFieldDependencies() {
        Set<QName> result = new HashSet<QName>();

        for (Pair<RecordMatcher, IndexCase> include : includes) {
            result.addAll(include.getV1().getFieldDependencies());
        }

        for (RecordMatcher exclude : excludes) {
            result.addAll(exclude.getFieldDependencies());
        }

        return result;
    }

    public boolean dependsOnRecordType() {
        for (Pair<RecordMatcher, IndexCase> include : includes) {
            if (include.getV1().dependsOnRecordType()) {
                return true;
            }
        }

        for (RecordMatcher exclude : excludes) {
            if (exclude.dependsOnRecordType()) {
                return true;
            }
        }

        return false;
    }

    public IndexCase getIndexCase(String table, Record record) {
        // If an exclude matches, the record is not included in this index.
        // Excludes have higher precedence than includes.
        for (RecordMatcher exclude : excludes) {
            if (exclude.matches(table, record)) {
                return null;
            }
        }

        for (Pair<RecordMatcher, IndexCase> include : includes) {
            if (include.getV1().matches(table, record)) {
                return include.getV2();
            }
        }

        return null;
    }

    public List<IndexCase> getAllIndexCases() {
        List<IndexCase> cases = new ArrayList<IndexCase>(includes.size());
        for (Pair<RecordMatcher, IndexCase> include : includes) {
            cases.add(include.getV2());
        }
        return cases;
    }
}
