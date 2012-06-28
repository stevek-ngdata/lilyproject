package org.lilyproject.indexer.model.indexerconf;

import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.util.Pair;

import java.util.*;

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

    public IndexCase getIndexCase(Record record) {
        // If an exclude matches, the record is not included in this index.
        // Excludes have higher precedence than includes.
        for (RecordMatcher exclude : excludes) {
            if (exclude.matches(record)) {
                return null;
            }
        }

        for (Pair<RecordMatcher, IndexCase> include : includes) {
            if (include.getV1().matches(record)) {
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
