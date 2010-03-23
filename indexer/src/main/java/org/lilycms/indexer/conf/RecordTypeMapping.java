package org.lilycms.indexer.conf;

import java.util.*;

/**
 * Describes for a certain record type the index fields and their mapping (binding) to record fields.
 */
public class RecordTypeMapping {
    private String recordType;
    private Set<String> versionTags = new HashSet<String>();
    protected List<IndexFieldBinding> indexFieldBindings = new ArrayList<IndexFieldBinding>();
    private Set<String> fieldDependencies;

    public RecordTypeMapping(String recordType, Set<String> versionTags) {
        this.recordType = recordType;
        this.versionTags = versionTags;
    }

    public List<IndexFieldBinding> getBindings() {
        return indexFieldBindings;
    }

    public Set<String> getReferencedFields() {
        if (fieldDependencies == null) {
            Set<String> deps = new HashSet<String>();
            for (IndexFieldBinding binding : indexFieldBindings) {
                binding.getValue().collectFieldDependencies(deps);
            }
            fieldDependencies = Collections.unmodifiableSet(deps);
        }
        return fieldDependencies;
    }
}
