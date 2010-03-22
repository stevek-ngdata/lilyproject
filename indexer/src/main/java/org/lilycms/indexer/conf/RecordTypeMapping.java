package org.lilycms.indexer.conf;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Describes for a certain record type the index fields and their mapping (binding) to record fields.
 */
public class RecordTypeMapping {
    private String recordType;
    private Set<String> versionTags = new HashSet<String>();
    protected List<IndexFieldBinding> indexFieldBindings = new ArrayList<IndexFieldBinding>();

    public RecordTypeMapping(String recordType, Set<String> versionTags) {
        this.recordType = recordType;
        this.versionTags = versionTags;
    }

    public List<IndexFieldBinding> getBindings() {
        return indexFieldBindings;
    }
}
