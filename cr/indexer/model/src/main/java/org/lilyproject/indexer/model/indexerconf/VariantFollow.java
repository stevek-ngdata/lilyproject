package org.lilyproject.indexer.model.indexerconf;

import java.util.Set;

/**
 * Represents a -prop1[,-prop2 ...] follow
 */
public class VariantFollow implements Follow {
    private Set<String> dimensions;

    public VariantFollow(Set<String> dimensions) {
        this.dimensions = dimensions;
    }

    public Set<String> getDimensions() {
        return dimensions;
    }
}