package org.lilyproject.indexer.model.indexerconf;

import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.base.Predicate;
import com.google.common.collect.Maps;

/**
 * Follow definition for the +foo=value,+bar syntax to dereference towards more dimensioned variants. If a record
 * has none of the defined variant dimensions, or only some, the indexer looks for variants which have all of the
 * defined dimensions. If a record already has all of the defined variant dimensions, the indexer looks no further.
 */
public class ForwardVariantFollow implements Follow {

    /**
     * Dimensions to follow. A null value means "any value".
     */
    private Map<String, String> dimensions;

    public ForwardVariantFollow(Map<String, String> dimensions) {
        this.dimensions = dimensions;
    }

    public Map<String, String> getDimensions() {
        return dimensions;
    }

    public Map<String, String> getValuedDimensions() {
        return Maps.filterValues(dimensions, new Predicate<String>() {
            @Override
            public boolean apply(@Nullable String input) {
                return input != null;
            }
        });
    }
}

