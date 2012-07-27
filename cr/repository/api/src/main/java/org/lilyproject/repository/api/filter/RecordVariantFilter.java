package org.lilyproject.repository.api.filter;

import java.util.Map;

import org.lilyproject.repository.api.RecordId;

/**
 * Filters based on variant properties. It only returns records which have exactly the same variant properties as the
 * given variant properties (i.e. not the records which have these variant properties and some additional ones). If the
 * value of the variant property is specified, it has to match exactly. If the value is <code>null</code>, any value
 * will match.
 *
 *
 */
public class RecordVariantFilter implements RecordFilter {

    private final RecordId masterRecordId;

    /**
     * Variant properties. Null values mean "any value", otherwise the value has to match exactly.
     */
    private final Map<String, String> variantProperties;

    public RecordVariantFilter(RecordId masterRecordId, Map<String, String> variantProperties) {
        this.masterRecordId = masterRecordId.getMaster();
        this.variantProperties = variantProperties;
    }

    public RecordId getMasterRecordId() {
        return masterRecordId;
    }

    public Map<String, String> getVariantProperties() {
        return variantProperties;
    }
}
