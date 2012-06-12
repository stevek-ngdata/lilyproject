package org.lilyproject.repository.api.filter;

import java.util.Map;

import org.lilyproject.repository.api.RecordId;

/**
 * Filters based on variant properties. It returns only records which have the same variant properties as the given
 * variant properties.
 *
 * @author Jan Van Besien
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
