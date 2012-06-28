package org.lilyproject.indexer.model.indexerconf;

import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.SchemaId;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Matches records based on certain conditions, i.e. a predicate on record objects.
 */
public class RecordMatcher {
    private WildcardPattern recordTypeNamespace;
    private WildcardPattern recordTypeName;

    private QName fieldName;
    private SchemaId fieldId;
    private String fieldValue;

    /**
     * The variant properties the record should have. Evaluation rules: a key named
     * "*" (star symbol) is a wildcard meaning that any variant dimensions not specified
     * are accepted. Otherwise the variant dimension count should match exactly. The other
     * keys in the map are required variant dimensions. If their value is not null, the
     * values should match.
     */
    private final Map<String, String> variantPropsPattern;

    public RecordMatcher(WildcardPattern recordTypeNamespace, WildcardPattern recordTypeName, QName fieldName,
            String fieldValue, Map<String, String> variantPropsPattern) {
        this.recordTypeNamespace = recordTypeNamespace;
        this.recordTypeName = recordTypeName;
        this.fieldName = fieldName;
        this.fieldValue = fieldValue;
        this.variantPropsPattern = variantPropsPattern;
    }

    public boolean matches(Record record) {
        QName recordTypeName = record.getRecordTypeName();
        Map<String, String> varProps = record.getId().getVariantProperties();

        // About "recordTypeName == null": normally record type name cannot be null, but it can
        // be in the case of IndexAwareMQFeeder
        if (this.recordTypeNamespace != null &&
                (recordTypeName == null || !this.recordTypeNamespace.lightMatch(recordTypeName.getNamespace()))) {
            return false;
        }

        if (this.recordTypeName != null
                && (recordTypeName == null || !this.recordTypeName.lightMatch(recordTypeName.getName()))) {
            return false;
        }

        if (variantPropsPattern.size() != varProps.size() && !variantPropsPattern.containsKey("*")) {
            return false;
        }

        for (Map.Entry<String, String> entry : variantPropsPattern.entrySet()) {
            if (entry.getKey().equals("*"))
                continue;

            String dimVal = varProps.get(entry.getKey());
            if (dimVal == null) {
                // this record does not have a required variant property
                return false;
            }

            if (entry.getValue() != null && !entry.getValue().equals(dimVal)) {
                // the variant property does not have the required value
                return false;
            }
        }

        if (fieldName != null) {
            Object value = record.getField(fieldName);
            if (fieldValue.equals(value)) {
                return false;
            }
        }

        return true;

    }

    public Set<QName> getFieldDependencies() {
        return fieldName != null ? Collections.singleton(fieldName) : Collections.<QName>emptySet();
    }

    public boolean dependsOnRecordType() {
        return recordTypeName != null || recordTypeNamespace != null;
    }
}
