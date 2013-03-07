package org.lilyproject.indexer.derefmap;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.lilyproject.util.ArgumentValidator;

/**
 * Representation of a variant properties pattern as stored in the dereference map.
 *
 *
 */
final class DerefMapVariantPropertiesPattern {

    /**
     * The pattern. Null values mean "any value", everything else must match exactly.
     */
    final Map<String, String> pattern;

    DerefMapVariantPropertiesPattern(Map<String, String> pattern) {
        ArgumentValidator.notNull(pattern, "pattern");

        this.pattern = pattern;
    }

    public boolean matches(Map<String, String> dependancyRecordVariantProperties) {
        if (dependancyRecordVariantProperties.size() != pattern.size()) {
            return false;
        } else {
            // all names should match exactly
            if (!dependancyRecordVariantProperties.keySet().equals(patternNames())) {
                return false;
            } else {
                // values should match if specified
                for (Map.Entry<String, String> entry : dependancyRecordVariantProperties.entrySet()) {
                    final String name = entry.getKey();
                    final String value = entry.getValue();

                    final String patternValue = patternValue(name);

                    if (patternValue != null && !patternValue.equals(value)) {
                        return false;
                    }
                }

                // no unmatching values found
                return true;
            }
        }
    }

    private Set<String> patternNames() {
        return pattern.keySet();
    }

    private String patternValue(String name) {
        return pattern.get(name);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DerefMapVariantPropertiesPattern that = (DerefMapVariantPropertiesPattern) o;

        return !(pattern != null ? !pattern.equals(that.pattern) : that.pattern != null);
    }

    @Override
    public int hashCode() {
        return pattern != null ? pattern.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "VariantPropertiesPattern{" +
                "pattern=" + pattern +
                '}';
    }

    public Map<String, String> getConcreteProperties() {
        final HashMap<String, String> result = new HashMap<String, String>();
        for (Map.Entry<String, String> patternEntry : pattern.entrySet()) {
            if (patternEntry.getValue() != null) {
                result.put(patternEntry.getKey(), patternEntry.getValue());
            }
        }
        return result;
    }

    public Set<String> getPatternProperties() {
        final Set<String> result = new HashSet<String>();
        for (Map.Entry<String, String> patternEntry : pattern.entrySet()) {
            if (patternEntry.getValue() == null) {
                result.add(patternEntry.getKey());
            }
        }
        return result;
    }
}
