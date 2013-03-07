package org.lilyproject.indexer.derefmap;

import java.util.Collections;
import java.util.Set;

import org.apache.commons.lang.builder.ToStringStyle;

import org.apache.commons.lang.builder.ToStringBuilder;

import org.lilyproject.repository.api.AbsoluteRecordId;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.util.ArgumentValidator;
import org.lilyproject.util.ObjectUtils;

/**
 * An entry in the dereference map, used when updating information in the dereference map.
 */
public final class DependencyEntry {

    /**
     * Identification of the record on which we depend. Note that if we want to express a dependency on all records
     * with a certain variant property set (with any value), this/these particular variant property/properties
     * have to be stripped of the record id, and have to be added to the set of more dimensioned variants.
     */
    private final AbsoluteRecordId dependency;

    /**
     * A set of variant properties that specify the variant properties that have to be added to the record id (with
     * any value) in order to come to the actual record ids on which we have a dependency.
     */
    private final Set<String> moreDimensionedVariants;

    public DependencyEntry(AbsoluteRecordId dependency) {
        this(dependency, Collections.<String>emptySet());
    }

    public DependencyEntry(AbsoluteRecordId dependency, Set<String> moreDimensionedVariants) {
        ArgumentValidator.notNull(dependency, "dependency");
        ArgumentValidator.notNull(moreDimensionedVariants, "moreDimensionedVariants");

        this.dependency = dependency;
        this.moreDimensionedVariants = Collections.unmodifiableSet(moreDimensionedVariants);
    }

    public AbsoluteRecordId getDependency() {
        return dependency;
    }

    public Set<String> getMoreDimensionedVariants() {
        return moreDimensionedVariants;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DependencyEntry entry = (DependencyEntry) o;

        if (dependency != null ? !dependency.equals(entry.dependency) : entry.dependency != null) {
            return false;
        }
        if (moreDimensionedVariants != null ? !moreDimensionedVariants.equals(entry.moreDimensionedVariants) :
                entry.moreDimensionedVariants != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = dependency != null ? dependency.hashCode() : 0;
        result = 31 * result + (moreDimensionedVariants != null ? moreDimensionedVariants.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
