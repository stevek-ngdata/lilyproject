/*
 * Copyright 2012 NGDATA nv
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilyproject.indexer.model.indexerconf;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.RecordId;

/**
 * Abstract dependency on a record. Basically a mutable variant of derefmap's DependencyEntry
 * with operations that make sense from the point of view of index mapping evaluation.
 *
 * <p>In the simplest and most common form, the dependency is fully described by the
 * id of the record. Only in case of forward-variant deref expressions (of the "+prop" style),
 * one can depend on a set of records which can change over time: i.e. the set of all
 * records which have the additional property "prop" is not fixed.
 */
public class Dep {

    public final RecordId id;

    /**
     * Forward-followed ("plussed") dimensions. A null value means any value.
     */
    public final Set<String> moreDimensionedVariants;

    public Dep(RecordId recordId, Set<String> moreDimensionedVariants) {
        this.id = recordId;
        this.moreDimensionedVariants = moreDimensionedVariants;
    }

    /**
     * Called for expressions of the kind "-prop".
     *
     * <p>[I guess] this is only of importance when expressions would be like "+prop=>-prop", e.g.
     * the same prop was first forward-dereferenced and then backward-dereferenced, otherwise
     * it would not be in the moreDimensionedVariants map.
     */
    public Dep minus(IdGenerator idGenerator, Set<String> vprops) {
        RecordId master = id.getMaster();
        Map<String, String> variantProps = Maps.newHashMap(id.getVariantProperties());
        Set<String> newVprops = Sets.newHashSet(vprops);

        for (String prop: vprops) {
            variantProps.remove(prop);
            newVprops.remove(prop);
        }

        return new Dep(idGenerator.newRecordId(master, variantProps), newVprops);
    }

    /**
     * Called for expressions of the kind "+prop".
     */
    public Dep plus(IdGenerator idGenerator, Map<String, String> propsToAdd) {
        RecordId master = id.getMaster();

        Map<String, String> newVariantProperties = Maps.newHashMap(id.getVariantProperties());
        Set<String> newVprops = Sets.newHashSet(this.moreDimensionedVariants);

        for (String key: propsToAdd.keySet()) {
            String value = propsToAdd.get(key);
            if (value == null) {
                newVprops.add(key);
            } else {
                newVariantProperties.put(key, value);
            }
        }

        return new Dep(idGenerator.newRecordId(master, newVariantProperties), newVprops);
    }

}
