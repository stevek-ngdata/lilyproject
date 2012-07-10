package org.lilyproject.indexer.model.indexerconf;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.RecordId;

public class Dep {

    public RecordId id;

    public Set<String> vprops;

    public Dep(RecordId recordId, Set<String> vprops) {
        this.id = recordId;
        this.vprops = vprops;
    }

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

    public Dep plus(IdGenerator idGenerator, Map<String, String> propsToAdd) {
        RecordId master = id.getMaster();

        Map<String, String> newVariantProperties = Maps.newHashMap(id.getVariantProperties());
        Set<String> newVprops = Sets.newHashSet(this.vprops);

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