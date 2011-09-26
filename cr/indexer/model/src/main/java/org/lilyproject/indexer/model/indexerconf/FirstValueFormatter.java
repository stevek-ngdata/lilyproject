package org.lilyproject.indexer.model.indexerconf;

import org.lilyproject.repository.api.ValueType;

import java.util.List;

/**
 * A formatter which only keeps the first entry of multi-valued values.
 *
 * <p>One case where this is useful is when you want to be able to sort
 * on fields which are sometimes multi-valued, in this case for multi-valued
 * fields you can keep the first value.</p>
 */
public class FirstValueFormatter extends DefaultFormatter {
    @Override
    protected void formatMultiValue(List<IndexValue> indexValues, ValueType valueType, List<String> result) {
        if (!indexValues.isEmpty()) {
            IndexValue item = indexValues.get(0);
            formatHierarchicalValue(item, valueType, result);
        }
    }
}
