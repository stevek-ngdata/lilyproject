package org.lilyproject.indexer.model.indexerconf;

import java.util.List;

/**
 * A formatter which only keeps the first entry of multi-valued values.
 *
 * <p>One case where this is useful is when you want to be able to sort
 * on LIST-type fields which only sometimes contain multiple values,
 * in this case you could decide to sort on the first value.
 */
public class FirstValueFormatter extends DefaultFormatter {

    @Override
    protected List<IndexValue> filterValues(List<IndexValue> indexValues) {
        return indexValues.isEmpty() ? indexValues : indexValues.subList(0, 1);
    }

}
