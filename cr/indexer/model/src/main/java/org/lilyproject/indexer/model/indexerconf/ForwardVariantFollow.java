package org.lilyproject.indexer.model.indexerconf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RepositoryException;

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

    @Override
    public void follow(IndexUpdateBuilder indexUpdateBuilder, FollowCallback callback)
            throws RepositoryException, IOException, InterruptedException {
        RecordContext ctx = indexUpdateBuilder.getRecordContext();
        Set<String> currentDimensions = Sets.newHashSet(ctx.dep.moreDimensionedVariants);
        currentDimensions.addAll(ctx.dep.id.getVariantProperties().keySet());

        if (currentDimensions.containsAll(dimensions.keySet())) {
            // the record already contains all of the new dimensions -> stop here
            return;
        } else {
            IdGenerator idGenerator = indexUpdateBuilder.getRepositoryManager().getIdGenerator();
            Dep newDep = ctx.dep.plus(idGenerator, dimensions);
            // now find all the records of this newly defined variant
            final ArrayList<Record> result = IndexerUtils.getVariantsAsRecords(indexUpdateBuilder, newDep);
            if (result == null || result.size() == 0) {
                //if there are no records, we must continue with evalDeref with a 'null' record!
                indexUpdateBuilder.push(null, newDep);
                callback.call();
                indexUpdateBuilder.pop();
            } else {
                for (Record record: result) {
                    indexUpdateBuilder.push(record, newDep);
                    callback.call();
                    indexUpdateBuilder.pop();
                }
            }
        }
    }
}

