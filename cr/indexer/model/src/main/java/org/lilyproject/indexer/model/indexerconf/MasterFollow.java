package org.lilyproject.indexer.model.indexerconf;

import java.io.IOException;
import java.util.Collections;

import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordNotFoundException;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.VersionNotFoundException;
import org.lilyproject.util.hbase.LilyHBaseSchema.Table;
import org.lilyproject.util.repo.VersionTag;

public class MasterFollow implements Follow {

    @Override
    public void follow(IndexUpdateBuilder indexUpdateBuilder, FollowCallback callback) throws RepositoryException, IOException, InterruptedException {
        RecordContext ctx = indexUpdateBuilder.getRecordContext();
        if (ctx.dep.id.isMaster()) {
            // We're already on a master record, stop here
            return;
        }
        
        Repository repository = indexUpdateBuilder.getRepositoryManager().getRepository(Table.RECORD.name);

        Dep masterDep = new Dep(ctx.dep.id.getMaster(), Collections.<String>emptySet());
        Record master = null;
        try {
            master = VersionTag.getIdRecord(masterDep.id, indexUpdateBuilder.getVTag(), repository);
        } catch (RecordNotFoundException e) {
            // It's ok that the master does not exist
        } catch (VersionNotFoundException e) {
            // It's ok that the master does not exist
        }

        indexUpdateBuilder.push(master, masterDep);
        callback.call();
        indexUpdateBuilder.pop();
    }


}
