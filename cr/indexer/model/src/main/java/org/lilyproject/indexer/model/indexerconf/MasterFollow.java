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

import java.io.IOException;
import java.util.Collections;

import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordNotFoundException;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.VersionNotFoundException;
import org.lilyproject.util.repo.VersionTag;

public class MasterFollow implements Follow {

    @Override
    public void follow(IndexUpdateBuilder indexUpdateBuilder, FollowCallback callback) throws RepositoryException, IOException, InterruptedException {
        RecordContext ctx = indexUpdateBuilder.getRecordContext();
        if (ctx.dep.id.isMaster()) {
            // We're already on a master record, stop here
            return;
        }

        Repository repository = (Repository)indexUpdateBuilder.getRepositoryManager().getTable(indexUpdateBuilder.getTable());

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
