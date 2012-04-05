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
package org.lilyproject.rowlog.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class RowLogShardList {
    private List<RowLogShard> shards = Collections.emptyList();

    /**
     * Gets the list of shards, this is unmodifiable and guaranteed to not change.
     */
    public List<RowLogShard> getShards() {
        return shards;
    }

    /**
     * Adds a shard to the end of the list with shards.
     */
    public void addShard(RowLogShard shard) {
        for (RowLogShard currentShard : shards) {
            if (currentShard.getId().equals(shard.getId())) {
                throw new IllegalStateException("There is already a shard with this ID: " + shard.getId());
            }
        }

        List<RowLogShard> shards = new ArrayList<RowLogShard>(this.shards);
        shards.add(shard);
        this.shards = Collections.unmodifiableList(shards);
    }

    public void removeShard(RowLogShard shard) {
        List<RowLogShard> shards = new ArrayList<RowLogShard>(this.shards);
        Iterator<RowLogShard> shardsIt = shards.iterator();
        boolean found = false;
        while (shardsIt.hasNext()) {
            RowLogShard currentShard = shardsIt.next();
            if (currentShard == shard) {
                found = true;
                shardsIt.remove();
                break;
            }
        }

        if (!found) {
            throw new IllegalStateException("There is no such shard currently registered: " + shard.getId());
        }

        this.shards = Collections.unmodifiableList(shards);
    }
}
