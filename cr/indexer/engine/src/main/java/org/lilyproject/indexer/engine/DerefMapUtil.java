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
package org.lilyproject.indexer.engine;

import java.util.Set;

import org.lilyproject.indexer.engine.DerefMap.Dependency;
import org.lilyproject.indexer.engine.DerefMap.DependencyEntry;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.SchemaId;

public class DerefMapUtil {

    private DerefMapUtil() {
        // prevent construction
    }

    static public DependencyEntry newEntry(RecordId id, SchemaId vtag, Set<String> moreDimensions) {
        Dependency dep = new Dependency(id, vtag);
        if (moreDimensions == null) {
            return new DependencyEntry(dep);
        } else {
            return new DependencyEntry(dep, moreDimensions);
        }
    }

}
