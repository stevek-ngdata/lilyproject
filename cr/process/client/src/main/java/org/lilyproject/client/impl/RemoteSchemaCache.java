/*
 * Copyright 2011 Outerthought bvba
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
package org.lilyproject.client.impl;

import org.lilyproject.client.LilyClient;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.impl.AbstractSchemaCache;
import org.lilyproject.repository.impl.SchemaCache;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

public class RemoteSchemaCache extends AbstractSchemaCache implements SchemaCache {

    private final LilyClient lilyClient;

    public RemoteSchemaCache(ZooKeeperItf zooKeeper, LilyClient lilyClient) {
        super(zooKeeper);
        this.lilyClient = lilyClient;
    }

    @Override
    protected TypeManager getTypeManager() {
        return lilyClient.getRepository().getTypeManager();
    }
}
