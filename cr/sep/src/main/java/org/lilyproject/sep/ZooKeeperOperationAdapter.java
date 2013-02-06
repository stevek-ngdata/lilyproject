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
package org.lilyproject.sep;

import org.apache.zookeeper.KeeperException;
import org.lilyproject.util.zookeeper.ZooKeeperOperation;


/**
 * Adapts an HBase SEP ZooKeeperOperation to allow it to be used within the context of a Lily ZooKeeperItf.
 */
public class ZooKeeperOperationAdapter<T> implements ZooKeeperOperation<T> {
    
    private com.ngdata.sep.util.zookeeper.ZooKeeperOperation<T> wrapped;
    
    /**
     * Construct around a Lily ZooKeeperOperation.
     * @param op operation to be adapted to the SEP
     */
    public ZooKeeperOperationAdapter(com.ngdata.sep.util.zookeeper.ZooKeeperOperation<T> op) {
        this.wrapped = op;
    }

    @Override
    public T execute() throws KeeperException, InterruptedException {
        return wrapped.execute();
    }


}
