/*
 * Copyright 2010 Outerthought bvba
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
package org.lilyproject.server.modules.general;

import javax.annotation.PreDestroy;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.lilyproject.util.hbase.LocalHTable;

public class HBaseConnectionDisposer {
    private Configuration conf;

    public HBaseConnectionDisposer(Configuration conf) {
        this.conf = conf;
    }

    @PreDestroy
    public void stop() {
        
        try {
         // LocalHTable keeps HTablePools in static vars: make sure that is cleaned out
            // (to avoid leaks when using resetLilyState)
            LocalHTable.closeAllPools();

            // FIXME : is this still needed?
            HConnectionManager.deleteConnection(conf, true);
        } catch (Throwable t) {
            LogFactory.getLog(getClass()).error("Problem cleaning up HBase connections", t);
        }
    }
}
