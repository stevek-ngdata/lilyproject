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
package org.lilyproject.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.lilyproject.util.hbase.HBaseAdminFactory;

import java.util.*;

public class HBaseConnections {
    private List<Configuration> configurations = new ArrayList<Configuration>();
    
    /**
     * If there is an existing configuration which has all the same properties as this configuration, return it.
     * Otherwise, returns the passed conf. This is an expensive method.
     */
    public Configuration getExisting(Configuration conf) {
        Configuration[] confs;
        synchronized (HBaseConnections.class) {
            confs = configurations.toArray(new Configuration[0]);
        }

        Map<String, String> confAsMap = toMap(conf);

        for (Configuration current : confs) {
            if (toMap(current).equals(confAsMap)) {
                return current;
            }
        }
        
        // It's a new configuration, add it to the list
        configurations.add(conf);

        return conf;
    }

    public List<Configuration> getConfigurations() {
        return Collections.unmodifiableList(configurations);
    }

    private Map<String, String> toMap(Configuration conf) {
        Map<String, String> result = new HashMap<String, String>();
        Iterator<Map.Entry<String, String>> it = conf.iterator();
        while (it.hasNext()) {
            Map.Entry<String, String> entry = it.next();
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }
    
    public synchronized void close() {
        for (Configuration conf : configurations) {
            HConnectionManager.deleteConnection(conf, true);
            // Close the corresponding HBaseAdmin connection (which uses a cloned conf object)
            HBaseAdminFactory.close(conf);
        }
        configurations.clear();
    }
}
