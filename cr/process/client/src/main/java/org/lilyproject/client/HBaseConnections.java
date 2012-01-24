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
