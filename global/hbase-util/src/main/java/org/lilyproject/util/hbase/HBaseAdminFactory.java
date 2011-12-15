package org.lilyproject.util.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnectionManager;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Utility to avoid creation of multiple HBaseAdmin instances for the same configuration object.
 *
 * <p>HBaseAdmin internally clones the configuration, causes a different HBase connection to be
 * set up. See http://groups.google.com/group/lily-discuss/msg/740774d0c027b8e0
 */
public class HBaseAdminFactory {
    private static Map<Configuration, HBaseAdmin> admins = new HashMap<Configuration, HBaseAdmin>();

    public static synchronized HBaseAdmin get(Configuration conf) throws ZooKeeperConnectionException,
            MasterNotRunningException {

        HBaseAdmin admin = admins.get(conf);
        if (admin == null) {
            admin = new HBaseAdmin(conf);
            admins.put(conf, admin);
        }
        return admin;
    }

    public static synchronized void closeAll() {
        for (HBaseAdmin admin : admins.values()) {
            try {
                Configuration conf = admin.getConnection().getConfiguration();
                HConnectionManager.deleteConnection(conf, true);
            } catch (Throwable t) {
                Log log = LogFactory.getLog(HBaseAdminFactory.class);
                log.error("Error closing HBaseAdmin connection", t);
            }
        }
        admins.clear();
    }

    /**
     * If there is an existing configuration which has all the same properties as this configuration, return it.
     * Otherwise, returns the passed conf. This is an expensive method.
     */
    public static Configuration getExisting(Configuration conf) {
        Configuration[] confs;
        synchronized (HBaseAdminFactory.class) {
            confs = admins.keySet().toArray(new Configuration[0]);
        }
        
        Map<String, String> confAsMap = toMap(conf);
        
        for (Configuration current : confs) {
            if (toMap(current).equals(confAsMap)) {
                return current;
            }
        }
        
        return conf;
    }
    
    private static Map<String, String> toMap(Configuration conf) {
        Map<String, String> result = new HashMap<String, String>();
        Iterator<Map.Entry<String, String>> it = conf.iterator();
        while (it.hasNext()) {
            Map.Entry<String, String> entry = it.next();
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }
}
