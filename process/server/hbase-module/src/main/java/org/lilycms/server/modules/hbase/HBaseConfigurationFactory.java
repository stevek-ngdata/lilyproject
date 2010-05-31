package org.lilycms.server.modules.hbase;

import org.apache.hadoop.conf.Configuration;

public interface HBaseConfigurationFactory {
    Configuration get();
}
