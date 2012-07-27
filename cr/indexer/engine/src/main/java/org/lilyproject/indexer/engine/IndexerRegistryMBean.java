package org.lilyproject.indexer.engine;

import java.util.Set;

/**
 * MBean exposing the index names known by the IndexerRegistry instance in this server (JVM).
 */
public interface IndexerRegistryMBean {
    Set<String> getIndexNames();
}
