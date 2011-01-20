package org.lilyproject.repository.impl;

/**
 * This MBean was added as a temporary measure to work around slow cache reloading in the TypeManager when working
 * with huge amounts (thousands) of field types and record types. It should be removed once that problem
 * is resolved (see also services.xml of repository module to remove the registration).
 */
public interface TypeManagerMBean {
    void enableCacheInvalidationTrigger();

    void disableCacheInvalidationTrigger();

    void notifyCacheInvalidate() throws Exception;

    boolean isCacheInvalidationTriggerEnabled();
}
