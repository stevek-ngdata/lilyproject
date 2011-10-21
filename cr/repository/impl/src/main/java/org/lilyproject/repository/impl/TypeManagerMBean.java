package org.lilyproject.repository.impl;

import org.lilyproject.repository.api.TypeException;

public interface TypeManagerMBean {

    void enableSchemaCacheRefresh() throws TypeException, InterruptedException;

    void disableSchemaCacheRefresh() throws TypeException, InterruptedException;

    void triggerSchemaCacheRefresh() throws TypeException, InterruptedException;

    boolean isSchemaCacheRefreshEnabled();
}
