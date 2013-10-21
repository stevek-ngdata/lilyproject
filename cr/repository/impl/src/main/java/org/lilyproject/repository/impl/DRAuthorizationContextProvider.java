package org.lilyproject.repository.impl;

import com.ngdata.lily.security.hbase.client.AuthorizationContext;
import com.ngdata.lily.security.hbase.client.AuthorizationContextProvider;
import org.lilyproject.repository.spi.AuthorizationContextHolder;

public class DRAuthorizationContextProvider implements AuthorizationContextProvider {
    @Override
    public AuthorizationContext getAuthorizationContext() {
        return AuthorizationContextHolder.getCurrentContext();
    }
}
