package com.ngdata.lily.security.hbase.client;

import javax.annotation.Nullable;

public interface AuthorizationContextProvider {
    @Nullable
    AuthorizationContext getAuthorizationContext();
}
