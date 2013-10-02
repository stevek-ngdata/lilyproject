package org.lilyproject.repository.spi;

import com.ngdata.lily.security.hbase.client.AuthorizationContext;

/**
 * Sets the current authorization context (= the authenticated user) based on thread-local state. Make sure you
 * clearing the context is assured!
 */
public class AuthorizationContextHolder {
    private final static ThreadLocal<AuthorizationContext> tl = new ThreadLocal<AuthorizationContext>();

    public static void setCurrentContext(AuthorizationContext context) {
        tl.set(context);
    }

    public static void clearContext() {
        tl.remove();
    }

    public static AuthorizationContext getCurrentContext() {
        return tl.get();
    }
}
