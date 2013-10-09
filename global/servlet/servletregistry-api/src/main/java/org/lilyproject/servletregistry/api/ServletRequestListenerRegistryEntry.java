package org.lilyproject.servletregistry.api;

import javax.servlet.ServletRequestListener;

public interface ServletRequestListenerRegistryEntry {
    ServletRequestListener getListenerInstance();
}
