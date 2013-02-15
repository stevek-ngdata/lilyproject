package org.lilyproject.servletregistry.api;

import java.util.List;

public interface ServletRegistry {

    void addEntry(ServletRegistryEntry entry);
    List<ServletRegistryEntry> getEntries();


}
