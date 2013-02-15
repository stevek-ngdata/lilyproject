package org.lilyproject.servletregistry.impl;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import org.lilyproject.servletregistry.api.ServletRegistry;
import org.lilyproject.servletregistry.api.ServletRegistryEntry;

public class ServletRegistryImpl implements ServletRegistry {

    private List<ServletRegistryEntry> entries = Lists.newArrayList();

    @Override
    public void addEntry(ServletRegistryEntry entry) {
        entries.add(entry);
    }

    @Override
    public List<ServletRegistryEntry> getEntries() {
        return Collections.unmodifiableList(entries);
    }

}