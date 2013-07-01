/*
 * Copyright 2013 NGDATA nv
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilyproject.servletregistry.impl;

import static java.lang.Integer.valueOf;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.google.common.collect.Lists;
import org.lilyproject.servletregistry.api.ServletFilterRegistryEntry;
import org.lilyproject.servletregistry.api.ServletRegistry;
import org.lilyproject.servletregistry.api.ServletRegistryEntry;

public class ServletRegistryImpl implements ServletRegistry {

    private List<ServletRegistryEntry> entries = Lists.newArrayList();
    private List<ServletFilterRegistryEntry> filters = Lists.newArrayList();

    @Override
    public void addEntry(ServletRegistryEntry entry) {
        entries.add(entry);
    }

    @Override
    public List<ServletRegistryEntry> getEntries() {
        return Collections.unmodifiableList(entries);
    }

    @Override
    public void addFilterEntry(ServletFilterRegistryEntry servletFilterEntry) {
        filters.add(servletFilterEntry);
    }

    @Override
    public List<ServletFilterRegistryEntry> getFilterEntries() {
        Collections.sort(filters, new Comparator<ServletFilterRegistryEntry>() {
            @Override
            public int compare(ServletFilterRegistryEntry e1,
                               ServletFilterRegistryEntry e2) {
                return valueOf(e1.getPriority()).compareTo(e2.getPriority());
            }
        });
        return Collections.unmodifiableList(filters);
    }

}